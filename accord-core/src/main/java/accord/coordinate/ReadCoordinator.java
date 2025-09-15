/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.coordinate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import accord.api.Tracing;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.ReadTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Callback;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.WithQuorum;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.DebugMap;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedList;
import accord.utils.UnhandledEnum;
import accord.utils.WrappableException;
import org.agrona.collections.IntHashSet;

import static accord.primitives.WithQuorum.HasQuorum;
import static accord.primitives.WithQuorum.NoQuorum;
import static accord.utils.Invariants.debug;
import static accord.utils.Invariants.illegalState;

// TODO (expected): configure the number of initial requests we send
public abstract class ReadCoordinator<Result, Reply extends accord.messages.Reply> extends ReadTracker implements Callback<Reply>, Coordination
{
    public enum Action
    {
        /**
         * The coordination has been failed prior to returning this response, and no further action should be taken.
         */
        Aborted,

        /**
         * This response is unsuitable for the purposes of this coordination, whether individually or as a quorum.
         */
        Reject,

        /**
         * An intermediate response has been received that suggests a full response may be delayed; another replica
         * should be contacted for its response. This is currently used when a read is lacking necessary information
         * (such as a commit) in order to serve the response, and so additional information is sent by the coordinator.
         */
        TryAlternative,

        None,

        /**
         * This response is unsuitable by itself, but if a quorum of such responses is received for the shard
         * we will Success.Quorum
         */
        ApproveIfQuorum,

        /**
         * This response is suitable by itself; if we receive such a response from each shard we will complete
         * successfully with Success.Success
         */
        Approve,

        /**
         * This response is suitable by itself, but covers only a subset of the requested range; any implementation
         * returning this must also implement {@link ReadCoordinator#unavailable}; the missing ranges will be tracked
         * and once a response has arrived covering the full range of the query these responses will be treated as
         * equivalent to Approve
         */
        ApprovePartial
    }

    protected enum Success
    {
        Quorum(HasQuorum), Success(NoQuorum);

        public final WithQuorum withQuorum;

        Success(WithQuorum withQuorum)
        {
            this.withQuorum = withQuorum;
        }
    }

    private final long coordinationId;
    protected final Node node;
    protected final SequentialAsyncExecutor executor;
    protected final TxnId txnId;
    protected final @Nullable Tracing tracing;
    private final DebugMap debug;

    private BiConsumer<? super Result, Throwable> callback;
    private boolean isDone;
    private Throwable failure;

    protected ReadCoordinator(Node node, SequentialAsyncExecutor executor, Topologies topologies, TxnId txnId, Participants<?> participants, BiConsumer<? super Result, Throwable> callback)
    {
        super(topologies);
        this.coordinationId = node.nextCoordinationId();
        this.node = node;
        this.executor = executor;
        this.txnId = txnId;
        this.tracing = node.agent().trace(txnId, participants, kind());
        this.debug = debug() ? new DebugMap(topologies.nodes()) : null;
        this.callback = callback;
    }

    protected abstract Action process(Id from, Reply reply);
    protected abstract void onDone(Success success, Throwable failure);
    protected abstract void contact(Id to);

    // TODO (desired): this isn't very clean way of integrating these responses
    protected Ranges unavailable(Reply reply) { throw new UnsupportedOperationException(); }

    @Override
    public void onSuccess(Id from, Reply reply)
    {
        if (isDone)
            return;

        if (debug != null)
            debug.debug(from, reply);

        Action action = process(from, reply);
        if (tracing != null)
            tracing.trace(null, "received reply from %s: (%s) %s", from, action, reply);
        switch (action)
        {
            default: throw new UnhandledEnum(action);
            case Aborted:
                setDone();

            case None:
                break;

            case TryAlternative:
                Invariants.require(!reply.isFinal());
                onSlowResponse(from);
                break;

            case Reject:
                handle(recordFailure(from));
                break;

            case ApproveIfQuorum:
                handle(recordQuorumReadSuccess(from));
                break;

            case Approve:
                handle(recordReadSuccess(from));
                break;

            case ApprovePartial:
                handle(recordPartialReadSuccess(from, unavailable(reply)));
                break;
        }
    }

    @Override
    public void onSlowResponse(Id from)
    {
        if (!isDone)
        {
            if (tracing != null)
                tracing.trace(null, "marking %s slow", from);
            handle(recordSlowResponse(from));
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        if (debug != null)
            debug.debug(from, failure);

        if (tracing != null)
        {
            if (failure == null) tracing.trace(null, "timeout %s", from);
            else tracing.trace(null, "received failure response from %s: %s", from, failure);
        }

        this.failure = FailureAccumulator.append(this.failure, failure);
        handle(recordFailure(from));
    }

    @Override
    public boolean onCallbackFailure(Id from, Throwable failure)
    {
        if (isDone) return false;

        if (tracing != null)
            tracing.trace(null, "callback failed processing reply from %s: %s", from, failure);

        finishWithFailureOverride(failure);
        return true;
    }

    protected void finishWithFailureOverride(Throwable failure)
    {
        Invariants.require(!isDone);
        this.failure = FailureAccumulator.append(failure, this.failure);
        finishOnFailure();
    }

    protected void finishWithFailure(Throwable failure)
    {
        Invariants.require(!isDone);
        this.failure = FailureAccumulator.append(this.failure, failure);
        finishOnFailure();
    }

    protected void tryFinishOnFailure()
    {
        if (!isDone)
        {
            failure = FailureAccumulator.fail(node.agent(), failure, txnId);
            finishOnFailure();
        }
    }

    protected void finishOnFailure()
    {
        Invariants.require(!isDone);
        Invariants.require(failure != null);
        onDone(null, failure);
        node.agent().coordinatorEvents().onFailed(failure, txnId, scope(), this);
        if (tracing != null)
            tracing.trace(null, "finishing with failure: %s", failure);
        setDone();
    }

    protected void finishOnExhaustion()
    {
        Invariants.require(!isDone);
        Ranges unavailable = Ranges.EMPTY;
        for (ReadShardTracker tracker : trackers)
        {
            if (tracker == null || tracker.hasSucceeded())
                continue;

            if (tracker.unavailable() != null) unavailable = unavailable.with(tracker.unavailable());
            else unavailable = unavailable.with(Ranges.of(tracker.shard.range));
        }
        failure = FailureAccumulator.fail(node.agent(), failure, txnId, null, unavailable);
        finishOnFailure();
    }

    void setDone()
    {
        Invariants.require(!isDone);
        isDone = true;
        node.unregister(this);
    }

    private void handle(RequestStatus result)
    {
        switch (result)
        {
            default: throw new AssertionError();
            case NoChange:
                Invariants.require(!inflight.isEmpty());
                break;
            case Success:
                Invariants.require(!isDone);
                Success success = waitingOnData == 0 ? Success.Success : Success.Quorum;
                onDone(success, null);
                // isDone = true needs to be last or exceptions thrown by onDone are ignored and this never finishes
                setDone();
                if (tracing != null)
                    tracing.trace(null, "finishing with %s", success);
                break;
            case Failed:
                finishOnExhaustion();
        }
    }

    protected void start(List<Id> to)
    {
        if (tracing != null)
            tracing.trace(null, "contacting %s", to);
        to.forEach(this::contact);
    }

    public final void start()
    {
        node.register(this);
        if (initialise()) startOnceInitialised();
        else finishOnExhaustion();
    }

    protected void startOnceInitialised()
    {
        List<Id> contact = new ArrayList<>(maxShardsPerEpoch());
        if (trySendMore(List::add, contact) != RequestStatus.NoChange)
            throw illegalState();
        start(contact);
    }

    @Override
    protected RequestStatus trySendMore()
    {
        // TODO (low priority): due to potential re-entrancy into this method, if the node we are contacting is unavailable
        //                      so onFailure is invoked immediately, for the moment we copy nodes to an intermediate list.
        //                      would be better to prevent reentrancy either by detecting this inside trySendMore or else
        //                      queueing callbacks externally, so two may not be in-flight at once
        List<Id> contact = new ArrayList<>(1);
        RequestStatus status = trySendMore(List::add, contact);
        if (tracing != null)
            tracing.trace(null, "contacting %s", contact);
        contact.forEach(this::contact);
        return status;
    }

    @Override
    public SortedList<Id> inflight()
    {
        Node.Id[] ids = new Node.Id[inflight.size()];
        int count = 0;
        IntHashSet.IntIterator iter = inflight.iterator();
        while (iter.hasNext())
            ids[count++] = new Node.Id(iter.nextValue());
        Arrays.sort(ids);
        return SortedArrayList.ofSorted(ids);
    }

    @Override
    public SortedList<Id> contacted()
    {
        return nodes().without(SortedArrayList.copyUnsorted(candidates, Node.Id[]::new));
    }

    @Override
    public long coordinationId()
    {
        return coordinationId;
    }

    @Override
    public final TxnId txnId()
    {
        return txnId;
    }

    @Override
    public AbstractTracker<?> tracker()
    {
        return this;
    }

    @Override
    public SequentialAsyncExecutor executor()
    {
        return executor;
    }

    @Override
    public boolean abort()
    {
        if (isDone)
            return false;

        finishWithFailureOverride(Aborted.aborted(txnId, Route.tryCastToRoute(scope())));
        return true;
    }

    // this is a little clunky, and could easily be misused
    // TODO (desired): try to rework ReadCoordinator to match some of the improved properties of AbstractCoordination
    Action retryWithEpochExact(long epoch, Runnable runnable)
    {
        node.withEpochExact(epoch, executor, (ignore, failure) -> finishWithFailureOverride(failure), WrappableException::wrap, runnable);
        return Action.Aborted;
    }

    void invokeCallback(Result success, Throwable failure)
    {
        takeCallback().accept(success, failure);
    }

    protected BiConsumer<? super Result, Throwable> takeCallback()
    {
        BiConsumer<? super Result, Throwable> callback = this.callback;
        this.callback = null;
        Invariants.require(callback != null);
        return callback;
    }

    @Override
    public String toString()
    {
        return getClass() + ":" + txnId;
    }
}
