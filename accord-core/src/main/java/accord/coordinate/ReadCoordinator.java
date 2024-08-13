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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Lists;

import accord.coordinate.tracking.ReadTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.CheckStatus.WithQuorum;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;

import static accord.messages.CheckStatus.WithQuorum.HasQuorum;
import static accord.messages.CheckStatus.WithQuorum.NoQuorum;
import static accord.utils.Invariants.debug;

// TODO (expected): configure the number of initial requests we send
public abstract class ReadCoordinator<Reply extends accord.messages.Reply> extends ReadTracker implements Callback<Reply>
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

    protected final Node node;
    protected final TxnId txnId;
    private boolean isDone;
    private Throwable failure;
    final Map<Id, Object> debug = debug() ? new TreeMap<>() : null;

    protected ReadCoordinator(Node node, Topologies topologies, TxnId txnId)
    {
        super(topologies);
        this.node = node;
        this.txnId = txnId;
    }

    protected abstract Action process(Id from, Reply reply);
    protected abstract void onDone(Success success, Throwable failure);
    protected abstract void contact(Id to);

    // TODO (desired): this isn't very clean way of integrating these responses
    protected Ranges unavailable(Reply reply) { throw new UnsupportedOperationException(); }

    @Override
    public void onSuccess(Id from, Reply reply)
    {
        if (debug != null)
            debug.merge(from, reply, (a, b) -> a instanceof List<?> ? ((List<Object>) a).add(b) : Lists.newArrayList(a, b));

        if (isDone)
            return;

        switch (process(from, reply))
        {
            default: throw new IllegalStateException();
            case Aborted:
                isDone = true;
                break;

            case TryAlternative:
                Invariants.checkState(!reply.isFinal());
                onSlowResponse(from);
                break;

            case Reject:
                handle(recordReadFailure(from));
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
        handle(recordSlowResponse(from));
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (debug != null)
            debug.merge(from, failure, (a, b) -> a instanceof List<?> ? ((List<Object>) a).add(b) : Lists.newArrayList(a, b));

        if (isDone)
            return;

        if (this.failure == null) this.failure = failure;
        else this.failure.addSuppressed(failure);

        handle(recordReadFailure(from));
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        node.agent().onUncaughtException(failure);
        if (isDone)
            return;

        if (this.failure != null)
            failure.addSuppressed(this.failure);
        this.failure = failure;
        finishOnFailure();
    }

    protected void finishOnFailure(Throwable failure, boolean overrideExistingFailure)
    {
        Invariants.checkState(!isDone);
        if (overrideExistingFailure)
        {
            if (this.failure != null)
                failure.addSuppressed(this.failure);
            this.failure = failure;
        }
        else
        {
            if (this.failure != null) this.failure.addSuppressed(failure);
            else this.failure = failure;
        }
        finishOnFailure();
    }

    protected void finishOnFailure()
    {
        Invariants.checkState(!isDone);
        Invariants.checkState(failure != null);
        isDone = true;
        onDone(null, failure);
    }

    protected void finishOnExhaustion()
    {
        Invariants.checkState(!isDone);
        if (failure == null)
        {
            Ranges unavailable = Ranges.EMPTY;
            for (ReadShardTracker tracker : trackers)
            {
                if (tracker == null || tracker.hasSucceeded())
                    continue;

                if (tracker.unavailable() != null)
                {
                    if (tracker.unavailable() != null)
                        unavailable = unavailable.with(tracker.unavailable());
                }
                else unavailable = unavailable.with(Ranges.of(tracker.shard.range));
            }

            failure = new Exhausted(txnId, null, unavailable);
        }
        finishOnFailure();
    }

    private void handle(RequestStatus result)
    {
        switch (result)
        {
            default: throw new AssertionError();
            case NoChange:
                break;
            case Success:
                Invariants.checkState(!isDone);
                onDone(waitingOnData == 0 ? Success.Success : Success.Quorum, null);
                // isDone = true needs to be last or exceptions thrown by onDone are ignored and this never finishes
                isDone = true;
                break;
            case Failed:
                finishOnExhaustion();
        }
    }

    protected void start(Iterable<Id> to)
    {
        to.forEach(this::contact);
    }

    public void start()
    {
        List<Id> contact = new ArrayList<>(maxShardsPerEpoch());
        if (trySendMore(List::add, contact) != RequestStatus.NoChange)
            throw new IllegalStateException();
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
        contact.forEach(this::contact);
        return status;
    }
}
