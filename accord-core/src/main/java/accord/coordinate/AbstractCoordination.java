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

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Timeouts;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.MapReduceConsumeCommandStores;
import accord.local.Node;
import accord.api.ExclusiveAsyncExecutor;
import accord.messages.Request;
import accord.messages.Callback;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.DebugMap;
import accord.utils.Invariants;
import accord.utils.SimpleBitSet;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedList;
import accord.utils.SortedListMap;
import accord.utils.SortedListSet;
import accord.utils.Rethrowable;
import accord.utils.async.AsyncChain;
import accord.utils.async.Cancellable;

import static accord.api.ProtocolModifiers.permitLocalDelivery;
import static accord.coordinate.AbstractCoordination.LocalExecuteState.PENDING;
import static accord.coordinate.AbstractCoordination.LocalExecuteState.SUCCESS;
import static accord.coordinate.AbstractCoordination.LocalExecuteState.TIMEOUT;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public abstract class AbstractCoordination<P extends Participants<?>, Result, Reply extends accord.messages.Reply, Ok> extends AbstractSimpleCoordination<P> implements Callback<Reply>, Callback.CallbackExclusive<Reply>
{
    private final SortedArrayList<Node.Id> nodes;
    private final SimpleBitSet expectingReply;
    private final @Nullable DebugMap debug;

    private BiConsumer<? super Result, Throwable> callback;
    private Object[] replyState;
    private int replyCount;
    private boolean unsafeToReply;

    protected AbstractCoordination(Node node, ExclusiveAsyncExecutor executor, TxnId txnId, P scope, SortedArrayList<Node.Id> nodes, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, executor, txnId, scope);
        this.nodes = nodes;
        this.callback = Invariants.nonNull(callback);
        this.replyState = new Object[nodes.size()];
        this.expectingReply = SimpleBitSet.allocate(nodes.size());
        this.debug = Invariants.debug() ? new DebugMap(nodes) : null;
    }

    abstract void onSuccessInternal(Node.Id from, int fromIndex, Reply reply);
    abstract void onFailureInternal(Node.Id from, int fromIndex, Throwable fail);
    void onSlowResponseInternal(Node.Id from) {}
    public abstract @Nonnull AbstractTracker<?> tracker();
    @Override public SortedList<Node.Id> nodes() { return nodes; }

    @Override
    final void setDone()
    {
        super.setDone();
        clearInFlight(false);
    }

    void recordOk(int fromIndex, Ok ok)
    {
        Invariants.require(replyState[fromIndex] == null, "%s", this);
        replyState[fromIndex] = ok;
        replyCount++;
    }

    SortedListMap<Node.Id, Ok> finishOks()
    {
        Invariants.require(replyState != null, "%s", this);
        setDoneWithReplies();
        clearInFlight(false);
        SortedListMap<Node.Id, Ok> result = new SortedListMap<>(nodes, replyState, replyCount);
        replyState = null;
        return result;
    }

    private void clearInFlight(boolean cancelSelf)
    {
        for (int i = expectingReply.nextSetBit(0) ; i >= 0 ; i = expectingReply.nextSetBit(i + 1))
        {
            Object cancel = replyState[i];
            if (cancel != null)
            {
                if (cancelSelf || !nodes.get(i).equals(node.id()))
                    ((Cancellable)cancel).cancel();
                replyState[i] = null;
            }
        }
        expectingReply.clear();
    }

    <V> V foldlOks(BiFunction<Ok, V, V> foldl, V zero)
    {
        V result = zero;
        for (int i = 0; i < replyState.length ; ++i)
        {
            if (replyState[i] != null && !expectingReply.get(i))
                result = foldl.apply((Ok)replyState[i], result);
        }
        return result;
    }

    void finishWithSuccess(Result success)
    {
        finishAndInvokeCallback(success, null);
    }

    protected void finishWithFailure(Throwable failure)
    {
        finishAndInvokeCallback(null, failure);
    }

    protected void finishOnFailure(Throwable failure)
    {
        finishWithFailure(FailureAccumulator.append(failure, this.failure()));
    }

    void finishOnFailure()
    {
        finishWithFailure(FailureAccumulator.fail(node.agent(), this.failure(), txnId, Route.tryCastToRoute(scope())));
    }

    void finishOnExaustion()
    {
        finishOnFailure();
    }

    void awaitEpochExactToFinish(long epoch, Runnable runnable)
    {
        setFinishing();
        node.withEpochExact(epoch, executor, (ignore, failure) -> finishOnFailure(failure), Rethrowable::rethrowable, () -> {
            runnable.run();
            Invariants.require(isDone(), "%s", this);
        });
    }

    void awaitEpochAtLeastToFinish(long epoch, Runnable runnable)
    {
        setFinishing();
        node.withEpochAtLeast(epoch, executor, (ignore, failure) -> finishOnFailure(failure), Rethrowable::rethrowable, () -> {
            runnable.run();
            Invariants.require(isDone(), "%s", this);
        });
    }

    void awaitToFinish(AsyncChain<?> await)
    {
        setFinishing();
        await.begin((success, fail) -> {
            if (fail != null) finishOnFailure(fail);
            else Invariants.require(isDone(), "%s", this);
        });
    }

    void markSelfContacted()
    {
        expectingReply.set(nodes.find(node.id()));
    }

    void contact(Function<Node.Id, Request> request)
    {
        contact(request, null);
    }

    void contact(Function<Node.Id, Request> request, @Nullable Predicate<Node.Id> include)
    {
        executor.executeMaybeImmediately(() -> {
            unsafeToReply = true;
            try
            {
                AbstractTracker<?> tracker = tracker();
                Topologies topologies = tracker.topologies();
                if (tracing != null)
                    tracing.trace(null, "contacting %s", nodes);

                for (int i = 0; i < nodes.size() ; ++i)
                {
                    Node.Id to = nodes.get(i);
                    if (include == null || include.test(to))
                    {
                        if (topologies.isFaulty(to))
                        {
                            if (tracing != null)
                                tracing.trace(null, "%s is considered faulty; recording failure instead", to);
                            if (RequestStatus.Failed == tracker.prerecordFailure(to))
                            {
                                finishOnExaustion();
                                return;
                            }
                        }
                        else
                        {
                            Invariants.require(replyState[i] == null);
                            expectingReply.set(i);
                            // TODO (expected): do not cancel PreAccept, Accept, Commit, Stable or Apply to self on done
                            replyState[i] = node.send(to, request.apply(to), executor, this, tracing);
                            Invariants.require(expectingReply.get(i));
                        }
                    }
                }
            }
            finally
            {
                unsafeToReply = false;
            }
        });
    }

    // this is a special case, where the request is expected to reply, responding to the existing callback
    // but for local requests, there is no registered callback, the reply is direct to the reply context, so we need to supply it again
    void recontact(Node.Id to, Request send)
    {
        node.maybeTraceRemote(to, send, tracing);
        if (permitLocalDelivery()) Invariants.expect(!to.equals(node.id())); // we should never have to recontact ourselves, and callbacks/timeouts will not work correctly if we do
        node.send(to, send, tracing);
    }

    @Override
    public final void onSuccess(Node.Id from, Reply reply)
    {
        CallbackExclusive.onSuccess(executor, unsafeToReply, this, from, reply);
    }

    @Override
    public final void onSlow(Node.Id from)
    {
        CallbackExclusive.onSlow(executor, unsafeToReply, this, from);
    }

    @Override
    public final void onFailure(Node.Id from, Throwable failure)
    {
        CallbackExclusive.onFailure(executor, unsafeToReply, this, from, failure);
    }

    @Override
    public final boolean onCallbackFailure(Node.Id from, Throwable failure)
    {
        node.agent().onException(failure, from.toString());
        return true;
    }

    @Override
    public void onSuccessExclusive(Node.Id from, Reply reply)
    {
        int fromIndex = onReply(from, reply, reply.isFinal());
        if (fromIndex < 0)
            return;

        onSuccessInternal(from, fromIndex, reply);
        Invariants.require(!expectingReply.isEmpty() || isFinishing() || isDone(), "%s", this);
    }

    @Override
    public void onSlowExclusive(Node.Id from)
    {
        if (isDoneWithReplies())
            return;

        if (tracing != null)
            tracing.trace(null, "marking %s slow", from);
        onSlowResponseInternal(from);
    }

    @Override
    public void onFailureExclusive(Node.Id from, @Nullable Throwable failure)
    {
        int fromIndex = onReply(from, failure, true);
        if (fromIndex < 0)
            return;

        if (tracing != null)
        {
            if (failure == null) tracing.trace(null, "timeout %s", from);
            else tracing.trace(null, "received failure reply from %s: %s", from, failure);
        }

        onFailureInternal(from, fromIndex, failure);
        Invariants.require(!expectingReply.isEmpty() || isFinishing() || isDone(), "%s", this);
    }

    private int onReply(Node.Id from, Object reply, boolean isFinal)
    {
        Invariants.require(!unsafeToReply);
        int fromIndex = nodes.find(from);
        if (isDoneWithReplies())
        {
            if (isFinal && replyState != null)
                replyState[fromIndex] = null;
            return -1;
        }

        if (debug != null)
            debug.debug(from, reply);

        if (tracing != null)
            tracing.trace(null, "from %s: %s", from, reply);

        if (isFinal)
        {
            boolean expecting = expectingReply.unset(fromIndex);
            Invariants.require(expecting, "%s", this);
            replyState[fromIndex] = null;
        }
        else if (!expectingReply.get(fromIndex))
        {
            // messages can (rarely) be reordered, so we could receive a non-final reply after a final one; simply ignore this case
            return -1;
        }
        return fromIndex;
    }

    @Override
    public void onCallbackFailureExclusive(Node.Id from, Throwable failure)
    {
        try
        {
            setDone();
            BiConsumer<?, Throwable> callback = tryTakeCallback();
            if (callback != null) callback.accept(null, failure);
            else node.agent().onException(failure);
            if (tracing != null)
                tracing.trace(null, "callback failure processing from %s: %s", from, failure);
        }
        catch (Throwable t)
        {
            t.addSuppressed(failure);
            node.agent().onException(t);
        }
    }

    void finishAndInvokeCallback(Result success, Throwable failure)
    {
        finishAndTakeCallback().accept(success, failure);
        if (failure != null) node.agent().coordinatorEvents().onFailed(failure, txnId, scope, this);
    }

    BiConsumer<? super Result, Throwable> finishAndTakeCallback()
    {
        setDone();
        return takeCallback();
    }

    private BiConsumer<? super Result, Throwable> takeCallback()
    {
        BiConsumer<? super Result, Throwable> callback = this.callback;
        this.callback = null;
        Invariants.require(callback != null, "%s", this);
        return callback;
    }

    private BiConsumer<? super Result, Throwable> tryTakeCallback()
    {
        if (callback == null)
            return null;
        return takeCallback();
    }

    @Override
    public boolean abort()
    {
        if (isDone())
            return false;

        finishOnFailure(Aborted.aborted(txnId, Route.tryCastToRoute(scope())));
        return true;
    }

    @Override
    public final SortedListMap<Node.Id, ?> replies()
    {
        Object[] replyState = this.replyState;
        if (replyState == null)
            return null;

        Object[] replies = replyState.clone();
        for (int i = 0 ; i < replies.length ; ++i)
        {
            if (replies[i] != null && !(replies[i] instanceof accord.messages.Reply))
                replies[i] = null;
        }
        return new SortedListMap<>(nodes, replies, replyCount);
    }

    public int inflightCount()
    {
        return expectingReply.getSetBitCount();
    }

    @Override
    public SortedList<Node.Id> inflight()
    {
        SortedListSet<Node.Id> build = SortedListSet.noneOf(nodes);
        for (int i = expectingReply.nextSetBit(0) ; i >= 0 ; i = expectingReply.nextSetBit(i + 1))
            build.addIndex(i);
        return SortedArrayList.copySorted(build, Node.Id[]::new);
    }

    @Override
    public String toString()
    {
        String describe = describe();
        AbstractTracker<?> tracker = tracker();
        SortedListMap<Node.Id, ?> replies = replies();
        return kind().name() + ':' + txnId
               + " scope:" + scope()
               + " inflight:" + inflight()
               + " tracker:" + tracker.summariseTracker()
               + (describe.isEmpty() ? "" : ' ' + describe)
               + (replies == null ? "" : " replies:" + summariseReplies(replies, 60));
    }

    public static String summariseReplies(@Nonnull SortedListMap<Node.Id, ?> replies, int maxReplyLength)
    {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (int i = 0 ; i < replies.domainSize() ; ++i)
        {
            Object value = replies.getValue(i);
            if (value == null) continue;
            Node.Id key = replies.getKey(i);
            if (first) first = false;
            else sb.append('\n');
            sb.append(key);
            sb.append('=');
            String v = Objects.toString(value);
            if (v.length() > maxReplyLength)
                v = v.substring(0, maxReplyLength) + "...";
            sb.append(v);
        }
        return sb.toString();
    }

    enum LocalExecuteState { PENDING, SUCCESS, TIMEOUT }
    abstract class AbstractLocalExecute extends MapReduceConsumeCommandStores<P, Reply> implements Timeouts.Timeout
    {
        LocalExecuteState state = PENDING;
        Cancellable cancel;
        Timeouts.RegisteredTimeout timeout;

        abstract long expiresAt();
        abstract Cancellable submit();
        abstract void acceptInternal(Reply result, Throwable failure);

        protected AbstractLocalExecute()
        {
            super(AbstractCoordination.this.scope);
            setTracing(AbstractCoordination.this.tracing);
        }

        protected void start()
        {
            markSelfContacted();
            Cancellable cancel = submit();
            long expiresAt = expiresAt();
            Timeouts.RegisteredTimeout timeout = expiresAt <= 0 ? null : node.timeouts().registerAt(this, expiresAt, MICROSECONDS);
            synchronized (this)
            {
                switch (state)
                {
                    case PENDING:
                        this.cancel = cancel;
                        this.timeout = timeout;
                        break;
                    case TIMEOUT:
                        if (cancel != null)
                            cancel.cancel();
                        break;
                    case SUCCESS:
                        if (timeout != null)
                            timeout.cancel();
                        break;
                }
            }
        }

        @Override
        public void accept(Reply result, Throwable failure)
        {
            done();
            executor.executeMaybeImmediately(() -> acceptInternal(result, failure));
        }

        @Override
        public void timeout()
        {
            Cancellable cancel;
            synchronized (this)
            {
                if (state != PENDING)
                    return;

                state = TIMEOUT;
                timeout = null;
                if (this.cancel == null)
                    return;
                cancel = this.cancel;
                this.cancel = null;
            }
            cancel.cancel();
        }

        void done()
        {
            Timeouts.RegisteredTimeout cancel;
            synchronized (this)
            {
                if (state != PENDING)
                    return;

                state = SUCCESS;
                this.cancel = null;
                if (timeout == null)
                    return;
                cancel = timeout;
                timeout = null;
            }
            cancel.cancel();
        }

        @Override
        public int stripe()
        {
            return txnId.hashCode();
        }

        @Nullable
        @Override
        public TxnId primaryTxnId()
        {
            return txnId;
        }

        @Override
        public String reason()
        {
            return "Local " + kind();
        }
    }
}
