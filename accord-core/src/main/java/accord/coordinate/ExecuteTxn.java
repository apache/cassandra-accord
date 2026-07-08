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

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import accord.api.Data;
import accord.api.Result;
import accord.api.Timeouts;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.coordinate.tracking.QuorumIdTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Commands;
import accord.local.Commands.CommitOutcome;
import accord.local.LoadKeys;
import accord.local.LogUnavailableException;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.api.ExclusiveAsyncExecutor;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.SafeCommandsForKey;
import accord.messages.Accept;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.InformDurable;
import accord.messages.MessageType;
import accord.messages.ReadData;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadTxnData;
import accord.messages.StableThenRead;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TimestampWithUniqueHlc;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedListSet;
import accord.utils.UnhandledEnum;
import org.agrona.collections.IntHashSet;

import static accord.api.ProtocolModifiers.coordinatorBacklogExecution;
import static accord.api.ProtocolModifiers.replicaExecuteDistributedPersist;
import static accord.api.ProtocolModifiers.recoverReads;
import static accord.api.ProtocolModifiers.fastReadExecutionMayResendTxn;
import static accord.api.ProtocolModifiers.fastReadsMayBypassSafeStore;
import static accord.api.ProtocolModifiers.permitCoordinatorLocalExecution;
import static accord.api.ProtocolModifiers.sendMinimal;
import static accord.api.ProtocolModifiers.sendNoStableIfFastExec;
import static accord.api.ProtocolModifiers.sendOnlyReadStableMessages;
import static accord.coordinate.Coordination.CoordinationKind.Execute;
import static accord.coordinate.CoordinationAdapter.Factory.Kind.Standard;
import static accord.coordinate.ExecuteFlag.READY_TO_EXECUTE;
import static accord.coordinate.ExecutePath.EPHEMERAL;
import static accord.coordinate.ExecutePath.FAST;
import static accord.coordinate.ExecutePath.RECOVER;
import static accord.coordinate.ReadCoordinator.Action.Approve;
import static accord.coordinate.ReadCoordinator.Action.ApprovePartial;
import static accord.local.CommandSummaries.SummaryStatus.STABLE;
import static accord.messages.Commit.Kind.StableFastPath;
import static accord.messages.Commit.Kind.StableMediumPath;
import static accord.messages.Commit.Kind.StableSlowPath;
import static accord.messages.Commit.Kind.StableWithTxnAndDeps;
import static accord.messages.MessageType.StandardMessage.READ_REQ;
import static accord.messages.MessageType.StandardMessage.STABLE_THEN_READ_REQ;
import static accord.messages.ReadData.CommitOrReadNack.Waiting;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.SaveStatus.Stable;
import static accord.primitives.Status.Durability.AllQuorums;
import static accord.primitives.Status.Durability.DurablyStable;
import static accord.primitives.TxnId.Cardinality.SingleKey;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

// TODO (expected): return Waiting from ReadData if not ready to execute, and do not submit more than one speculative retry in this case
// TODO (expected): by default, if we can execute locally, never contact a remote replica regardless of local outcome
public class ExecuteTxn extends ReadCoordinator<Result, ReadReply>
{
    class StableTracker extends QuorumIdTracker implements CallbackExclusive<ReadReply>, Callback<ReadReply>
    {
        private boolean isDone;
        private boolean informOnSuccess;
        private boolean hasOnlyRedundant;

        public StableTracker(Topologies topologies)
        {
            super(topologies);
        }

        @Override
        public final void onSuccess(Node.Id from, ReadReply reply)
        {
            CallbackExclusive.onSuccess(executor, unsafeToReplyImmediately, this, from, reply);
        }

        @Override
        public final void onSlow(Node.Id from)
        {
            CallbackExclusive.onSlow(executor, unsafeToReplyImmediately, this, from);
        }

        @Override
        public final void onFailure(Node.Id from, Throwable failure)
        {
            CallbackExclusive.onFailure(executor, unsafeToReplyImmediately, this, from, failure);
        }

        @Override
        public void onSuccessExclusive(Id from, ReadReply reply)
        {
            hasOnlyRedundant &= reply == CommitOrReadNack.Redundant;
            if ((reply.isOk() || reply == Waiting) && RequestStatus.Success == recordSuccess(from) && informOnSuccess)
                informStable();
        }

        @Override
        public void onFailureExclusive(Id from, Throwable failure)
        {
        }

        @Override
        public void onCallbackFailureExclusive(Id from, @Nullable Throwable failure)
        {
            node.agent().onException(failure);
        }

        void informStable()
        {
            Invariants.require(hasReachedQuorum());
            isDone = true;
            if (hasOnlyRedundant) InformDurable.informDefault(node, topologies, txnId, route, ballot, executeAt, stableDeps, AllQuorums, tracing);
            else InformDurable.informHome(node, topologies, txnId, route, executeAt, DurablyStable, tracing);
        }

        void maybeInformStable()
        {
            if (!isDone && stable.hasReachedQuorum())
                informStable();
        }

        void setDone()
        {
            isDone = true;
        }

        void informStableOnceQuorum()
        {
            if (!isDone)
            {
                if (stable.hasReachedQuorum()) informStable();
                else informOnSuccess = true;
            }
        }
    }

    final ExecutePath path;
    final Txn txn;
    final FullRoute<?> route;
    final Ballot ballot;
    final Timestamp executeAt;
    final Deps stableDeps;
    final Deps sendDeps;
    final Topologies allTopologies;
    final CoordinationFlags flags;
    private final StableTracker stable;
    private @Nullable SortedListSet<Node.Id> unstableFastReads;

    private Data data;
    private long uniqueHlc;
    private boolean isPrivilegedVoteCommitting;

    ExecuteTxn(Node node, ExclusiveAsyncExecutor executor, Topologies topologies, FullRoute<?> route, Ballot ballot, ExecutePath path, CoordinationFlags flags, TxnId txnId, Txn txn, Timestamp executeAt, Deps stableDeps, Deps sendDeps, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, executor, topologies.forEpoch(executeAt.epoch()), txnId, route, callback);
        if (!ballot.equals(Ballot.ZERO))
        {
            path = RECOVER;
            flags.setNoWait();
        }
        this.path = path;
        this.txn = txn;
        this.route = route;
        this.ballot = ballot;
        this.allTopologies = topologies;
        this.executeAt = executeAt;
        this.stableDeps = stableDeps;
        this.sendDeps = sendDeps;
        this.flags = flags;
        this.stable = new StableTracker(topologies.forEpochs(txnId.epoch(), executeAt.epoch()));
        Invariants.require(!txnId.awaitsOnlyDeps());
        Invariants.require(!txnId.awaitsPreviouslyOwned());
    }

    @Override
    protected void startOnceInitialised()
    {
        node.agent().coordinatorEvents().onExecuting(txnId, ballot, stableDeps, path);
        Node.Id self = node.id();
        if (permitCoordinatorLocalExecution() && tryIfUniversal(self))
        {
            isPrivilegedVoteCommitting = txnId.hasPrivilegedCoordinator() && path == FAST;
            ExecuteFlags executeFlags = flags.get(self);
            /*
              This decides if our local execution may fast execute with the provided flags.
              LocalExecute is treated specially because the privileged coordinator optimisation requires that
              the coordinator record STABLE before sending further messages to any other replica.
              So, if the privileged coordinator optimisation is enabled and we _are_ the privileged coordinator
              taking the fast path, it is unsafe to perform a fast read (that skips updating the Accord state machine)
              because it would be unsafe to continue to the next phase until the local coordinator has updated its state machine.
              It woudl be possible to push this work onto the Persist phase, so that we have an equivalent LocalExecute that
              ensures PREAPPLIED is recorded at the local coordinator before any other replicas are sent Apply, but
              for now we simply disable this optimisation in this case.
             */
            boolean mayFastExecute = !isPrivilegedVoteCommitting
                                     && executeFlags.contains(READY_TO_EXECUTE)
                                     && sendNoStableIfFastExec()
                                     && fastReadsMayBypassSafeStore(txnId);

            if (mayFastExecute)
                markUnstableFastRead(node.id());
            new LocalExecute(txnId, executeFlags, mayFastExecute).process(node, node.agent().selfExpiresAt(txnId, READ_REQ, MICROSECONDS));
            if (!isPrivilegedVoteCommitting)
                start(Collections.emptyList(), Collections.singletonList(node.id()));
        }
        else if (path == FAST && txnId.hasPrivilegedCoordinator())
        {
            // we can't safely take the fast path via PRIVILEGED_COORDINATOR optimisation if we aren't permitted to execute locally,
            // so we take the MEDIUM or SLOW path
            adapter().propose(node, executor, null, route, txnId.hasMediumPath() ? Accept.Kind.MEDIUM : Accept.Kind.SLOW,
                              Ballot.ZERO, txnId, txn, executeAt, stableDeps, takeCallback());
        }
        else
        {
            super.startOnceInitialised();
        }
    }

    @Override
    protected void start(List<Id> readFrom)
    {
        start(readFrom, readFrom);
    }

    protected void start(List<Id> sendReadTo, List<Id> readingFrom)
    {
        // TODO (desired): migrate to SortedListSet; or introduce a specialised version for integer keys; or introduce a hash equivalent
        Topologies all = allTopologies;
        for (int i = 0, size = sendReadTo.size() ; i < size ; ++i)
        {
            Node.Id to = sendReadTo.get(i);
            ExecuteFlags flags = this.flags.get(to);
            boolean sendUnstable = flags.contains(READY_TO_EXECUTE) && sendNoStableIfFastExec() && path != RECOVER;
            if (sendUnstable) sendUnstableRead(to, flags);
            else
            {
                Commit.Kind kind = commitKind(to);
                Invariants.require(kind.compareTo(StableFastPath) >= 0);
                sendStableRead(to, kind);
            }
        }

        boolean sendOnlyReadStableMessages = flags.isAnyReadyToExecute() && sendOnlyReadStableMessages(txnId);
        if (sendOnlyReadStableMessages && (all.size() == 1 || all.current().nodes().containsAll(all.nodes())))
            return;

        IntHashSet readSet = new IntHashSet();
        readingFrom.forEach(i -> readSet.add(i.id));
        SortedArrayList<Id> contact = all.nodes().without(all::isFaulty);
        for (Node.Id to : contact)
        {
            if (readSet.contains(to.id))
                continue;

            if (sendOnlyReadStableMessages && all.current().contains(to))
                continue;

            sendStableOnly(to);
        }
    }

    private void sendStableOnly(Node.Id to)
    {
        Commit send = new Commit(commitKind(to), to, allTopologies, txnId, txn, route, ballot, executeAt, stableDeps);
        boolean addCallback = allTopologies.size() == 1 || stable.nodes().contains(to);
        if (addCallback) node.send(to, send, executor, stable, tracing);
        else node.send(to, send, tracing);
    }

    private void sendUnstableRead(Node.Id to, ExecuteFlags flags)
    {
        Txn sendTxn = null;
        Timestamp sendExecuteAt = null;
        if (flags.contains(READY_TO_EXECUTE) && fastReadExecutionMayResendTxn() && fastReadsMayBypassSafeStore(txnId))
        {
            sendTxn = txn;
            sendExecuteAt = executeAt;
            markUnstableFastRead(to);
        }
        node.send(to, new ReadTxnData(to, allTopologies, txnId, route, sendTxn, sendExecuteAt, executeAt.epoch(), flags), executor, this, tracing);
    }

    private void markUnstableFastRead(Node.Id to)
    {
        if (unstableFastReads == null)
            unstableFastReads = SortedListSet.noneOf(allTopologies.current().nodes());
        unstableFastReads.add(to);
    }

    private void sendStableRead(Node.Id to, Commit.Kind kind)
    {
        node.send(to, new StableThenRead(kind, to, allTopologies, txnId, txn, route, executeAt, stableDeps), executor, this, tracing);
    }

    private void sendMaximal(Node.Id to)
    {
        Commit send = new Commit(StableWithTxnAndDeps, to, allTopologies, txnId, txn, route, ballot, executeAt, stableDeps);
        node.send(to, send, executor, stable, tracing);
    }

    private Commit.Kind commitKind(Node.Id to)
    {
        // we MUST send StableFastPath to self to ensure we use the privileged coordinator fast commit machinery that rejects if we have witnessed a future ballot
        if (!sendMinimal() && (path != FAST || !to.equals(node.id())))
            return StableWithTxnAndDeps;

        switch (path)
        {
            default: throw UnhandledEnum.unknown(path);
            case EPHEMERAL: throw UnhandledEnum.invalid(EPHEMERAL);
            case BACKLOG:
            case FAST:    return StableFastPath;
            case MEDIUM:  return StableMediumPath;
            case SLOW:    return StableSlowPath;
            case RECOVER: return StableWithTxnAndDeps;
        }
    }

    @Override
    public void contact(Id to)
    {
        ExecuteFlags flags = this.flags.get(to);
        boolean sendUnstable = !sendOnlyReadStableMessages(txnId) || path == RECOVER || flags.contains(READY_TO_EXECUTE);
        if (sendUnstable) sendUnstableRead(to, flags);
        else sendStableRead(to, commitKind(to));
    }

    @Override
    protected Ranges unavailable(ReadReply reply)
    {
        return ((ReadOk)reply).unavailable;
    }

    @Override
    public void onSuccessExclusive(Id from, ReadReply reply)
    {
        super.onSuccessExclusive(from, reply);
        // we forward all not-oks for consideration, and any stable fast path read oks;
        // it's up to the stable tracker to decide what to do with them, we're just filtering out unstable Acks
        if (!reply.isOk() || unstableFastReads == null || !unstableFastReads.contains(from))
            stable.onSuccess(from, reply);
    }

    @Override
    protected Action process(Id from, ReadReply reply)
    {
        if (reply.isOk())
        {
            ReadOk ok = ((ReadOk) reply);
            Data next = ok.data;
            if (next != null)
                data = data == null ? next : data.merge(next);

            if (txnId.is(Txn.Kind.Write) && ok.uniqueHlc > 0)
            {
                Invariants.require(ok.uniqueHlc > executeAt.hlc());
                uniqueHlc = Math.max(uniqueHlc, ok.uniqueHlc);
            }
            return ok.unavailable == null ? Approve : ApprovePartial;
        }

        CommitOrReadNack nack = (CommitOrReadNack) reply;
        switch (nack.kind)
        {
            default: throw UnhandledEnum.unknown(nack.kind);
            case InsufficientEpochs: throw UnhandledEnum.invalid(nack.kind);
            case Waiting:
                if (from.id == node.id().id && isPrivilegedVoteCommitting)
                {
                    start(Collections.emptyList(), Collections.singletonList(node.id()));
                    isPrivilegedVoteCommitting = false;
                }
                return Action.None;

            case Redundant:
                return Action.Reject;

            case Rejected:
                invokeCallback(null, Preempted.preempted(node.agent(), txnId, route.homeKey()));
                return Action.Aborted;

            case InsufficientAndWaiting:
                // the replica may be missing the original commit, or any additional commit, so send everything
                sendMaximal(from);
                // also try sending a read command to another replica, in case they're ready to serve a response
                return Action.TryAlternative;
        }
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (success == Success.Success)
        {
            stable.setDone();
            Timestamp executeAt = this.executeAt;
            if (txnId.is(Txn.Kind.Write) && uniqueHlc != 0)
            {
                Invariants.require(uniqueHlc > executeAt.hlc());
                executeAt = new TimestampWithUniqueHlc(executeAt, uniqueHlc);
            }

            // Always compute Writes before Result to provide integrations with a predictable invocation order
            // in case there is shared state between Result and Update.
            Writes writes = txnId.is(Txn.Kind.Write) ? txn.execute(txnId, executeAt, data) : null;
            Result result = txn.result(txnId, executeAt, data);
            adapter().persist(node, executor, allTopologies, route, ballot, flags, txnId, txn, executeAt, stableDeps, writes, result, takeCallback());
        }
        else
        {
            if (!isPrivilegedVoteCommitting)
            {
                if (txnId.isSomeRead() && !recoverReads())
                {
                    adapter().persist(node, executor, allTopologies, route, ballot, flags, txnId, txn, executeAt, stableDeps, null, null, null);
                }
                else
                {
                    stable.informStableOnceQuorum();
                    if (sendOnlyReadStableMessages(txnId))
                    {
                        // send additional stable messages to record the transaction outcome
                        if (!candidates.isEmpty())
                        {
                            for (int i = 0, size = candidates.size() ; i < size ; ++i)
                            {
                                Node.Id to = candidates.get(i);
                                sendStableOnly(to);
                            }
                        }
                        if (unstableFastReads != null)
                        {
                            for (Node.Id to : unstableFastReads)
                                sendStableOnly(to);
                        }
                    }
                }
            }
            invokeCallback(null, failure);
        }
    }

    @Override
    public void onSlowExclusive(Id from)
    {
        // send stable messages to everyone not yet contacted, and then inform decided, to avoid unnecessary recoveries
        stable.maybeInformStable();
        super.onSlowExclusive(from);
    }

    @Override
    public void onFailureExclusive(Id from, Throwable failure)
    {
        if (isPrivilegedVoteCommitting && from.id == node.id().id && !isDone()) finishWithFailure(failure);
        else super.onFailureExclusive(from, failure);
    }

    protected CoordinationAdapter<Result> adapter()
    {
        return node.coordinationAdapter(txnId, Standard);
    }

    private void onExternalSuccess(Result result)
    {
        executor.executeMaybeImmediately(() -> {
            if (!trySetDone())
                return;

            stable.setDone();
            BiConsumer<? super Result, Throwable> callback = tryTakeCallback();
            if (callback != null)
                callback.accept(result, null);
        });
    }

    public void onRemoteSuccess(Result result)
    {
        if (tracing != null)
            tracing.trace(null, "Remote Success");
        onExternalSuccess(result);
    }

    public void onLocalDirectSuccess(Timestamp executeAt, Writes writes, Result result)
    {
        if (tracing != null)
            tracing.trace(null, "Local Direct Success");

        if (replicaExecuteDistributedPersist())
        {
            executor.executeMaybeImmediately(() -> {
                if (!trySetDone())
                    return;

                stable.setDone();
                adapter().persist(node, executor, allTopologies, route, ballot, flags, txnId, txn, executeAt, stableDeps, writes, result, takeCallback());
            });
        }
        else
        {
            onExternalSuccess(result);
        }
    }

    @Override
    public String toString()
    {
        return "ExecuteTxn{" +
               "txnId=" + txnId +
               ", txn=" + txn +
               ", route=" + route +
               '}';
    }

    class LocalExecute extends ReadData
    {
        final boolean mayFastExecute;
        private boolean committed;
        private Timeouts.RegisteredTimeout slowTimeout;

        public LocalExecute(TxnId txnId, ExecuteFlags flags, boolean mayFastExecute)
        {
            super(txnId, route, mayFastExecute ? txn.intersecting(route, true) : null, ExecuteTxn.this.executeAt, ExecuteTxn.this.executeAt.epoch(), flags);
            this.mayFastExecute = mayFastExecute;
        }

        @Override
        public LoadKeys loadKeys()
        {
            return LoadKeys.SYNC;
        }

        @Override
        protected boolean mayFastExecute()
        {
            return mayFastExecute;
        }

        @Override
        public CommitOrReadNack applyInternal(SafeCommandStore safeStore)
        {
            StoreParticipants participants = StoreParticipants.execute(safeStore, route, txnId, minEpoch(), executeAtEpoch);
            SafeCommand safeCommand = safeStore.get(txnId, participants);
            return applyInternal(safeStore, safeCommand, participants);
        }

        @Override
        protected CommitOrReadNack applyInternal(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants)
        {
            if (CommitOutcome.Rejected == Commands.commit(safeStore, safeCommand, participants, Stable, Ballot.ZERO, txnId, route, txn, executeAt, stableDeps, commitKind(node.id())))
                return CommitOrReadNack.Rejected;

            if (coordinatorBacklogExecution(ballot) && txnId.is(SingleKey) && txnId.is(Key))
            {
                SafeCommandsForKey safeCfk = safeStore.ifLoadedAndInitialised(scope.get(0).asRoutingKey());
                if (safeCfk != null)
                {
                    CommandsForKey cfk = safeCfk.current();
                    int i = cfk.unappliedCommittedIndexOf(executeAt);
                    if (i < 0) i = -1 -i;
                    else ++i;

                    boolean executeBacklog = false;
                    if (i < cfk.committedSize())
                    {
                        TxnInfo next = cfk.get(i);
                        executeBacklog = next.is(STABLE) && next.is(SingleKey);
                    }
                    if (executeBacklog) safeCfk.overrideSink(node.executeBacklogSink());
                    else safeCfk.overrideSink(null);
                }
            }

            return super.applyInternal(safeStore, safeCommand, participants);
        }

        @Override
        protected CommitOrReadNack refuseInternal(SafeCommandStore safeStore)
        {
            if (isPrivilegedVoteCommitting)
                throw new LogUnavailableException();
            return super.refuseInternal(safeStore);
        }

        @Override
        public void accept(CommitOrReadNack reply, Throwable failure)
        {
            if (reply != null && reply.kind == CommitOrReadNack.Kind.Waiting)
            {
                committed = true;
                long slowAt = node.agent().selfSlowAt(txnId, READ_REQ, MICROSECONDS);
                // TODO (desired): avoid converting to MICROSECONDS again here
                if (slowAt >= 0)
                {
                    slowTimeout = node.timeouts().registerAt(new Timeouts.Timeout()
                    {
                        @Override public void timeout() { executor.executeMaybeImmediately(() -> {
                            onSlow(node.id());
                            slowTimeout = null;
                        }); }
                        @Override public int stripe() { return txnId.hashCode(); }
                    }, slowAt, MICROSECONDS);
                }
            }
            else if (failure == null)
            {
                committed = reply == null && !mayFastExecute;
            }
            super.accept(reply, failure);
        }

        @Override
        protected ExecuteOn executeOn()
        {
            return StableThenRead.EXECUTE_ON;
        }

        @Override
        protected boolean timeoutInternal()
        {
            if (!super.timeoutInternal())
                return false;

            cancelSlowTimeout();
            if (committed)
                ExecuteTxn.this.onFailure(node.id(), new Timeout(txnId, route.homeKey(), "Could not promptly read from local coordinator"));
            else
                ExecuteTxn.this.onFailure(node.id(), new Timeout(txnId, route.homeKey(), "Could not promptly commit to local coordinator"));
            return true;
        }

        @Override
        protected void reply(ReadReply reply, Throwable fail)
        {
            if (reply != Waiting)
                cancelSlowTimeout();

            // TODO (expected): execute immediately if already on CommandStore
            if (fail == null) ExecuteTxn.this.onSuccess(node.id(), reply);
            else ExecuteTxn.this.onFailure(node.id(), fail);
        }

        private void cancelSlowTimeout()
        {
            Timeouts.RegisteredTimeout cancel = slowTimeout;
            if (cancel != null)
            {
                cancel.cancel();
                slowTimeout = null;
            }
        }

        @Override
        public ReadType kind() { throw new UnsupportedOperationException(); }

        @Override
        public MessageType type() { return STABLE_THEN_READ_REQ; }

        @Override
        public String reason()
        {
            return "Local Execute";
        }
    }

    @Override
    public CoordinationKind kind()
    {
        return Execute;
    }

    @Override
    public Participants<?> scope()
    {
        return route;
    }

    @Override
    public String describe()
    {
        // TODO (desired): summarise what data replies we have
        return "path=" + path +
               ", ballot=" + ballot +
               ", executeAt=" + executeAt +
               ", flags=" + flags +
               ", uniqueHlc=" + uniqueHlc +
               ", isPrivilegedVoteCommitting=" + isPrivilegedVoteCommitting;
    }
}
