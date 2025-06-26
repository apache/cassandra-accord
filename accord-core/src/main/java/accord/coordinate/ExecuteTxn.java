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

import java.util.function.BiConsumer;

import accord.api.Data;
import accord.api.Result;
import accord.api.Timeouts;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Commands;
import accord.local.Commands.CommitOutcome;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SequentialAsyncExecutor;
import accord.local.StoreParticipants;
import accord.messages.Accept;
import accord.messages.Commit;
import accord.messages.MessageType;
import accord.messages.ReadData;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.messages.Request;
import accord.messages.SafeCallback;
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
import accord.utils.UnhandledEnum;
import org.agrona.collections.IntHashSet;

import static accord.api.ProtocolModifiers.Toggles.fastReadsMayBypassSafeStore;
import static accord.api.ProtocolModifiers.Toggles.permitLocalExecution;
import static accord.api.ProtocolModifiers.Toggles.sendOnlyReadStableMessages;
import static accord.coordinate.CoordinationAdapter.Factory.Kind.Standard;
import static accord.coordinate.ExecuteFlag.READY_TO_EXECUTE;
import static accord.coordinate.ExecutePath.FAST;
import static accord.coordinate.ExecutePath.RECOVER;
import static accord.coordinate.ReadCoordinator.Action.Approve;
import static accord.coordinate.ReadCoordinator.Action.ApprovePartial;
import static accord.messages.Commit.Kind.StableFastPath;
import static accord.messages.Commit.Kind.StableMediumPath;
import static accord.messages.Commit.Kind.StableSlowPath;
import static accord.messages.Commit.Kind.StableWithTxnAndDeps;
import static accord.messages.ReadData.CommitOrReadNack.Waiting;
import static accord.primitives.SaveStatus.Stable;
import static accord.primitives.Status.Phase.Execute;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

// TODO (expected): return Waiting from ReadData if not ready to execute, and do not submit more than one speculative retry in this case
// TODO (expected): by default, if we can execute locally, never contact a remote replica regardless of local outcome
public class ExecuteTxn extends ReadCoordinator<ReadReply>
{
    final ExecutePath path;
    final Txn txn;
    final FullRoute<?> route;
    final Ballot ballot;
    final Timestamp executeAt;
    final Deps stableDeps;
    final Deps sendDeps;
    final Topologies allTopologies;
    final CoordinationFlags flags;
    final BiConsumer<? super Result, Throwable> callback;

    private final Participants<?> readScope;
    private Data data;
    private long uniqueHlc;

    ExecuteTxn(Node node, SequentialAsyncExecutor executor, Topologies topologies, FullRoute<?> route, Ballot ballot, ExecutePath path, CoordinationFlags flags, TxnId txnId, Txn txn, Timestamp executeAt, Deps stableDeps, Deps sendDeps, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, executor, topologies.forEpoch(executeAt.epoch()), txnId);
        this.path = path;
        this.txn = txn;
        this.route = route;
        this.ballot = ballot;
        this.allTopologies = topologies;
        this.executeAt = executeAt;
        this.stableDeps = stableDeps;
        this.sendDeps = sendDeps;
        this.flags = flags;
        this.callback = callback;
        this.readScope = txn == null ? route : route.intersecting(txn.keys());
        Invariants.require(!txnId.awaitsOnlyDeps());
        Invariants.require(!txnId.awaitsPreviouslyOwned());
    }

    @Override
    protected void startOnceInitialised()
    {
        Node.Id self = node.id();
        if (permitLocalExecution() && tryIfUniversal(self))
        {
            new LocalExecute(txnId, flags.get(self)).process(node, node.agent().selfExpiresAt(txnId, Execute, MICROSECONDS));
        }
        else if (path == FAST && txnId.hasPrivilegedCoordinator())
        {
            // we can't safely take the fast path via PRIVILEGED_COORDINATOR optimisation if we aren't permitted to execute locally,
            // so we take the MEDIUM or SLOW path
            adapter().propose(node, executor, null, route, txnId.hasMediumPath() ? Accept.Kind.MEDIUM : Accept.Kind.SLOW,
                              Ballot.ZERO, txnId, txn, executeAt, stableDeps, callback);
        }
        else
        {
            super.startOnceInitialised();
        }
    }

    @Override
    protected void start(Iterable<Id> to)
    {
        // TODO (desired): migrate to SortedListSet; or introduce a specialised version for integer keys; or introduce a hash equivalent
        IntHashSet readSet = new IntHashSet();
        to.forEach(i -> readSet.add(i.id));
        // TODO (desired): if READY_TO_EXECUTE send a simple read (skip setting Stable)
        Commit.stableAndRead(node, executor, allTopologies, commitKind(), txnId, txn, route, readScope, executeAt, sendDeps, readSet, flags, sendOnlyReadStableMessages() && path != RECOVER, this);
    }

    private Commit.Kind commitKind()
    {
        switch (path)
        {
            default: throw new UnhandledEnum(path);
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
        boolean alreadySentStable = !(sendOnlyReadStableMessages() && path != RECOVER);
        Request request = Commit.requestTo(to, true, allTopologies, commitKind(), Ballot.ZERO, txnId, txn, route, readScope, executeAt, sendDeps, flags, alreadySentStable, false);
        // we are always sending to a replica in the latest epoch and requesting a read, so onlyContactOldAndReadSet is a redundant parameter
        node.send(to, request, executor, this);
    }

    @Override
    protected Ranges unavailable(ReadReply reply)
    {
        return ((ReadOk)reply).unavailable;
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
        switch (nack)
        {
            default: throw new UnhandledEnum(nack);
            case Waiting:
                return Action.None;

            case Redundant:
            case Rejected:
                callback.accept(null, new Preempted(txnId, route.homeKey()));
                return Action.Aborted;

            case Insufficient:
                // the replica may be missing the original commit, or the additional commit, so send everything
                Commit.stableMaximal(node, from, txn, txnId, executeAt, route, stableDeps);
                // also try sending a read command to another replica, in case they're ready to serve a response
                return Action.TryAlternative;
        }
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        // TODO (expected): if we fail on the fast path and we haven't sent any Stable messages, we should send them now to make recovery easier
        if (failure == null)
        {
            Timestamp executeAt = this.executeAt;
            if (txnId.is(Txn.Kind.Write) && uniqueHlc != 0)
            {
                Invariants.require(uniqueHlc > executeAt.hlc());
                executeAt = new TimestampWithUniqueHlc(executeAt, uniqueHlc);
            }

            Writes writes = txnId.is(Txn.Kind.Write) ? txn.execute(txnId, executeAt, data) : null;
            Result result = txn.result(txnId, executeAt, data);
            adapter().persist(node, executor, allTopologies, route, ballot, flags, txnId, txn, executeAt, stableDeps, writes, result, callback);
        }
        else
        {
            callback.accept(null, failure);
        }
    }

    protected CoordinationAdapter<Result> adapter()
    {
        return node.coordinationAdapter(txnId, Standard);
    }

    @Override
    public String toString()
    {
        return "ExecuteTxn{" +
               "txn=" + txn +
               ", route=" + route +
               '}';
    }

    /**
     * This method is used by LocalExecute to decide if it may fast execute with the provided flags.
     * LocalExecute is treated specially because the privileged coordinator optimisation requires that
     * the coordinator record STABLE before sending further messages to any other replica.
     * So, if the privileged coordinator optimisation is enabled and we _are_ the privileged coordinator
     * taking the fast path, it is unsafe to perform a fast read (that skips updating the Accord state machine)
     * because it would be unsafe to continue to the next phase until the local coordinator has updated its state machine.
     * It woudl be possible to push this work onto the Persist phase, so that we have an equivalent LocalExecute that
     * ensures PREAPPLIED is recorded at the local coordinator before any other replicas are sent Apply, but
     * for now we simply disable this optimisation in this case.
     */
    boolean mayFastExecute(ExecuteFlags flags)
    {
        return flags.contains(READY_TO_EXECUTE) && (!txnId.hasPrivilegedCoordinator() || path != FAST) && fastReadsMayBypassSafeStore(txnId);
    }

    class LocalExecute extends ReadData
    {
        private boolean committed;
        private final SafeCallback<ReadReply> callback;
        private Timeouts.RegisteredTimeout slowTimeout;

        public LocalExecute(TxnId txnId, ExecuteFlags flags)
        {
            super(txnId, route, mayFastExecute(flags) ? txn.intersecting(route, true) : null, ExecuteTxn.this.executeAt, ExecuteTxn.this.executeAt.epoch(), flags);
            this.callback = new SafeCallback<>(executor, ExecuteTxn.this);
        }

        @Override
        public CommitOrReadNack apply(SafeCommandStore safeStore)
        {
            StoreParticipants participants = StoreParticipants.execute(safeStore, route, txnId, minEpoch(), executeAtEpoch);
            SafeCommand safeCommand = safeStore.get(txnId, participants);
            return apply(safeStore, safeCommand, participants);
        }

        @Override
        protected CommitOrReadNack apply(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants)
        {
            if (CommitOutcome.Rejected == Commands.commit(safeStore, safeCommand, participants, Stable, Ballot.ZERO, txnId, route, txn, executeAt, stableDeps, commitKind()))
                return CommitOrReadNack.Rejected;

            return super.apply(safeStore, safeCommand, participants);
        }

        @Override
        public void accept(CommitOrReadNack reply, Throwable failure)
        {
            if (failure == null && reply == null)
            {
                committed = true;
                reply = Waiting;
                long slowAt = node.agent().selfSlowAt(txnId, Execute, MICROSECONDS);
                slowTimeout = node.timeouts().registerAt(new Timeouts.Timeout()
                {
                    @Override public void timeout() { executor.maybeExecuteImmediately(() -> {
                        onSlowResponse(node.id());
                        slowTimeout = null;
                    }); }
                    @Override public int stripe() { return txnId.hashCode(); }
                }, slowAt, MICROSECONDS);
            }
            super.accept(reply, failure);
        }

        @Override
        protected ExecuteOn executeOn()
        {
            return StableThenRead.EXECUTE_ON;
        }

        @Override
        protected boolean cancel()
        {
            if (!super.cancel())
                return false;

            // TODO (desired): if we fail to commit locally we can submit a slow/medium path request
            callback.failure(node.id(), new Timeout(txnId, route.homeKey(), "Could not promptly " + (committed ? "commit to" : "read from") + " local coordinator"));
            return true;
        }

        @Override
        protected void onFailure(CommitOrReadNack failReply, Throwable throwable)
        {
            if (!super.cancel())
                return;

            if (throwable != null)
            {
                reply(null, throwable);
                node.agent().onUncaughtException(throwable);
            }
            else
            {
                reply(failReply, null);
            }
        }


        @Override
        protected void reply(ReadReply reply, Throwable fail)
        {
            if (slowTimeout != null)
            {
                slowTimeout.cancel();
                slowTimeout = null;
            }
            // TODO (expected): execute immediately if already on CommandStore
            if (fail == null) callback.success(node.id(), reply);
            else callback.failure(node.id(), fail);
        }

        @Override
        public ReadType kind() { throw new UnsupportedOperationException(); }

        @Override
        public MessageType type() { throw new UnsupportedOperationException(); }
    }
}
