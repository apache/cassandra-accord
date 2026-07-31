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

package accord.messages;

import javax.annotation.Nullable;

import accord.api.Result;
import accord.api.Result.PersistableResult;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Commands;
import accord.local.LoadKeys;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.messages.Apply.ApplyReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import static accord.api.ProtocolModifiers.fastWritesMayBypassCommandsForKey;
import static accord.api.ProtocolModifiers.fastWritesMayBypassSafeStore;
import static accord.coordinate.ExecuteFlag.READY_TO_EXECUTE;
import static accord.messages.Apply.ApplyReply.Kind.InsufficientEpochs;
import static accord.messages.MessageType.StandardMessage.APPLY_REQ;
import static accord.messages.MessageType.StandardMessage.APPLY_RSP;
import static accord.primitives.SaveStatus.Applied;
import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.SaveStatus.PreApplied;

public class Apply extends RouteRequest<ApplyReply>
{
    public static final Factory FACTORY = Apply::new;
    public static class SerializationSupport
    {
        public static Apply create(TxnId txnId, Ballot ballot, Route<?> scope, long minEpoch, long waitForEpoch, long maxEpoch, Kind kind, Timestamp executeAt, PartialDeps deps, PartialTxn txn, @Nullable FullRoute<?> fullRoute, Writes writes, PersistableResult result, ExecuteFlags flags)
        {
            return new Apply(kind, txnId, ballot, scope, minEpoch, waitForEpoch, maxEpoch, executeAt, deps, txn, fullRoute, writes, result, flags);
        }
    }

    public interface Factory
    {
        Apply create(Kind kind, Id to, Topologies participates, TxnId txnId, Ballot ballot, Route<?> scope, Txn txn, Timestamp executeAt, Deps deps, Writes writes, PersistableResult result, FullRoute<?> fullRoute, ExecuteFlags flags);
    }

    public enum Kind { Minimal, Maximal }

    public final Kind kind;
    public final Ballot ballot;
    public final Timestamp executeAt;
    private PartialDeps deps; // not much benefit in nullability, as we try only to commit to readers, so most recipients will need to receive deps
    private @Nullable PartialTxn txn;
    public final @Nullable FullRoute<?> fullRoute;
    private @Nullable Writes writes;
    private PersistableResult result;
    public final long minEpoch;
    public final long maxEpoch;
    public final ExecuteFlags flags;

    public PartialTxn   txn() { return txn; }
    public PartialDeps deps() { return deps; }
    public Writes    writes() { return writes; }
    public Result    result() { return result; }

    protected Apply(Kind kind, Id to, Topologies participates, TxnId txnId, Ballot ballot, Route<?> sendTo, Txn txn, Timestamp executeAt, Deps deps, Writes writes, PersistableResult result, FullRoute<?> fullRoute, ExecuteFlags flags)
    {
        super(to, participates, sendTo, txnId);
        Invariants.require(txnId.kind() != Txn.Kind.Write || writes != null);
        this.ballot = ballot;
        this.kind = kind;
        this.deps = deps.intersecting(scope);
        this.txn = kind == Kind.Maximal ? txn.intersecting(scope, true) : null;
        this.fullRoute = kind == Kind.Maximal ? fullRoute : null;
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
        this.flags = flags;
        this.minEpoch = participates.oldestEpoch();
        this.maxEpoch = participates.currentEpoch();
    }

    protected Apply(Kind kind, TxnId txnId, Ballot ballot, Route<?> route, long minEpoch, long waitForEpoch, long maxEpoch, Timestamp executeAt, PartialDeps deps, @Nullable PartialTxn txn, @Nullable FullRoute<?> fullRoute, Writes writes, PersistableResult result, ExecuteFlags flags)
    {
        super(txnId, route, waitForEpoch);
        this.kind = kind;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.deps = deps;
        this.txn = txn;
        this.fullRoute = fullRoute;
        this.writes = writes;
        this.result = result;
        this.flags = flags;
        this.minEpoch = minEpoch;
        this.maxEpoch = maxEpoch;
    }

    @Override
    public Cancellable submit()
    {
        if (flags.contains(READY_TO_EXECUTE) && fastWritesMayBypassSafeStore() && kind == Kind.Maximal && !txnId.isSyncPoint() && !txn.read().isUsedForImport())
            return node.commandStores().mapReduceConsume(minEpoch, maxEpoch, this.overrideWithSynchronousApply(this::applyDirect));

        return node.commandStores().mapReduceConsume(minEpoch, maxEpoch, this);
    }

    @Override
    public ApplyReply applyInternal(SafeCommandStore safeStore)
    {
        Route<?> route = bestRoute();
        StoreParticipants participants = StoreParticipants.execute(safeStore, route, minEpoch, txnId, maxEpoch);
        return apply(safeStore, participants);
    }

    @Override
    protected void acceptInternal(ApplyReply reply, Throwable failure)
    {
        txn = null;
        deps = null;
        writes = null;
        result = null;
        if (reply != null || failure != null) super.acceptInternal(reply, failure);
        else Invariants.require(isCancelled());
    }

    class ApplyLink extends AsyncChains.FlatMapLink<Void, ApplyReply>
    {
        final CommandStore commandStore;
        final StoreParticipants participants;

        protected ApplyLink(Head<?> head, CommandStore commandStore, StoreParticipants participants)
        {
            super(head);
            this.commandStore = commandStore;
            this.participants = participants;
        }

        @Override
        public AsyncChain<ApplyReply> apply(Void o)
        {
            return commandStore.chain(Apply.this, safeStore -> {
                return Apply.apply(Applied, safeStore, participants, ballot, txn, txnId, executeAt, deps, participants.route(), writes, result);
            });
        }
    }

    // TODO (desired): always applyDirect reads, whether or not the replica is ready, as reads are no-ops (note: affects linearizability warnings)
    private AsyncChain<ApplyReply> applyDirect(CommandStore commandStore)
    {
        // TODO (required): RangesForEpoch may be stale, must protect against this
        CommandStores.RangesForEpoch storeRanges = commandStore.unsafeGetRangesForEpoch();
        Route<?> route = bestRoute();
        StoreParticipants participants = StoreParticipants.execute(storeRanges, route, minEpoch, txnId, maxEpoch);
        AsyncChain<Void> written = writes == null ? AsyncChains.success(null)
                                                  : writes.applyDirect(commandStore, participants.executes(), txn);

        return written.then(head -> new ApplyLink(head, commandStore, participants));
    }

    public ApplyReply apply(SafeCommandStore safeStore, StoreParticipants participants)
    {
        SaveStatus newSaveStatus = PreApplied;
        if (flags.contains(READY_TO_EXECUTE) && fastWritesMayBypassCommandsForKey())
            newSaveStatus = Applying;
        return apply(newSaveStatus, safeStore, participants);
    }

    private Route<?> bestRoute()
    {
        return fullRoute != null ? fullRoute : scope;
    }

    private ApplyReply apply(SaveStatus newSaveStatus, SafeCommandStore safeStore, StoreParticipants participants)
    {
        PartialTxn txn = this.txn;
        PartialDeps deps = this.deps;
        Writes writes = this.writes;
        PersistableResult result = this.result;
        if (ifDoneExpectCancelled()) // check cancellation after reading nullable fields
            return null;

        return apply(newSaveStatus, safeStore, participants, ballot, txn, txnId, executeAt, deps, bestRoute(), writes, result);
    }

    public static ApplyReply apply(SafeCommandStore safeStore, StoreParticipants participants, Ballot ballot, PartialTxn txn, TxnId txnId, Timestamp executeAt, PartialDeps deps, Route<?> route, Writes writes, PersistableResult result)
    {
        return apply(PreApplied, safeStore, participants, ballot, txn, txnId, executeAt, deps, route, writes, result);
    }

    public static ApplyReply apply(SaveStatus newSaveStatus, SafeCommandStore safeStore, StoreParticipants participants, Ballot ballot, PartialTxn txn, TxnId txnId, Timestamp executeAt, PartialDeps deps, Route<?> route, Writes writes, PersistableResult result)
    {
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        return apply(newSaveStatus, safeStore, safeCommand, participants, ballot, txn, txnId, executeAt, deps, route, writes, result);
    }

    public static ApplyReply apply(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, Ballot ballot, PartialTxn txn, TxnId txnId, Timestamp executeAt, PartialDeps deps, Route<?> route, Writes writes, PersistableResult result)
    {
        return apply(PreApplied, safeStore, safeCommand, participants, ballot, txn, txnId, executeAt, deps, route, writes, result);
    }

    public static ApplyReply apply(SaveStatus newSaveStatus, SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, Ballot ballot, PartialTxn txn, TxnId txnId, Timestamp executeAt, PartialDeps deps, Route<?> route, Writes writes, PersistableResult result)
    {
        switch (Commands.apply(newSaveStatus, safeStore, safeCommand, participants, ballot, txnId, route, executeAt, deps, txn, writes, result))
        {
            default:
            case Success: return ApplyReply.Applied;
            case Redundant: return ApplyReply.Redundant;
            case RaceWithRecovery: return ApplyReply.RaceWithRecovery;
            case Insufficient: return ApplyReply.Insufficient;
            case InsufficientEpochs:
                return new ApplyReply(InsufficientEpochs, Commit.insufficientEpoch(safeStore, safeCommand, txnId, route));
        }
    }

    @Override
    public Unseekables<?> keys()
    {
        if (flags.contains(READY_TO_EXECUTE) && fastWritesMayBypassCommandsForKey())
            return RoutingKeys.EMPTY;
        return super.keys();
    }

    @Override
    public ApplyReply reduce(ApplyReply a, ApplyReply b)
    {
        return ApplyReply.reduce(a, b);
    }

    @Override
    public LoadKeys loadKeys()
    {
        // TODO (expected): need to guarantee execution order then can make this ASYNC
        return LoadKeys.SYNC;
    }

    @Override
    public MessageType type()
    {
        return APPLY_REQ;
    }

    // TODO (expected): merge with CommitOrReadNack?
    public static class ApplyReply implements Reply
    {
        public enum Kind { Applied, Redundant, Insufficient, InsufficientEpochs, RaceWithRecovery }
        public static final ApplyReply Applied = new ApplyReply(Kind.Applied);
        public static final ApplyReply Redundant = new ApplyReply(Kind.Redundant);
        public static final ApplyReply Insufficient = new ApplyReply(Kind.Insufficient);
        public static final ApplyReply RaceWithRecovery = new ApplyReply(Kind.RaceWithRecovery);
        private static final ApplyReply[] byKind = new ApplyReply[] { Applied, Redundant, Insufficient, null, RaceWithRecovery };
        public static ApplyReply lookupByKind(Kind kind)
        {
            return byKind[kind.ordinal()];
        }

        public final ApplyReply.Kind kind;
        public final long minEpoch;
        public ApplyReply(ApplyReply.Kind kind)
        {
            this(kind, 0);
        }

        public ApplyReply(ApplyReply.Kind kind, long minEpoch)
        {
            this.kind = kind;
            this.minEpoch = minEpoch;
            Invariants.requireArgument(minEpoch == 0 || kind == InsufficientEpochs);
        }

        @Override
        public MessageType type()
        {
            return APPLY_RSP;
        }

        @Override
        public String toString()
        {
            return "Apply" + kind.name();
        }

        @Override
        public boolean isFinal()
        {
            return this != Insufficient;
        }

        public final long minEpoch()
        {
            Invariants.require(kind == InsufficientEpochs);
            return minEpoch;
        }

        public static ApplyReply reduce(ApplyReply a, ApplyReply b)
        {
            if (a == null || b == null)
                return a != null ? a : b;

            int c = a.kind.compareTo(b.kind);
            if (c > 0 || (c == 0 && a.kind != InsufficientEpochs)) return a;
            else if (c < 0) return b;
            else return a.minEpoch <= b.minEpoch ? a : b;
        }
    }

    @Override
    public String toString()
    {
        return "Apply{kind:" + kind +
               ", txnId:" + txnId +
               ", deps:" + deps +
               ", executeAt:" + executeAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }
}
