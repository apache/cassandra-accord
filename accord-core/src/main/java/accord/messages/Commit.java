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

import accord.api.Tracing;
import accord.impl.LocalDelivery;
import accord.local.Commands;
import accord.local.Commands.CommitOutcome;
import accord.local.LoadKeys;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.local.StoreParticipants;
import accord.messages.ReadData.CommitOrReadNack;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.ActiveEpochs;
import accord.topology.Topologies;
import accord.topology.TopologyException;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.async.Cancellable;

import static accord.messages.Commit.Kind.CommitWithTxn;
import static accord.messages.MessageType.StandardMessage.COMMIT_INVALIDATE_REQ;
import static accord.messages.MessageType.StandardMessage.COMMIT_REQ;
import static accord.messages.ReadData.CommitOrReadNack.Kind.InsufficientEpochs;
import static accord.primitives.SaveStatus.Committed;
import static accord.messages.Commit.Kind.StableWithTxnAndDeps;
import static accord.messages.Commit.WithDeps.HasDeps;
import static accord.messages.Commit.WithDeps.NoDeps;
import static accord.messages.Commit.WithTxn.HasNewlyOwnedTxnRanges;
import static accord.messages.Commit.WithTxn.HasTxn;
import static accord.messages.Commit.WithTxn.NoTxn;
import static accord.topology.SelectShards.ALL;

public class Commit extends RouteRequest.WithUnsynced<CommitOrReadNack>
{
    public static class SerializerSupport
    {
        public static Commit create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Kind kind, Ballot ballot, Timestamp executeAt, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute)
        {
            return new Commit(kind, txnId, scope, waitForEpoch, minEpoch, ballot, executeAt, partialTxn, partialDeps, fullRoute);
        }
    }

    public enum WithTxn
    {
        NoTxn, HasNewlyOwnedTxnRanges, HasTxn;
        public PartialTxn select(Txn txn, Participants<?> scope, Topologies topologies, TxnId txnId, Id to)
        {
            switch (this)
            {
                default: throw new UnhandledEnum(this);
                case HasTxn: return txn.intersecting(scope, true);
                case HasNewlyOwnedTxnRanges:
                    if (txnId.epoch() != topologies.currentEpoch())
                    {
                        Ranges coordinateRanges = topologies.getEpoch(txnId.epoch()).rangesForNode(to);
                        Ranges commitRanges = topologies.computeRangesForNode(to);
                        Ranges extraRanges = commitRanges.without(coordinateRanges);
                        if (!extraRanges.isEmpty())
                            return txn.intersecting(scope.without(coordinateRanges), true);
                    }
                case NoTxn:
                    return null;
            }
        }

        public FullRoute<?> select(FullRoute<?> route)
        {
            switch (this)
            {
                default: throw new UnhandledEnum(this);
                case HasTxn: return route;
                case NoTxn:
                case HasNewlyOwnedTxnRanges:
                    return null;
            }
        }
    }

    public enum WithDeps
    {
        NoDeps, HasDeps;

        public PartialDeps select(Deps deps, Participants<?> scope)
        {
            switch (this)
            {
                default: throw new UnhandledEnum(this);
                case HasDeps: return deps.intersecting(scope);
                case NoDeps: return null;
            }
        }
    }

    public enum Kind
    {
        CommitSlowPath(      HasNewlyOwnedTxnRanges, HasDeps, Committed),
        CommitWithTxn (      HasTxn,                 HasDeps, Committed),
        // We retain HasNewlyOwnedTxnRanges for the later eventuality where we permit fast path decisions if the fast quorum is valid for all topologies and everyone agrees on the execution timestamp.
        StableFastPath(      HasNewlyOwnedTxnRanges, HasDeps, SaveStatus.Stable),
        StableMediumPath(    NoTxn,                  HasDeps, SaveStatus.Stable),
        StableSlowPath(      NoTxn,                  NoDeps,  SaveStatus.Stable),
        StableWithTxnAndDeps(HasTxn,                 HasDeps, SaveStatus.Stable);

        public final WithTxn withTxn;
        public final WithDeps withDeps;
        public final SaveStatus saveStatus;

        Kind(WithTxn withTxn, WithDeps withDeps, SaveStatus saveStatus)
        {
            this.withTxn = withTxn;
            this.withDeps = withDeps;
            this.saveStatus = saveStatus;
        }
    }

    public final Kind kind;
    public final Ballot ballot;
    public final Timestamp executeAt;
    // TODO (expected): share keys with partialTxn and partialDeps - in memory and on wire
    private @Nullable PartialTxn partialTxn;
    private @Nullable PartialDeps partialDeps;
    public final @Nullable FullRoute<?> route;


    // TODO (low priority, clarity): cleanup passing of topologies here - maybe fetch them afresh from Node?
    //                               Or perhaps introduce well-named classes to represent different topology combinations

    public Commit(Kind kind, Id to, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, Ballot ballot, Timestamp executeAt, Deps deps)
    {
        super(to, topologies, txnId, route);
        this.ballot = ballot;
        this.kind = kind;
        this.executeAt = executeAt;
        this.partialTxn = kind.withTxn.select(txn, scope, topologies, txnId, to);
        this.partialDeps = kind.withDeps.select(deps, scope);
        this.route = kind.withTxn.select(route);
    }

    protected Commit(Kind kind, TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Ballot ballot, Timestamp executeAt, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.kind = kind;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.partialTxn = partialTxn;
        this.partialDeps = partialDeps;
        this.route = fullRoute;
    }

    public @Nullable PartialTxn partialTxn()
    {
        return partialTxn;
    }

    public @Nullable PartialDeps partialDeps()
    {
        return partialDeps;
    }

    @Override
    public LoadKeys loadKeys()
    {
        return LoadKeys.ASYNC;
    }

    @Override
    public ExecutionKind executionKind()
    {
        return ExecutionKind.COMMIT;
    }

    @Override
    public Cancellable submit()
    {
        return node.commandStores().mapReduceConsume(minEpoch, executeAt.epoch(), this);
    }

    @Override
    public CommitOrReadNack applyInternal(SafeCommandStore safeStore)
    {
        PartialDeps partialDeps = this.partialDeps;
        PartialTxn partialTxn = this.partialTxn;
        if (ifDoneExpectCancelled()) // check cancellation after reading nullable fields
            return null; // we can't throw an exception here else we override any non-exceptional reply informing the reason

        Route<?> route = this.route != null ? this.route : scope;
        StoreParticipants participants = StoreParticipants.execute(safeStore, route, minEpoch, txnId, executeAt.epoch());
        SafeCommand safeCommand = safeStore.get(txnId, participants);

        CommitOutcome outcome = Commands.commit(safeStore, safeCommand, participants, kind.saveStatus, ballot, txnId, route, partialTxn, executeAt, partialDeps, kind);
        switch (outcome)
        {
            default: throw new UnhandledEnum(outcome);
            case Success:
            case Redundant:
                return null;
            case InsufficientEpochs:
                return new CommitOrReadNack(InsufficientEpochs, insufficientEpoch(safeStore, safeCommand, txnId, route));
            case Insufficient:
                Invariants.require(kind != StableWithTxnAndDeps && kind != CommitWithTxn);
                return CommitOrReadNack.InsufficientAndWaiting;
            case Rejected:
                return CommitOrReadNack.Rejected;
        }
    }

    static long insufficientEpoch(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, Route<?> route)
    {
        FullRoute<?> fullRoute = Route.tryCastToFullRoute(route);
        if (route == null) fullRoute = Route.tryCastToFullRoute(safeCommand.current().route());
        Invariants.require(route != null);
        return safeStore.redundantBefore().foldl(fullRoute, (RedundantBefore.Bounds b, Long v, TxnId id) -> {
            if (b.endEpoch < Long.MAX_VALUE && b.endEpoch <= v && !b.isLocallyRetiredOrUnready(id))
                return b.endEpoch - 1;
            return v;
        }, Long.MAX_VALUE, txnId);
    }

    @Override
    public CommitOrReadNack reduce(CommitOrReadNack r1, CommitOrReadNack r2)
    {
        return r1 != null ? r1 : r2;
    }

    @Override
    protected void acceptInternal(CommitOrReadNack reply, Throwable failure)
    {
        partialDeps = null;
        partialTxn = null;
        if (reply == null && failure == null)
        {
            if (isCancelled())
            {
                if (!(replyContext instanceof LocalDelivery<?>))
                    return;
                failure = CANCELLATION_EXCEPTION;
            }
            else
            {
                // TODO (expected): indicate in some way whether a reply is needed
                node.reply(replyTo, replyContext, new ReadData.ReadOk(null, null, 0), null, tracing());
                return;
            }
        }
        node.reply(replyTo, replyContext, reply, failure, tracing());
    }

    @Override
    public MessageType type()
    {
        return COMMIT_REQ;
    }

    @Override
    public String toString()
    {
        return "Commit{kind:" + kind +
               ", txnId: " + txnId +
               ", executeAt: " + executeAt +
               ", deps: " + partialDeps +
               '}';
    }

    public static class Invalidate extends ParticipantsRequest<Participants<?>, Reply>
    {
        public static class SerializerSupport
        {
            public static Invalidate create(TxnId txnId, Participants<?> scope, long waitForEpoch, long invalidateUntilEpoch)
            {
                return new Invalidate(txnId, scope, waitForEpoch, invalidateUntilEpoch);
            }
        }

        public static void commitInvalidate(Node node, TxnId txnId, Participants<?> inform, Timestamp until, @Nullable Tracing tracing)
        {
            commitInvalidate(node, txnId, inform, until.epoch(), tracing);
        }

        public static void commitInvalidate(Node node, TxnId txnId, Participants<?> inform, long untilEpoch, @Nullable Tracing tracing)
        {
            // TODO (expected, safety): this kind of check needs to be inserted in all equivalent methods
            Invariants.require(untilEpoch >= txnId.epoch());
            ActiveEpochs epochs = node.topology().active();
            Invariants.require(epochs.hasAtLeastEpoch(untilEpoch));
            Topologies commitTo;
            try
            {
                commitTo = epochs.preciseEpochsIfExists(inform, txnId.epoch(), untilEpoch, ALL);
            }
            catch (TopologyException e)
            {
                node.agent().onException(e);
                return;
            }
            commitInvalidate(node, commitTo, txnId, inform, tracing);
        }

        public static void commitInvalidate(Node node, Topologies commitTo, TxnId txnId, Participants<?> inform, @Nullable Tracing tracing)
        {
            node.send(commitTo, to -> new Invalidate(to, commitTo, txnId, inform), tracing);
        }

        public final long invalidateUntilEpoch;

        Invalidate(Id to, Topologies topologies, TxnId txnId, Participants<?> scope)
        {
            this(to, topologies, txnId, scope, latestRelevantEpochIndex(to, topologies, scope));
        }

        private Invalidate(Id to, Topologies topologies, TxnId txnId, Participants<?> scope, int latestRelevantIndex)
        {
            super(txnId, computeScope(to, topologies, (Participants)scope, latestRelevantIndex, Participants::overlapping, Participants::with), computeWaitForEpoch(to, topologies, latestRelevantIndex));
            // TODO (expected): make sure we're picking the right upper limit - it can mean future owners that have never witnessed the command are invalidated
            this.invalidateUntilEpoch = topologies.currentEpoch();
        }

        Invalidate(TxnId txnId, Participants<?> scope, long waitForEpoch, long invalidateUntilEpoch)
        {
            super(txnId, scope, waitForEpoch);
            this.invalidateUntilEpoch = invalidateUntilEpoch;
        }

        @Nullable
        @Override
        protected Cancellable submit()
        {
            return node.commandStores().mapReduceConsume(txnId.epoch(), invalidateUntilEpoch, this);
        }

        @Override
        protected void acceptInternal(Reply reply, Throwable failure)
        {
        }

        @Override
        public Reply reduce(Reply o1, Reply o2)
        {
            return null;
        }

        @Override
        public String reason()
        {
            return "CommitInvalidate";
        }

        @Override
        protected boolean abort(Refuse.MinMax refuses)
        {
            return refuses.min == Refuse.ALL;
        }

        @Override
        protected Reply refuseInternal(SafeCommandStore safeStore)
        {
            // don't want to fail to invalidate on other shards because one shard has re-bootstrapped
            return null;
        }

        @Override
        protected Reply applyInternal(SafeCommandStore safeStore)
        {
            // it's fine for this to operate on a non-participating home key, since invalidation is a terminal state,
            // so it doesn't matter if we resurrect a redundant entry
            StoreParticipants participants = StoreParticipants.notAccept(safeStore, scope, txnId);
            Commands.commitInvalidate(safeStore, safeStore.get(txnId, participants), scope);
            return null;
        }

        @Override
        public MessageType type()
        {
            return COMMIT_INVALIDATE_REQ;
        }

        @Override
        public String toString()
        {
            return "CommitInvalidate{txnId: " + txnId + '}';
        }
    }
}
