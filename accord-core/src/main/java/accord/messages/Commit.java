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

import java.util.function.BiPredicate;
import javax.annotation.Nullable;

import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.TriFunction;
import org.agrona.collections.IntHashSet;

import static accord.local.SaveStatus.Committed;
import static accord.messages.Commit.Kind.StableWithTxnAndDeps;
import static accord.messages.Commit.WithDeps.HasDeps;
import static accord.messages.Commit.WithDeps.NoDeps;
import static accord.messages.Commit.WithTxn.HasNewlyOwnedTxnRanges;
import static accord.messages.Commit.WithTxn.HasTxn;
import static accord.messages.Commit.WithTxn.NoTxn;

public class Commit extends TxnRequest<CommitOrReadNack>
{
    public static class SerializerSupport
    {
        public static Commit create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Kind kind, Ballot ballot, Timestamp executeAt, Seekables<?, ?> keys, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute, @Nullable ReadData readData)
        {
            return new Commit(kind, txnId, scope, waitForEpoch, ballot, executeAt, keys, partialTxn, partialDeps, fullRoute, readData);
        }
    }

    public final Kind kind;
    public final Ballot ballot;
    public final Timestamp executeAt;
    public final Seekables<?, ?> keys;
    // TODO (expected): share keys with partialTxn and partialDeps - in memory and on wire
    public final @Nullable PartialTxn partialTxn;
    public final @Nullable PartialDeps partialDeps;
    public final @Nullable FullRoute<?> route;
    public final @Nullable ReadData readData;

    public enum WithTxn  { NoTxn,  HasNewlyOwnedTxnRanges, HasTxn }
    public enum WithDeps { NoDeps, HasDeps }

    public enum Kind
    {
        CommitSlowPath(      HasNewlyOwnedTxnRanges, HasDeps, Committed),
        CommitWithTxn (      HasTxn,                 HasDeps, Committed),
        // We retain HasNewlyOwnedTxnRanges for the later eventuality where we permit fast path decisions if the fast quorum is valid for all topologies and everyone agrees on the execution timestamp.
        StableFastPath(      HasNewlyOwnedTxnRanges, HasDeps, SaveStatus.Stable),
        StableSlowPath(      NoTxn,                  NoDeps,  SaveStatus.Stable),
        StableWithTxnAndDeps(HasTxn,                 HasDeps, SaveStatus.Stable);

        final WithTxn withTxn;
        final WithDeps withDeps;
        final SaveStatus saveStatus;

        Kind(WithTxn withTxn, WithDeps withDeps, SaveStatus saveStatus)
        {
            this.withTxn = withTxn;
            this.withDeps = withDeps;
            this.saveStatus = saveStatus;
        }
    }

    // TODO (low priority, clarity): cleanup passing of topologies here - maybe fetch them afresh from Node?
    //                               Or perhaps introduce well-named classes to represent different topology combinations

    public Commit(Kind kind, Id to, Topology coordinateTopology, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, Ballot ballot, Timestamp executeAt, Deps deps, ReadData read)
    {
        this(kind, to, coordinateTopology, topologies, txnId, txn, route, ballot, executeAt, deps, read == null ? null : (u1, u2, u3) -> read);
    }

    public Commit(Kind kind, Id to, Topology coordinateTopology, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, Ballot ballot, Timestamp executeAt, Deps deps, @Nullable Participants<?> readScope)
    {
        this(kind, to, coordinateTopology, topologies, txnId, txn, route, ballot, executeAt, deps, readScope != null ? new ReadTxnData(to, topologies, txnId, readScope, executeAt.epoch()) : null);
    }

    public Commit(Kind kind, Id to, Topology coordinateTopology, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, Ballot ballot, Timestamp executeAt, Deps deps, TriFunction<Txn, PartialRoute<?>, PartialDeps, ReadData> toExecuteFactory)
    {
        super(to, topologies, route, txnId);
        this.ballot = ballot;

        FullRoute<?> sendRoute = null;
        PartialTxn partialTxn = null;
        if (kind.withTxn == HasTxn)
        {
            // TODO (desired): only includeQuery if isHome; this affects state eviction and is low priority given size in C*
            partialTxn = txn.slice(scope.covering(), true);
            sendRoute = route;
        }
        else if (kind.withTxn == HasNewlyOwnedTxnRanges && executeAt.epoch() != txnId.epoch())
        {
            Ranges coordinateRanges = coordinateTopology.rangesForNode(to);
            Ranges executeRanges = topologies.computeRangesForNode(to);
            Ranges extraRanges = executeRanges.subtract(coordinateRanges);
            if (!extraRanges.isEmpty())
                partialTxn = txn.slice(scope.covering().subtract(coordinateRanges), coordinateRanges.contains(route.homeKey()));
        }

        this.kind = kind;
        this.executeAt = executeAt;
        this.keys = txn.keys().slice(scope.covering());
        this.partialTxn = partialTxn;
        this.partialDeps = deps.slice(scope.covering());
        this.route = sendRoute;
        this.readData = toExecuteFactory == null ? null : toExecuteFactory.apply(partialTxn != null ? partialTxn : txn, scope, partialDeps);
    }

    protected Commit(Kind kind, TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Ballot ballot, Timestamp executeAt, Seekables<?, ?> keys, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute, @Nullable ReadData readData)
    {
        super(txnId, scope, waitForEpoch);
        this.kind = kind;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.keys = keys;
        this.partialTxn = partialTxn;
        this.partialDeps = partialDeps;
        this.route = fullRoute;
        this.readData = readData;
    }

    public static void commitMinimal(Node node, Topologies coordinateEpochOnly, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps unstableDeps, Callback<ReadReply> callback)
    {
        Invariants.checkArgument(coordinateEpochOnly.size() == 1);
        // we want to send to everyone, and we want to include all of the relevant data, but we stabilise on the coordination epoch replica responses
        Topology coordinates = coordinateEpochOnly.forEpoch(txnId.epoch());
        Topologies all = coordinateEpochOnly;
        if (txnId.epoch() != executeAt.epoch())
            all = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());

        send(null, (i1, i2) -> true, null, node, coordinates, coordinates, all, Kind.CommitSlowPath, ballot,
             txnId, txn, route, executeAt, unstableDeps, callback);
    }

    // TODO (desired, efficiency): do not commit if we're already ready to execute (requires extra info in Accept responses)
    public static void stableAndRead(Node node, Topologies executeEpochOnly, Kind kind, TxnId txnId, Txn txn, FullRoute<?> route, Participants<?> readScope, Timestamp executeAt, Deps stableDeps, IntHashSet readSet, Callback<ReadReply> callback)
    {
        Topologies all = executeEpochOnly;
        if (txnId.epoch() != executeAt.epoch())
            all = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());

        Topology executes = executeEpochOnly.forEpoch(executeAt.epoch());
        Topology coordinates = all.forEpoch(txnId.epoch());

        send(readSet, (set, id) -> set.contains(id.id), readScope, node, coordinates, executes, all, kind, Ballot.ZERO,
             txnId, txn, route, executeAt, stableDeps, callback);
    }

    private static <P> void send(P param, BiPredicate<P, Id> shouldRegisterCallback, @Nullable Participants<?> readScopeIfCallback,
                                 Node node, Topology coordinates, Topology primary, Topologies all, Kind kind, Ballot ballot,
                                 TxnId txnId, @Nullable Txn txn, FullRoute<?> route, Timestamp executeAt, @Nullable Deps deps,
                                 Callback<ReadReply> callback)
    {
        for (Node.Id to : primary.nodes())
        {
            boolean registerCallback = shouldRegisterCallback.test(param, to);
            // if we register a callback, supply the provided readScope (which may be null)
            Participants<?> readScope = registerCallback ? readScopeIfCallback : null;
            Commit send = new Commit(kind, to, coordinates, all, txnId, txn, route, ballot, executeAt, deps, readScope);
            if (registerCallback) node.send(to, send, callback);
            else node.send(to, send);
        }
        if (all.size() > 1)
        {
            for (Node.Id to : all.nodes())
            {
                if (!primary.contains(to))
                {
                    boolean registerCallback = shouldRegisterCallback.test(param, to);
                    Commit send = new Commit(kind, to, coordinates, all, txnId, txn, route, ballot, executeAt, deps, (ReadTxnData) null);
                    if (registerCallback) node.send(to, send, callback);
                    else node.send(to, send);
                }
            }
        }
    }

    public static void stableMaximal(Node node, Node.Id to, Txn txn, TxnId txnId, Timestamp executeAt, FullRoute<?> route, Deps deps)
    {
        // the replica may be missing the original commit, or the additional commit, so send everything
        Topologies topologies = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
        Topology coordinates = topologies.forEpoch(txnId.epoch());
        node.send(to, new Commit(StableWithTxnAndDeps, to, coordinates, topologies, txnId, txn, route, Ballot.ZERO, executeAt, deps, (ReadTxnData) null));
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return keys;
    }

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.COMMANDS;
    }

    @Override
    public void process()
    {
        node.mapReduceConsumeLocal(this, txnId.epoch(), executeAt.epoch(), this);
    }

    // TODO (expected, efficiency, clarity): do not guard with synchronized; let mapReduceLocal decide how to enforce mutual exclusivity
    @Override
    public synchronized CommitOrReadNack apply(SafeCommandStore safeStore)
    {
        Route<?> route = this.route != null ? this.route : scope;
        SafeCommand safeCommand = safeStore.get(txnId, executeAt, route);

        switch (Commands.commit(safeStore, safeCommand, kind.saveStatus, ballot, txnId, route, progressKey, partialTxn, executeAt, partialDeps))
        {
            default:
            case Success:
            case Redundant:
                return null;
            case Insufficient:
                return CommitOrReadNack.Insufficient;
            case Rejected:
                return CommitOrReadNack.Rejected;
        }
    }

    @Override
    public CommitOrReadNack reduce(CommitOrReadNack r1, CommitOrReadNack r2)
    {
        return r1 != null ? r1 : r2;
    }

    @Override
    public synchronized void accept(CommitOrReadNack reply, Throwable failure)
    {
        if (reply != null || failure != null)
            node.reply(replyTo, replyContext, reply, failure);
        else if (readData != null)
            readData.process(node, replyTo, replyContext);
        else if (kind.saveStatus == Committed)
            node.reply(replyTo, replyContext, new ReadData.ReadOk(null, null), null);
    }

    @Override
    public MessageType type()
    {
        switch (kind)
        {
            case CommitSlowPath: return MessageType.COMMIT_SLOW_PATH_REQ;
            case CommitWithTxn: return MessageType.COMMIT_MAXIMAL_REQ;
            case StableFastPath: return MessageType.STABLE_FAST_PATH_REQ;
            case StableSlowPath: return MessageType.STABLE_SLOW_PATH_REQ;
            case StableWithTxnAndDeps: return MessageType.STABLE_MAXIMAL_REQ;
            default: throw new IllegalStateException();
        }
    }

    @Override
    public String toString()
    {
        return "Commit{kind:" + kind +
               ", txnId: " + txnId +
               ", executeAt: " + executeAt +
               ", deps: " + partialDeps +
               ", toExecute: " + readData +
               '}';
    }

    public static class Invalidate implements Request, PreLoadContext
    {
        public static class SerializerSupport
        {
            public static Invalidate create(TxnId txnId, Unseekables<?> scope, long waitForEpoch, long invalidateUntilEpoch)
            {
                return new Invalidate(txnId, scope, waitForEpoch, invalidateUntilEpoch);
            }
        }

        public static void commitInvalidate(Node node, TxnId txnId, Unseekables<?> inform, Timestamp until)
        {
            commitInvalidate(node, txnId, inform, until.epoch());
        }

        public static void commitInvalidate(Node node, TxnId txnId, Unseekables<?> inform, long untilEpoch)
        {
            // TODO (expected, safety): this kind of check needs to be inserted in all equivalent methods
            Invariants.checkState(untilEpoch >= txnId.epoch());
            Invariants.checkState(node.topology().hasEpoch(untilEpoch));
            Topologies commitTo = node.topology().preciseEpochs(inform, txnId.epoch(), untilEpoch);
            commitInvalidate(node, commitTo, txnId, inform);
        }

        public static void commitInvalidate(Node node, Topologies commitTo, TxnId txnId, Unseekables<?> inform)
        {
            for (Node.Id to : commitTo.nodes())
            {
                Invalidate send = new Invalidate(to, commitTo, txnId, inform);
                node.send(to, send);
            }
        }

        public final TxnId txnId;
        public final Unseekables<?> scope;
        public final long waitForEpoch;
        public final long invalidateUntilEpoch;

        Invalidate(Id to, Topologies topologies, TxnId txnId, Unseekables<?> scope)
        {
            this.txnId = txnId;
            int latestRelevantIndex = latestRelevantEpochIndex(to, topologies, scope);
            this.scope = computeScope(to, topologies, (Unseekables)scope, latestRelevantIndex, Unseekables::slice, Unseekables::with);
            this.waitForEpoch = computeWaitForEpoch(to, topologies, latestRelevantIndex);
            // TODO (expected): make sure we're picking the right upper limit - it can mean future owners that have never witnessed the command are invalidated
            this.invalidateUntilEpoch = topologies.currentEpoch();
        }

        Invalidate(TxnId txnId, Unseekables<?> scope, long waitForEpoch, long invalidateUntilEpoch)
        {
            this.txnId = txnId;
            this.scope = scope;
            this.waitForEpoch = waitForEpoch;
            this.invalidateUntilEpoch = invalidateUntilEpoch;
        }

        @Override
        public TxnId primaryTxnId()
        {
            return txnId;
        }

        @Override
        public long waitForEpoch()
        {
            return waitForEpoch;
        }

        @Override
        public void preProcess(Node on, Id from, ReplyContext replyContext)
        {
            // no-op
        }

        @Override
        public void process(Node node, Id from, ReplyContext replyContext)
        {

            node.forEachLocal(this, scope, txnId.epoch(), invalidateUntilEpoch, safeStore -> {
                // it's fine for this to operate on a non-participating home key, since invalidation is a terminal state,
                // so it doesn't matter if we resurrect a redundant entry
                Commands.commitInvalidate(safeStore, safeStore.get(txnId, txnId, scope), scope);
            }).begin(node.agent());
        }

        @Override
        public MessageType type()
        {
            return MessageType.COMMIT_INVALIDATE_REQ;
        }

        @Override
        public String toString()
        {
            return "CommitInvalidate{txnId: " + txnId + '}';
        }
    }
}
