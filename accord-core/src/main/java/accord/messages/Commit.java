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

import java.util.Set;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Commands;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.messages.ReadData.ReadNack;
import accord.messages.ReadData.ReadReply;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
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

import static accord.utils.Invariants.checkArgument;

public class Commit extends TxnRequest<ReadNack>
{
    private static final Logger logger = LoggerFactory.getLogger(Commit.class);

    public static class SerializerSupport
    {
        public static Commit create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Kind kind, Timestamp executeAt, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute, @Nullable ReadData readData)
        {
            return new Commit(kind, txnId, scope, waitForEpoch, executeAt, partialTxn, partialDeps, fullRoute, readData);
        }
    }

    public final Kind kind;
    public final Timestamp executeAt;
    public final @Nullable PartialTxn partialTxn;
    public final PartialDeps partialDeps;
    public final @Nullable FullRoute<?> route;
    public final ReadData readData;

    public enum Kind { Minimal, Maximal }

    // TODO (low priority, clarity): cleanup passing of topologies here - maybe fetch them afresh from Node?
    //                               Or perhaps introduce well-named classes to represent different topology combinations

    public Commit(Kind kind, Id to, Topology coordinateTopology, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps, ReadTxnData read)
    {
        this(kind, to, coordinateTopology, topologies, txnId, txn, route, executeAt, deps, (u1, u2, u3) -> read);
    }

    public Commit(Kind kind, Id to, Topology coordinateTopology, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, @Nullable Participants<?> readScope, Timestamp executeAt, Deps deps, boolean read)
    {
        this(kind, to, coordinateTopology, topologies, txnId, txn, route, executeAt, deps, read ? new ReadTxnData(to, topologies, txnId, readScope, executeAt) : null);
    }

    public Commit(Kind kind, Id to, Topology coordinateTopology, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps, TriFunction<Txn, PartialRoute<?>, PartialDeps, ReadData> toExecuteFactory)
    {
        super(to, topologies, route, txnId);

        FullRoute<?> sendRoute = null;
        PartialTxn partialTxn = null;
        if (kind == Kind.Maximal)
        {
//            boolean isHome = coordinateTopology.rangesForNode(to).contains(route.homeKey());
            // TODO (expected): only includeQuery if isHome; this affects state eviction and is low priority given size in C*
            partialTxn = txn.slice(scope.covering(), true);
            sendRoute = route;
        }
        else if (executeAt.epoch() != txnId.epoch())
        {
            Ranges coordinateRanges = coordinateTopology.rangesForNode(to);
            Ranges executeRanges = topologies.computeRangesForNode(to);
            Ranges extraRanges = executeRanges.subtract(coordinateRanges);
            if (!extraRanges.isEmpty())
                partialTxn = txn.slice(extraRanges, coordinateRanges.contains(route.homeKey()));
        }

        this.kind = kind;
        this.executeAt = executeAt;
        this.partialTxn = partialTxn;
        this.partialDeps = deps.slice(scope.covering());
        this.route = sendRoute;
        this.readData = toExecuteFactory.apply(partialTxn != null ? partialTxn : txn, scope, partialDeps);
    }

    Commit(Kind kind, TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Timestamp executeAt, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute, @Nullable ReadData readData)
    {
        super(txnId, scope, waitForEpoch);
        this.kind = kind;
        this.executeAt = executeAt;
        this.partialTxn = partialTxn;
        this.partialDeps = partialDeps;
        this.route = fullRoute;
        this.readData = readData;
    }

    // TODO (low priority, clarity): accept Topology not Topologies
    // TODO (desired, efficiency): do not commit if we're already ready to execute (requires extra info in Accept responses)
    public static void commitMinimalAndRead(Node node, Topologies executeTopologies, TxnId txnId, Txn txn, FullRoute<?> route, Participants<?> readScope, Timestamp executeAt, Deps deps, Set<Id> readSet, Callback<ReadReply> callback)
    {
        Topologies allTopologies = executeTopologies;
        if (txnId.epoch() != executeAt.epoch())
            allTopologies = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());

        Topology executeTopology = executeTopologies.forEpoch(executeAt.epoch());
        Topology coordinateTopology = allTopologies.forEpoch(txnId.epoch());
        for (Node.Id to : executeTopology.nodes())
        {
            boolean read = readSet.contains(to);
            Commit send = new Commit(Kind.Minimal, to, coordinateTopology, allTopologies, txnId, txn, route, readScope, executeAt, deps, read);
            if (read) node.send(to, send, callback);
            else node.send(to, send);
        }
        if (coordinateTopology != executeTopology)
        {
            for (Node.Id to : allTopologies.nodes())
            {
                if (!executeTopology.contains(to))
                    node.send(to, new Commit(Kind.Minimal, to, coordinateTopology, allTopologies, txnId, txn, route, readScope, executeAt, deps, false));
            }
        }
    }

    public static void commitMaximalAndBlockOnDeps(Node node, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, Deps deps, Callback<ReadReply> callback)
    {
        checkArgument(topologies.size() == 1);
        Topology topology = topologies.get(0);
        for (Node.Id to : topology.nodes())
        {
            // To simplify making sure the agent is notified once and is notified before the barrier coordination
            // returns a result; we never notify the agent on the coordinator as part of WaitForDependenciesThenApply execution
            // Also always send a maximal commit since we don't block on deps often and that saves having to have an Insufficient code path
            // going back for the Apply in `ApplyThenWaitUntilApplied
            boolean notifyAgent = !to.equals(node.id());
            Commit commit = new Commit(
                    Kind.Maximal, to, topology, topologies, txnId,
                    txn, route, txnId, deps,
                    (maybePartialTransaction, partialRoute, partialDeps) -> new ApplyThenWaitUntilApplied(txnId, partialRoute, partialDeps, maybePartialTransaction.keys().slice(partialDeps.covering), txn.execute(txnId, txnId, null), txn.result(txnId, txnId, null), notifyAgent));
            node.send(to, commit, callback);
        }
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return partialTxn != null ? partialTxn.keys() : Keys.EMPTY;
    }

    @Override
    public void process()
    {
        node.mapReduceConsumeLocal(this, txnId.epoch(), executeAt.epoch(), this);
    }

    // TODO (expected, efficiency, clarity): do not guard with synchronized; let mapReduceLocal decide how to enforce mutual exclusivity
    @Override
    public synchronized ReadNack apply(SafeCommandStore safeStore)
    {
        Route<?> route = this.route != null ? this.route : scope;
        SafeCommand safeCommand = safeStore.get(txnId, executeAt, route);

        switch (Commands.commit(safeStore, safeCommand, txnId, route, progressKey, partialTxn, executeAt, partialDeps))
        {
            default:
            case Success:
            case Redundant:
                return null;
            case Insufficient:
                return ReadNack.NotCommitted;
        }
    }

    @Override
    public ReadNack reduce(ReadNack r1, ReadNack r2)
    {
        return r1 != null ? r1 : r2;
    }

    @Override
    public synchronized void accept(ReadNack reply, Throwable failure)
    {
        if (reply != null || failure != null)
            node.reply(replyTo, replyContext, reply, failure);
        else if (readData != null)
            readData.process(node, replyTo, replyContext);
    }

    @Override
    public MessageType type()
    {
        switch (kind)
        {
            case Minimal: return MessageType.COMMIT_MINIMAL_REQ;
            case Maximal: return MessageType.COMMIT_MAXIMAL_REQ;
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
