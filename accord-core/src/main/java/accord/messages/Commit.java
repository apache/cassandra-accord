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

import java.util.Collections;
import java.util.Set;

import accord.local.*;
import accord.local.PreLoadContext;
import accord.messages.ReadData.ReadNack;
import accord.messages.ReadData.ReadReply;
import accord.primitives.*;
import accord.local.Node.Id;
import accord.topology.Topologies;
import javax.annotation.Nullable;

import accord.utils.Invariants;

import accord.topology.Topology;

import static accord.local.Status.Committed;
import static accord.local.Status.Known.DefinitionOnly;

public class Commit extends TxnRequest<ReadNack>
{
    public static class SerializerSupport
    {
        public static Commit create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Timestamp executeAt, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute, @Nullable ReadData read)
        {
            return new Commit(txnId, scope, waitForEpoch, executeAt, partialTxn, partialDeps, fullRoute, read);
        }
    }

    public final Timestamp executeAt;
    public final @Nullable PartialTxn partialTxn;
    public final PartialDeps partialDeps;
    public final @Nullable FullRoute<?> route;
    public final ReadData read;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private transient Defer defer;

    public enum Kind { Minimal, Maximal }

    // TODO (low priority, clarity): cleanup passing of topologies here - maybe fetch them afresh from Node?
    //                               Or perhaps introduce well-named classes to represent different topology combinations
    public Commit(Kind kind, Id to, Topology coordinateTopology, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, @Nullable Seekables<?, ?> readScope, Timestamp executeAt, Deps deps, boolean read)
    {
        super(to, topologies, route, txnId);

        FullRoute<?> sendRoute = null;
        PartialTxn partialTxn = null;
        if (kind == Kind.Maximal)
        {
            boolean isHome = coordinateTopology.rangesForNode(to).contains(route.homeKey());
            partialTxn = txn.slice(scope.covering(), isHome);
            if (isHome)
                sendRoute = route;
        }
        else if (executeAt.epoch() != txnId.epoch())
        {
            Ranges coordinateRanges = coordinateTopology.rangesForNode(to);
            Ranges executeRanges = topologies.computeRangesForNode(to);
            Ranges extraRanges = executeRanges.difference(coordinateRanges);
            if (!extraRanges.isEmpty())
                partialTxn = txn.slice(extraRanges, coordinateRanges.contains(route.homeKey()));
        }

        this.executeAt = executeAt;
        this.partialTxn = partialTxn;
        this.partialDeps = deps.slice(scope.covering());
        this.route = sendRoute;
        this.read = read ? new ReadData(to, topologies, txnId, readScope, executeAt) : null;
    }

    Commit(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Timestamp executeAt, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute, @Nullable ReadData read)
    {
        super(txnId, scope, waitForEpoch);
        this.executeAt = executeAt;
        this.partialTxn = partialTxn;
        this.partialDeps = partialDeps;
        this.route = fullRoute;
        this.read = read;
    }

    // TODO (low priority, clarity): accept Topology not Topologies
    // TODO (desired, efficiency): do not commit if we're already ready to execute (requires extra info in Accept responses)
    public static void commitMinimalAndRead(Node node, Topologies executeTopologies, TxnId txnId, Txn txn, FullRoute<?> route, Seekables<?, ?> readScope, Timestamp executeAt, Deps deps, Set<Id> readSet, Callback<ReadReply> callback)
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

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return Keys.EMPTY;
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
        Command command = safeStore.command(txnId);
        switch (command.commit(safeStore, route != null ? route : scope, progressKey, partialTxn, executeAt, partialDeps))
        {
            default:
            case Success:
            case Redundant:
                return null;

            case Insufficient:
                Invariants.checkState(!command.hasBeenWitnessed());
                if (defer == null)
                    defer = new Defer(DefinitionOnly, Committed.minKnown, Commit.this);
                defer.add(command, safeStore.commandStore());
                return ReadNack.NotCommitted;
        }
    }

    @Override
    public ReadNack reduce(ReadNack r1, ReadNack r2)
    {
        return r1 != null ? r1 : r2;
    }

    @Override
    public void accept(ReadNack reply, Throwable failure)
    {
        if (reply != null)
            node.reply(replyTo, replyContext, reply);
        else if (read != null)
            read.process(node, replyTo, replyContext);
    }

    @Override
    public MessageType type()
    {
        return MessageType.COMMIT_REQ;
    }

    @Override
    public String toString()
    {
        return "Commit{txnId: " + txnId +
               ", executeAt: " + executeAt +
               ", deps: " + partialDeps +
               ", read: " + read +
               '}';
    }

    public static class Invalidate implements Request, PreLoadContext
    {
        public static class SerializerSupport
        {
            public static Invalidate create(TxnId txnId, Unseekables<?, ?> scope, long waitForEpoch, long invalidateUntilEpoch)
            {
                return new Invalidate(txnId, scope, waitForEpoch, invalidateUntilEpoch);
            }
        }

        public static void commitInvalidate(Node node, TxnId txnId, Unseekables<?, ?> inform, Timestamp until)
        {
            commitInvalidate(node, txnId, inform, until.epoch());
        }

        public static void commitInvalidate(Node node, TxnId txnId, Unseekables<?, ?> inform, long untilEpoch)
        {
            // TODO (expected, safety): this kind of check needs to be inserted in all equivalent methods
            Invariants.checkState(untilEpoch >= txnId.epoch());
            Invariants.checkState(node.topology().hasEpoch(untilEpoch));
            Topologies commitTo = node.topology().preciseEpochs(inform, txnId.epoch(), untilEpoch);
            commitInvalidate(node, commitTo, txnId, inform);
        }

        public static void commitInvalidate(Node node, Topologies commitTo, TxnId txnId, Unseekables<?, ?> inform)
        {
            for (Node.Id to : commitTo.nodes())
            {
                Invalidate send = new Invalidate(to, commitTo, txnId, inform);
                node.send(to, send);
            }
        }

        public final TxnId txnId;
        public final Unseekables<?, ?> scope;
        public final long waitForEpoch;
        public final long invalidateUntilEpoch;

        Invalidate(Id to, Topologies topologies, TxnId txnId, Unseekables<?, ?> scope)
        {
            this.txnId = txnId;
            int latestRelevantIndex = latestRelevantEpochIndex(to, topologies, scope);
            this.scope = computeScope(to, topologies, (Unseekables)scope, latestRelevantIndex, Unseekables::slice, Unseekables::with);
            this.waitForEpoch = computeWaitForEpoch(to, topologies, latestRelevantIndex);
            this.invalidateUntilEpoch = topologies.currentEpoch();
        }

        Invalidate(TxnId txnId, Unseekables<?, ?> scope, long waitForEpoch, long invalidateUntilEpoch)
        {
            this.txnId = txnId;
            this.scope = scope;
            this.waitForEpoch = waitForEpoch;
            this.invalidateUntilEpoch = invalidateUntilEpoch;
        }

        @Override
        public Iterable<TxnId> txnIds()
        {
            return Collections.singleton(txnId);
        }

        @Override
        public Seekables<?, ?> keys()
        {
            return Keys.EMPTY;
        }

        @Override
        public long waitForEpoch()
        {
            return waitForEpoch;
        }

        @Override
        public void process(Node node, Id from, ReplyContext replyContext)
        {
            node.forEachLocal(this, scope, txnId.epoch(), invalidateUntilEpoch,
                            safeStore -> safeStore.command(txnId).commitInvalidate(safeStore))
                    .begin(node.agent());
        }

        @Override
        public MessageType type()
        {
            return MessageType.COMMIT_INVALIDATE;
        }

        @Override
        public String toString()
        {
            return "CommitInvalidate{txnId: " + txnId + '}';
        }
    }
}
