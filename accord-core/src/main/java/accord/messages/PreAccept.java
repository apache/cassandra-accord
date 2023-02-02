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
import java.util.Objects;

import accord.local.*;
import accord.local.SafeCommandStore.TestKind;

import accord.local.Node.Id;
import accord.messages.TxnRequest.WithUnsynced;
import accord.topology.Topologies;
import accord.primitives.Timestamp;
import javax.annotation.Nullable;

import accord.primitives.*;

import accord.primitives.TxnId;

import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestKind.RorWs;
import static accord.local.SafeCommandStore.TestKind.Ws;
import static accord.local.SafeCommandStore.TestTimestamp.STARTED_BEFORE;

public class PreAccept extends WithUnsynced<PreAccept.PreAcceptReply>
{
    public static class SerializerSupport
    {
        public static PreAccept create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, long maxEpoch, PartialTxn partialTxn, @Nullable FullRoute<?> fullRoute)
        {
            return new PreAccept(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey, maxEpoch, partialTxn, fullRoute);
        }
    }

    public final PartialTxn partialTxn;
    public final @Nullable FullRoute<?> route; // ordinarily only set on home shard
    public final long maxEpoch;

    public PreAccept(Id to, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        super(to, topologies, txnId, route);
        this.partialTxn = txn.slice(scope.covering(), route.contains(route.homeKey()));
        this.maxEpoch = topologies.currentEpoch();
        this.route = scope.contains(scope.homeKey()) ? route : null;
    }

    private PreAccept(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, long maxEpoch, PartialTxn partialTxn, @Nullable FullRoute<?> fullRoute)
    {
        super(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey);
        this.partialTxn = partialTxn;
        this.maxEpoch = maxEpoch;
        this.route = fullRoute;
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return partialTxn.keys();
    }

    @Override
    protected void process()
    {
        node.mapReduceConsumeLocal(this, minUnsyncedEpoch, maxEpoch, this);
    }

    @Override
    public PreAcceptReply apply(SafeCommandStore safeStore)
    {
        // note: this diverges from the paper, in that instead of waiting for JoinShard,
        //       we PreAccept to both old and new topologies and require quorums in both.
        //       This necessitates sending to ALL replicas of old topology, not only electorate (as fast path may be unreachable).
        if (minUnsyncedEpoch < txnId.epoch() && !safeStore.ranges().at(txnId.epoch()).intersects(scope))
        {
            // we only preaccept in the coordination epoch, but we might contact other epochs for dependencies
            return new PreAcceptOk(txnId, txnId,
                    calculatePartialDeps(safeStore, txnId, partialTxn.keys(), txnId, safeStore.ranges().between(minUnsyncedEpoch, txnId.epoch())));
        }

        switch (Commands.preaccept(safeStore, txnId, partialTxn, route != null ? route : scope, progressKey))
        {
            default:
            case Success:
            case Redundant:
                Command command = safeStore.command(txnId).current();
                return new PreAcceptOk(txnId, command.executeAt(),
                        calculatePartialDeps(safeStore, txnId, partialTxn.keys(), txnId, safeStore.ranges().between(minUnsyncedEpoch, txnId.epoch())));

            case RejectedBallot:
                return PreAcceptNack.INSTANCE;
        }
    }

    @Override
    public PreAcceptReply reduce(PreAcceptReply r1, PreAcceptReply r2)
    {
        if (!r1.isOk()) return r1;
        if (!r2.isOk()) return r2;
        PreAcceptOk ok1 = (PreAcceptOk) r1;
        PreAcceptOk ok2 = (PreAcceptOk) r2;
        PreAcceptOk okMax = ok1.witnessedAt.compareTo(ok2.witnessedAt) >= 0 ? ok1 : ok2;
        PartialDeps deps = ok1.deps.with(ok2.deps);
        if (deps == okMax.deps)
            return okMax;
        return new PreAcceptOk(txnId, okMax.witnessedAt, deps);
    }

    @Override
    public void accept(PreAcceptReply reply, Throwable failure)
    {
        // TODO (required, error handling): communicate back the failure
        node.reply(replyTo, replyContext, reply);
    }

    @Override
    public MessageType type()
    {
        return MessageType.PREACCEPT_REQ;
    }

    public static abstract class PreAcceptReply implements Reply
    {
        @Override
        public MessageType type()
        {
            return MessageType.PREACCEPT_RSP;
        }

        public abstract boolean isOk();
    }

    public static class PreAcceptOk extends PreAcceptReply
    {
        public final TxnId txnId;
        public final Timestamp witnessedAt;
        public final PartialDeps deps;

        public PreAcceptOk(TxnId txnId, Timestamp witnessedAt, PartialDeps deps)
        {
            this.txnId = txnId;
            this.witnessedAt = witnessedAt;
            this.deps = deps;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PreAcceptOk that = (PreAcceptOk) o;
            return witnessedAt.equals(that.witnessedAt) && deps.equals(that.deps);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(witnessedAt, deps);
        }

        @Override
        public String toString()
        {
            return "PreAcceptOk{" +
                    "txnId:" + txnId +
                    ", witnessedAt:" + witnessedAt +
                    ", deps:" + deps +
                    '}';
        }
    }

    public static class PreAcceptNack extends PreAcceptReply
    {
        public static final PreAcceptNack INSTANCE = new PreAcceptNack();

        private PreAcceptNack() {}

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "PreAcceptNack{}";
        }
    }

    static PartialDeps calculatePartialDeps(SafeCommandStore commandStore, TxnId txnId, Seekables<?, ?> keys, Timestamp executeAt, Ranges ranges)
    {
        try (PartialDeps.Builder builder = PartialDeps.builder(ranges))
        {
            return calculateDeps(commandStore, txnId, keys, executeAt, ranges, builder);
        }
    }

    private static <T extends Deps> T calculateDeps(SafeCommandStore commandStore, TxnId txnId, Seekables<?, ?> keys, Timestamp executeAt, Ranges ranges, Deps.AbstractBuilder<T> builder)
    {
        TestKind testKind = txnId.rw().isWrite() ? RorWs : Ws;
        // could use MAY_EXECUTE_BEFORE to prune those we know execute later, but shouldn't usually be of much help
        // and would need to supply !hasOrderedTxnId
        commandStore.mapReduce(keys, ranges, testKind, STARTED_BEFORE, executeAt, ANY_DEPS, null, null, null,
                (keyOrRange, testTxnId, testExecuteAt, in) -> {
                    // TODO (easy, efficiency): either pass txnId as parameter or encode this behaviour in a specialised builder to avoid extra allocations
                    if (!testTxnId.equals(txnId))
                        in.add(keyOrRange, testTxnId);
                    return in;
                }, builder, null);
        return builder.build();
    }

    @Override
    public String toString()
    {
        return "PreAccept{" +
               "txnId:" + txnId +
               ", txn:" + partialTxn +
               ", scope:" + scope +
               '}';
    }
}
