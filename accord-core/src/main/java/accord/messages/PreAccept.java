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

import accord.api.Key;
import accord.local.*;
import accord.local.CommandsForKey.CommandTimeseries.TestKind;

import accord.local.Node.Id;
import accord.messages.TxnRequest.WithUnsynced;
import accord.topology.Topologies;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import javax.annotation.Nullable;

import accord.primitives.*;

import accord.primitives.Deps;
import accord.primitives.TxnId;

import static accord.local.CommandsForKey.CommandTimeseries.TestDep.ANY_DEPS;
import static accord.local.CommandsForKey.CommandTimeseries.TestKind.RorWs;
import static accord.local.CommandsForKey.CommandTimeseries.TestKind.Ws;
import static accord.local.CommandsForKey.CommandTimeseries.TestStatus.ANY_STATUS;

public class PreAccept extends WithUnsynced<PreAccept.PreAcceptReply>
{
    public static class SerializerSupport
    {
        public static PreAccept create(TxnId txnId, PartialRoute scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, long maxEpoch, PartialTxn partialTxn, @Nullable Route fullRoute)
        {
            return new PreAccept(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey, maxEpoch, partialTxn, fullRoute);
        }
    }

    public final PartialTxn partialTxn;
    public final @Nullable Route route; // ordinarily only set on home shard
    public final long maxEpoch;

    public PreAccept(Id to, Topologies topologies, TxnId txnId, Txn partialTxn, Route route)
    {
        super(to, topologies, txnId, route);
        this.partialTxn = partialTxn.slice(scope.covering, route.contains(route.homeKey));
        this.maxEpoch = topologies.currentEpoch();
        this.route = scope.contains(scope.homeKey) ? route : null;
    }

    private PreAccept(TxnId txnId, PartialRoute scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, long maxEpoch, PartialTxn partialTxn, @Nullable Route fullRoute)
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
    public Iterable<Key> keys()
    {
        return partialTxn.keys();
    }

    @Override
    protected void process()
    {
        node.mapReduceConsumeLocal(this, minEpoch, maxEpoch, this);
    }

    public PreAcceptReply apply(SafeCommandStore safeStore)
    {
        // note: this diverges from the paper, in that instead of waiting for JoinShard,
        //       we PreAccept to both old and new topologies and require quorums in both.
        //       This necessitates sending to ALL replicas of old topology, not only electorate (as fast path may be unreachable).
        Command command = safeStore.command(txnId);
        switch (command.preaccept(safeStore, partialTxn, route != null ? route : scope, progressKey))
        {
            default:
            case Success:
            case Redundant:
                return new PreAcceptOk(txnId, command.executeAt(), calculatePartialDeps(safeStore, txnId, partialTxn.keys(), partialTxn.kind(), txnId, safeStore.ranges().between(minEpoch, txnId.epoch)));

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
        // TODO: communicate back the failure
        node.reply(replyTo, replyContext, reply);
    }

    @Override
    public MessageType type()
    {
        return MessageType.PREACCEPT_REQ;
    }

    public interface PreAcceptReply extends Reply
    {
        @Override
        default MessageType type()
        {
            return MessageType.PREACCEPT_RSP;
        }

        boolean isOk();
    }

    public static class PreAcceptOk implements PreAcceptReply
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

    public static class PreAcceptNack implements PreAcceptReply
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

    static PartialDeps calculatePartialDeps(SafeCommandStore commandStore, TxnId txnId, Keys keys, Txn.Kind kindOfTxn, Timestamp executeAt, KeyRanges ranges)
    {
        try (PartialDeps.OrderedBuilder builder = PartialDeps.orderedBuilder(ranges, false))
        {
            return calculateDeps(commandStore, txnId, keys, kindOfTxn, executeAt, ranges, builder);
        }
    }

    private static <T extends Deps> T calculateDeps(SafeCommandStore commandStore, TxnId txnId, Keys keys, Txn.Kind kindOfTxn, Timestamp executeAt, KeyRanges ranges, Deps.AbstractOrderedBuilder<T> builder)
    {
        keys.foldl(ranges, (i, key, ignore) -> {
            CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
            if (forKey == null)
                return null;

            builder.nextKey(key);
            TestKind testKind = kindOfTxn.isWrite() ? RorWs : Ws;
            forKey.uncommitted().before(executeAt, testKind, ANY_DEPS, null, ANY_STATUS, null)
                    .forEach(info -> {
                        if (!info.txnId.equals(txnId)) builder.add(info.txnId);
                    });
            forKey.committedByExecuteAt().before(executeAt, testKind, ANY_DEPS, null, ANY_STATUS, null)
                    .forEach(id -> {
                        if (!id.equals(txnId)) builder.add(id);
                    });
            return null;
        }, null);

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
