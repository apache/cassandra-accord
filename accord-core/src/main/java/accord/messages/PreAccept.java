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

import java.util.List;
import java.util.Objects;

import accord.local.*;
import accord.local.SafeCommandStore.TestKind;

import accord.local.Node.Id;
import accord.messages.TxnRequest.WithUnsynced;
import accord.topology.Shard;
import accord.topology.Topologies;

import javax.annotation.Nullable;

import accord.primitives.*;

import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestTimestamp.STARTED_BEFORE;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

public class PreAccept extends WithUnsynced<PreAccept.PreAcceptReply> implements EpochSupplier
{
    public static class SerializerSupport
    {
        public static PreAccept create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, long maxEpoch, PartialTxn partialTxn, @Nullable FullRoute<?> fullRoute)
        {
            return new PreAccept(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey, maxEpoch, partialTxn, fullRoute);
        }
    }

    public final PartialTxn partialTxn;
    public final FullRoute<?> route;
    public final long maxEpoch;

    public PreAccept(Id to, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        super(to, topologies, txnId, route);
        // TODO (expected): only includeQuery if route.contains(route.homeKey()); this affects state eviction and is low priority given size in C*
        this.partialTxn = txn.slice(scope.covering(), true);
        this.maxEpoch = topologies.currentEpoch();
        this.route = route;
    }

    PreAccept(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, long maxEpoch, PartialTxn partialTxn, @Nullable FullRoute<?> fullRoute)
    {
        super(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey);
        this.partialTxn = partialTxn;
        this.maxEpoch = maxEpoch;
        this.route = fullRoute;
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
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

    protected PreAcceptReply applyIfDoesNotCoordinate(SafeCommandStore safeStore)
    {
        // we only preaccept in the coordination epoch, but we might contact other epochs for dependencies
        Ranges ranges = safeStore.ranges().allBetween(minUnsyncedEpoch, txnId);
        if (txnId.rw() == ExclusiveSyncPoint)
            safeStore.commandStore().markExclusiveSyncPoint(safeStore, txnId, ranges);
        return new PreAcceptOk(txnId, txnId, calculatePartialDeps(safeStore, txnId, partialTxn.keys(), txnId, ranges));
    }

    @Override
    public PreAcceptReply apply(SafeCommandStore safeStore)
    {
        // TODO (desired): restore consistency with paper, either by changing code or paper
        // note: this diverges from the paper, in that instead of waiting for JoinShard,
        //       we PreAccept to both old and new topologies and require quorums in both.
        //       This necessitates sending to ALL replicas of old topology, not only electorate (as fast path may be unreachable).
        if (minUnsyncedEpoch < txnId.epoch() && !safeStore.ranges().coordinates(txnId).intersects(scope))
            return applyIfDoesNotCoordinate(safeStore);

        SafeCommand safeCommand = safeStore.get(txnId, this, route);
        switch (Commands.preaccept(safeStore, safeCommand, txnId, maxEpoch, partialTxn, route, progressKey))
        {
            default:
            case Success:
            case Redundant: // we might hit 'Redundant' if we have to contact later epochs and partially re-contact a node we already contacted
                Command command = safeCommand.current();
                // for efficiency, we don't usually return dependencies newer than txnId as they aren't necessarily needed
                // for recovery, and it's better to persist less data than more. However, for exclusive sync points we
                // don't need to perform an Accept round, nor do we need to persist this state to aid recovery. We just
                // want the issuer of the sync point to know which transactions to wait for before it can safely treat
                // all transactions with lower txnId as expired.
                return new PreAcceptOk(txnId, command.executeAt(),
                        calculatePartialDeps(safeStore, txnId, partialTxn.keys(), txnId, safeStore.ranges().allBetween(minUnsyncedEpoch, txnId)));

            case Truncated:
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
        PreAcceptOk okMin = okMax == ok1 ? ok2 : ok1;

        Timestamp witnessedAt = Timestamp.mergeMax(okMax.witnessedAt, okMin.witnessedAt);
        PartialDeps deps = ok1.deps.with(ok2.deps);

        if (deps == okMax.deps && witnessedAt == okMax.witnessedAt)
            return okMax;
        return new PreAcceptOk(txnId, witnessedAt, deps);
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
        return MessageType.PRE_ACCEPT_REQ;
    }

    public static abstract class PreAcceptReply implements Reply
    {
        @Override
        public MessageType type()
        {
            return MessageType.PRE_ACCEPT_RSP;
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
        TestKind testKind = TestKind.conflicts(txnId.rw());
        // could use MAY_EXECUTE_BEFORE to prune those we know execute later.
        // NOTE: ExclusiveSyncPoint *relies* on STARTED_BEFORE to ensure it reports a dependency on *every* earlier TxnId that may execute after it.
        //       This is necessary for reporting to a bootstrapping replica which TxnId it must not prune from dependencies
        //       i.e. the source replica reports to the target replica those TxnId that STARTED_BEFORE and EXECUTES_AFTER.
        commandStore.mapReduce(keys, ranges, testKind, STARTED_BEFORE, executeAt, ANY_DEPS, null, null, null,
                (keyOrRange, testTxnId, testExecuteAt, in) -> {
                    // TODO (easy, efficiency): either pass txnId as parameter or encode this behaviour in a specialised builder to avoid extra allocations
                    if (!testTxnId.equals(txnId))
                        in.add(keyOrRange, testTxnId);
                    return in;
                }, builder, null);
        return builder.build();
    }

    /**
     * To simplify the implementation of bootstrap/range movements, we have coordinators abort transactions
     * that span too many topology changes for any given shard. This means that we can always daisy-chain a replica
     * that can inform a new/joining/bootstrapping replica of the data table state and relevant transaction
     * history without peeking into the future.
     *
     * This is necessary because when we create an ExclusiveSyncPoint there may be some transactions that
     * are captured by it as necessary to witness the result of, but that will execute after it at some arbitrary
     * future point. For simplicity, we wait for these transactions to execute on the source replicas
     * before streaming the table state to the target replicas. But if these execute in a future topology,
     * there may not be a replica that is able to wait for and execute the transaction.
     * So, we simply prohibit them from doing so.
     *
     * TODO (desired): it would be nice if this were enforced by some register on replicas that inform coordinators
     * of the maximum permitted executeAt. But this would make ExclusiveSyncPoint more complicated to coordinate.
     */
    public static boolean rejectExecuteAt(TxnId txnId, Topologies topologies)
    {
        // for each superseding shard, mark any nodes removed in a long bitmap; once the number of removals
        // is greater than the minimum maxFailures for any shard, we reject the executeAt.
        // Note, this over-estimates the number of removals by counting removals from _any_ superseding shard
        // (rather than counting each superseding shard separately)
        int originalIndex = topologies.indexForEpoch(txnId.epoch());
        if (originalIndex == 0)
            return false;

        List<Shard> originalShards = topologies.get(originalIndex).shards();
        if (originalShards.stream().anyMatch(s -> s.sortedNodes.size() > 64))
            return true;

        long[] removals = new long[originalShards.size()];
        int minMaxFailures = originalShards.stream().mapToInt(s -> s.maxFailures).min().getAsInt();
        for (int i = originalIndex - 1 ; i >= 0 ; --i)
        {
            List<Shard> newShards = topologies.get(i).shards();
            minMaxFailures = Math.min(minMaxFailures, newShards.stream().mapToInt(s -> s.maxFailures).min().getAsInt());
            int n = 0, o = 0;
            while (n < newShards.size() && o < originalShards.size())
            {
                Shard nv = newShards.get(n);
                Shard ov = originalShards.get(o);
                {
                    int c = nv.range.compareIntersecting(ov.range);
                    if (c < 0) { ++n; continue; }
                    else if (c > 0) { ++o; continue; }
                }
                int nvi = 0, ovi = 0;
                while (nvi < nv.sortedNodes.size() && ovi < ov.sortedNodes.size())
                {
                    int c = nv.sortedNodes.get(nvi).compareTo(ov.sortedNodes.get(ovi));
                    if (c < 0) ++nvi;
                    else if (c == 0) { ++nvi; ++ovi; }
                    // TODO (required): consider if this needs to be >=
                    //    consider case where one (or more) of the original nodes is bootstrapping from other original nodes
                    else if (Long.bitCount(removals[o] |= 1L << ovi++) > minMaxFailures)
                        return true;
                }
                while (ovi < ov.sortedNodes.size())
                {
                    if (Long.bitCount(removals[o] |= 1L << ovi++) > minMaxFailures)
                        return true;
                }
                int c = nv.range.end().compareTo(ov.range.end());
                if (c <= 0) ++n;
                if (c >= 0) ++o;
            }
        }
        return false;
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

    @Override
    public long epoch()
    {
        return maxEpoch;
    }
}
