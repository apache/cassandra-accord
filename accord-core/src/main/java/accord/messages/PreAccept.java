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
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Command;
import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.messages.TxnRequest.WithUnsynced;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.Invariants;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

public class PreAccept extends WithUnsynced<PreAccept.PreAcceptReply> implements EpochSupplier
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(PreAccept.class);

    public static class SerializerSupport
    {
        public static PreAccept create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, long maxEpoch, PartialTxn partialTxn, @Nullable FullRoute<?> fullRoute)
        {
            return new PreAccept(txnId, scope, waitForEpoch, minEpoch, maxEpoch, partialTxn, fullRoute);
        }
    }

    public final PartialTxn partialTxn;
    public final FullRoute<?> route;
    public final long maxEpoch;

    public PreAccept(Id to, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        super(to, topologies, txnId, route);
        // TODO (expected): only includeQuery if route.contains(route.homeKey()); this affects state eviction and is low priority given size in C*
        this.partialTxn = txn.intersecting(scope, true);
        this.maxEpoch = topologies.currentEpoch();
        this.route = route;
    }

    PreAccept(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, long maxEpoch, PartialTxn partialTxn, @Nullable FullRoute<?> fullRoute)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
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
    public KeyHistory keyHistory()
    {
        return KeyHistory.COMMANDS;
    }

    @Override
    protected void process()
    {
        node.mapReduceConsumeLocal(this, minEpoch, maxEpoch, this);
    }

    protected PreAcceptReply applyIfDoesNotCoordinate(SafeCommandStore safeStore)
    {
        // we only preaccept in the coordination epoch, but we might contact other epochs for dependencies
        // TODO (required): for recovery we need to update CommandsForKeys et al, even if we don't participate in coordination.
        //      must consider cleanup though. (alternative is to make recovery more complicated)
        Ranges ranges = safeStore.ranges().allBetween(minEpoch, txnId);
        if (txnId.kind() == ExclusiveSyncPoint)
            // TODO (required): this must be removed for correctly restoring rejectBefore (so journal can update it). This is expected to happen in a follow-up patch soon after.
            safeStore.commandStore().markExclusiveSyncPoint(safeStore, txnId, ranges);
        return new PreAcceptOk(txnId, txnId, calculatePartialDeps(safeStore, txnId, partialTxn.keys(), scope, EpochSupplier.constant(minEpoch), txnId, ranges));
    }

    @Override
    public PreAcceptReply apply(SafeCommandStore safeStore)
    {
        // TODO (desired): restore consistency with paper, either by changing code or paper
        // note: this diverges from the paper, in that instead of waiting for JoinShard,
        //       we PreAccept to both old and new topologies and require quorums in both.
        //       This necessitates sending to ALL replicas of old topology, not only electorate (as fast path may be unreachable).
        if (minEpoch < txnId.epoch() && !safeStore.ranges().coordinates(txnId).intersects(scope))
            return applyIfDoesNotCoordinate(safeStore);

        SafeCommand safeCommand = safeStore.get(txnId, this, route);
        Commands.AcceptOutcome outcome = Commands.preaccept(safeStore, safeCommand, txnId, maxEpoch, partialTxn, route);
        Command command = safeCommand.current();
        switch (outcome)
        {
            default:
                // we might hit 'Redundant' if we have to contact later epochs and partially re-contact a node we already contacted
                // TODO (desired): consider dedicated special case, or rename
            case Redundant:
            case Success:
                // Either messages are being delivered out of order or the coordinator was interrupted by some
                // recovery coordinator
                if (command.saveStatus().compareTo(SaveStatus.PreAccepted) > 0)
                    return PreAcceptNack.INSTANCE;

                Ranges ranges = safeStore.ranges().allBetween(minEpoch, txnId);
                Timestamp executeAt = command.executeAt();
                PartialDeps deps = calculatePartialDeps(safeStore, txnId, command.partialTxn().keys(), scope, EpochSupplier.constant(minEpoch), txnId, ranges);
                // we can have future transaction dependencies if we are responding with an earlier witnessed executeAt, and in this case
                // we may still execute on the fast path, so we need to include the future dependency (which is included by CFK only if it has pruned earlier transactions)
                // for execution on any replica these dependencies may be committed to.
                // TODO (desired): if we proceed to propose phase we could choose to filter these future dependencies, as any necessary will be re-included by the Accept responses
                Invariants.checkState(txnId.kind().isSyncPoint() || deps.maxTxnId(txnId).compareTo(executeAt) <= 0,
                                      "Calculated dependencies for %s containing a txnId %s greater than the decided executeAt %s: %s", txnId, deps.maxTxnId(), executeAt, deps);
                return new PreAcceptOk(txnId, command.executeAt(), deps);

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
        node.reply(replyTo, replyContext, reply, failure);
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

    static PartialDeps calculatePartialDeps(SafeCommandStore safeStore, TxnId txnId, Seekables<?, ?> keys, Route<?> route, EpochSupplier minEpoch, Timestamp executeAt, Ranges ranges)
    {
        // TODO (expected): do not build covering ranges; no longer especially valuable given use of FullRoute
        // NOTE: ExclusiveSyncPoint *relies* on STARTED_BEFORE to ensure it reports a dependency on *every* earlier TxnId that may execute after it.
        //       This is necessary for reporting to a bootstrapping replica which TxnId it must not prune from dependencies
        //       i.e. the source replica reports to the target replica those TxnId that STARTED_BEFORE and EXECUTES_AFTER.

        Route<?> scope = route.slice(ranges);
        try (Deps.AbstractBuilder<PartialDeps> builder = new PartialDeps.Builder(scope);
             Deps.AbstractBuilder<PartialDeps> redundantBuilder = new PartialDeps.Builder(scope))
        {
            safeStore.mapReduceActive(keys, ranges, executeAt, txnId.kind().witnesses(),
                                      (p1, keyOrRange, testTxnId, testExecuteAt, in) -> {
                                          if (p1 == null || !testTxnId.equals(p1))
                                              in.add(keyOrRange, testTxnId);
                                          return in;
                                      }, executeAt.equals(txnId) ? null : txnId, builder);

            // TODO (required): make sure any sync point is in the past
            PartialDeps redundant = safeStore.commandStore().redundantBefore().collectDeps(keys, redundantBuilder, minEpoch, executeAt).build();
            return builder.build().with(redundant);
        }
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
        if (originalShards.stream().anyMatch(s -> s.nodes.size() > 64))
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
                while (nvi < nv.nodes.size() && ovi < ov.nodes.size())
                {
                    int c = nv.nodes.get(nvi).compareTo(ov.nodes.get(ovi));
                    if (c < 0) ++nvi;
                    else if (c == 0) { ++nvi; ++ovi; }
                    // TODO (required): consider if this needs to be >=
                    //    consider case where one (or more) of the original nodes is bootstrapping from other original nodes
                    else if (Long.bitCount(removals[o] |= 1L << ovi++) > minMaxFailures)
                        return true;
                }
                while (ovi < ov.nodes.size())
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
