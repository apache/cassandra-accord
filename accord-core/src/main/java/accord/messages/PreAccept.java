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
import accord.primitives.SaveStatus;
import accord.local.StoreParticipants;
import accord.messages.TxnRequest.WithUnsynced;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.FullRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

public class PreAccept extends WithUnsynced<PreAccept.PreAcceptReply>
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

    // TODO (desired): we send partialTxn pieces to epochs that no longer own the range and that won't save it; stop
    //  (depends in part on moving to RoutingKey for CommandsForKey and Deps)
    public final PartialTxn partialTxn;
    public final FullRoute<?> route;
    public final long acceptEpoch;

    public PreAccept(Id to, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        super(to, topologies, txnId, route);
        // TODO (desired): only includeQuery if route.contains(route.homeKey()); this affects state eviction and is low priority given size in C*
        this.partialTxn = txn.intersecting(scope, true);
        this.acceptEpoch = topologies.currentEpoch();
        this.route = route;
    }

    PreAccept(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, long acceptEpoch, PartialTxn partialTxn, @Nullable FullRoute<?> fullRoute)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.partialTxn = partialTxn;
        this.acceptEpoch = acceptEpoch;
        this.route = fullRoute;
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Unseekables<?> keys()
    {
        return scope;
    }

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.SYNC;
    }

    @Override
    protected Cancellable submit()
    {
        return node.mapReduceConsumeLocal(this, minEpoch, acceptEpoch, this);
    }

    @Override
    public PreAcceptReply apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.update(safeStore, route, minEpoch, txnId, acceptEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Commands.AcceptOutcome outcome = Commands.preaccept(safeStore, safeCommand, participants, txnId, acceptEpoch, partialTxn, route);
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

                Deps deps = calculateDeps(safeStore, txnId, participants, EpochSupplier.constant(minEpoch), txnId);
                // NOTE: we CANNOT test whether we adopt a future dependency here because it might be that this command
                // is guaranteed to not reach agreement, but that this replica is unaware of that fact and has pruned
                // all preceding transactions.
                //  in which case we may be able to adopt a future dependency but won't propose it
                Invariants.checkState(deps.maxTxnId(txnId).epoch() <= txnId.epoch());
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
        Deps deps = ok1.deps.with(ok2.deps);

        if (deps == okMax.deps && witnessedAt == okMax.witnessedAt)
            return okMax;
        return new PreAcceptOk(txnId, witnessedAt, deps);
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
        public final Deps deps;

        public PreAcceptOk(TxnId txnId, Timestamp witnessedAt, Deps deps)
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

    static Deps calculateDeps(SafeCommandStore safeStore, TxnId txnId, StoreParticipants participants, EpochSupplier minEpoch, Timestamp executeAt)
    {
        // TODO (expected): do not build covering ranges; no longer especially valuable given use of FullRoute
        // NOTE: ExclusiveSyncPoint *relies* on STARTED_BEFORE to ensure it reports a dependency on *every* earlier TxnId that may execute after it.
        //       This is necessary for reporting to a bootstrapping replica which TxnId it must not prune from dependencies
        //       i.e. the source replica reports to the target replica those TxnId that STARTED_BEFORE and EXECUTES_AFTER.

        try (Deps.AbstractBuilder<Deps> builder = new Deps.Builder();
             Deps.AbstractBuilder<Deps> redundantBuilder = new Deps.Builder())
        {
            safeStore.mapReduceActive(participants.touches(), executeAt, txnId.witnesses(),
                                      (p1, keyOrRange, testTxnId, testExecuteAt, in) -> {
                                          if (p1 == null || !testTxnId.equals(p1))
                                              in.add(keyOrRange, testTxnId);
                                          return in;
                                      }, executeAt.equals(txnId) ? null : txnId, builder);

            // TODO (required): make sure any sync point is in the past
            Deps redundant = safeStore.redundantBefore().collectDeps(participants.touches(), redundantBuilder, minEpoch, executeAt).build();
            Deps result = builder.build().with(redundant);
            Invariants.checkState(!result.contains(txnId));
            return result;
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
}
