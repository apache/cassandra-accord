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

package accord.local.cfk;

import java.util.Arrays;
import java.util.List;

import accord.local.RedundantBefore.QuickBounds;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers.RecursiveObjectBuffers;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.UnhandledEnum;
import accord.utils.btree.BTree;
import accord.utils.btree.BTreeRemoval;
import accord.utils.btree.BulkIterator;
import accord.utils.btree.UpdateFunction;
import org.agrona.collections.Long2ObjectHashMap;

import static accord.api.ProtocolModifiers.isTransitiveDependencyVisible;
import static accord.local.CommandSummaries.SummaryStatus.APPLIED;
import static accord.local.cfk.CommandsForKey.InternalStatus.APPLIED_DURABLE;
import static accord.local.cfk.CommandsForKey.InternalStatus.COMMITTED;
import static accord.local.cfk.CommandsForKey.InternalStatus.PRUNED;
import static accord.local.cfk.CommandsForKey.InternalStatus.STABLE;
import static accord.local.cfk.CommandsForKey.appliedBefore;
import static accord.local.cfk.CommandsForKey.readyAt;
import static accord.local.cfk.CommandsForKey.insertPos;
import static accord.local.cfk.CommandsForKey.managesExecution;
import static accord.local.cfk.CommandsForKey.mayExecute;
import static accord.local.cfk.CommandsForKey.redundantBefore;
import static accord.local.cfk.CommandsForKey.reportLinearizabilityViolations;
import static accord.local.cfk.Updating.nextUndecided;
import static accord.local.cfk.Updating.recomputeMaxAppliedUnreadyWriteById;
import static accord.local.cfk.Utils.removeRedundantMissing;
import static accord.primitives.Timestamp.Flag.UNSTABLE;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.ArrayBuffers.cachedAny;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static accord.utils.Invariants.Paranoia.LINEAR;
import static accord.utils.Invariants.Paranoia.NONE;
import static accord.utils.Invariants.ParanoiaCostFactor.LOW;
import static accord.utils.Invariants.testParanoia;
import static accord.utils.SortedArrays.Search.FLOOR;
import static accord.utils.btree.BTree.Dir.ASC;
import static accord.utils.btree.UpdateFunction.noOp;

public class Pruning
{
    /**
     * A TxnId that we have witnessed as a dependency that predates {@link CommandsForKey#prunedBefore()}, so we must load its
     * Command state to determine if this is a new transaction to track, or if it is an already-applied transaction
     * we have pruned.
     *
     * Intended to be stored in a BTree, and includes static methods for managing the BTree.
     */
    static class LoadingPruned extends TxnId
    {
        static class Merge implements UpdateFunction<TxnId, LoadingPruned>
        {
            private final TxnId[] witnessedBy;
            private TxnId[] witnessedByUnstable;
            private final List<TxnId> inserted;
            private final boolean isVisible;

            Merge(TxnId[] witnessedBy, List<TxnId> inserted, boolean isVisible)
            {
                this.isVisible = isVisible;
                this.witnessedBy = witnessedBy;
                this.inserted = inserted;
            }

            @Override
            public LoadingPruned insert(TxnId insert)
            {
                TxnId[] witnessedBy = this.witnessedBy;
                if (insert.is(UNSTABLE))
                {
                    insert = insert.withoutNonIdentityFlags();
                    // TODO (desired): this is a bit ugly - maybe we should save all prunedIds in byId by default?
                    if (witnessedByUnstable == null)
                        witnessedByUnstable = new TxnId[] { witnessedBy[0].addFlag(UNSTABLE) };
                    witnessedBy = witnessedByUnstable;
                }
                inserted.add(insert);
                return new LoadingPruned(insert, witnessedBy, isVisible);
            }

            @Override
            public LoadingPruned merge(LoadingPruned replacing, TxnId update)
            {
                return new LoadingPruned(update, SortedArrays.linearUnion(replacing.witnessedBy, witnessedBy, cachedTxnIds()), replacing.isVisible | isVisible);
            }
        }

        /**
         * Transactions that had witnessed this pre-pruned TxnId and are therefore waiting for the load to complete
         */
        final TxnId[] witnessedBy;
        final boolean isVisible;

        public LoadingPruned(TxnId copy, TxnId[] witnessedBy, boolean isVisible)
        {
            super(copy);
            this.witnessedBy = witnessedBy;
            this.isVisible = isVisible;
        }

        LoadingPruned merge(LoadingPruned that)
        {
            return new LoadingPruned(this, SortedArrays.linearUnion(witnessedBy, that.witnessedBy, cachedTxnIds()), this.isVisible | that.isVisible);
        }

        TxnId plainTxnId()
        {
            return new TxnId(this);
        }

        static Object[] empty()
        {
            return BTree.empty();
        }
    }

    /**
     * Updating {@code loadingPruned} to register that each element of {@code toLoad} is being loaded for {@code loadingFor}
     */
    static Object[] loadPruned(Object[] loadingPruned, TxnId[] toLoad, TxnId witnessedBy, List<TxnId> inserted)
    {
        return loadPruned(loadingPruned, toLoad, new TxnId[]{ witnessedBy }, inserted, isTransitiveDependencyVisible(witnessedBy));
    }

    private static Object[] loadPruned(Object[] loadingPruned, TxnId[] toLoad, TxnId[] witnessedBy, List<TxnId> inserted, boolean isVisible)
    {
        Object[] toLoadAsTree = BTree.build(BulkIterator.of(toLoad), toLoad.length, UpdateFunction.noOp());
        return BTree.update(loadingPruned, toLoadAsTree, TxnId::compareTo, new LoadingPruned.Merge(witnessedBy, inserted, isVisible));
    }

    /**
     * Find the list of TxnId that are waiting for {@code find} to load
     */
    static TxnId[] loadingPrunedFor(Object[] loadingPruned, TxnId find, TxnId[] ifNoMatch)
    {
        LoadingPruned obj = (LoadingPruned) BTree.find(loadingPruned, TxnId::compareTo, find);
        if (obj == null)
            return ifNoMatch;

        return obj.witnessedBy;
    }

    /**
     * Find the list of TxnId that are waiting for {@code find} to load
     */
    static LoadingPruned find(Object[] loadingPruned, TxnId find)
    {
        return (LoadingPruned) BTree.find(loadingPruned, TxnId::compareTo, find);
    }

    /**
     * Updating {@code loadingPruned} to remove {@code find}, as it has been loaded
     */
    static Object[] removeLoadingPruned(Object[] loadingPruned, TxnId find)
    {
        return BTreeRemoval.remove(loadingPruned, TxnId::compareTo, find);
    }

    /**
     * Return true if {@code waitingId} is waiting for any transaction with a lower TxnId than waitingExecuteAt
     */
    static boolean isWaitingOnPruned(Object[] loadingPruned, TxnId waitingId, Timestamp waitingExecuteAt, QuickBounds bounds)
    {
        if (BTree.isEmpty(loadingPruned))
            return false;

        int startIndex = BTree.ceilIndex(loadingPruned, Timestamp::compareTo, bounds.readyAt);
        int endIndex = BTree.ceilIndex(loadingPruned, Timestamp::compareTo, waitingExecuteAt);
        // TODO (desired): this is O(n.lg n), whereas we could import the accumulate function and perform in O(max(m, lg n))
        for (int i = startIndex; i < endIndex; ++i)
        {
            LoadingPruned loading = BTree.findByIndex(loadingPruned, i);
            if (!managesExecution(loading)) continue;
            if (Arrays.binarySearch(loading.witnessedBy, waitingId) >= 0)
                return true;
        }

        return false;
    }

    /**
     * Return true if {@code waitingId} is waiting for any transaction with a lower TxnId than waitingExecuteAt
     */
    static boolean isAnyPredecessorWaitingOnPruned(Object[] loadingPruned, TxnId waitingId)
    {
        if (BTree.isEmpty(loadingPruned))
            return false;

        int ceilIndex = BTree.ceilIndex(loadingPruned, Timestamp::compareTo, waitingId);
        // TODO (desired): this is O(n.lg n), whereas we could import the accumulate function and perform in O(max(m, lg n))
        for (int i = 0; i < ceilIndex; ++i)
        {
            LoadingPruned loading = BTree.findByIndex(loadingPruned, i);
            if (!managesExecution(loading)) continue;
            // if we exactly match any, or if we sort after the first element then we're waiting for some txnId <= waitingId
            if (-1 != Arrays.binarySearch(loading.witnessedBy, waitingId))
                return true;
        }

        return false;
    }

    /**
     * Remove transitively redundant applied or invalidated commands
     * @param pruneInterval the number of committed commands we must have prior to the first prune point candidate to trigger a prune attempt
     * @param minHlcDelta do not prune any commands with an HLC within this distance of the prune point candidate
     */
    static CommandsForKey maybePrune(CommandsForKey cfk, int pruneInterval, long minHlcDelta)
    {
        TxnInfo newPruneBefore = newPruneBefore(cfk, pruneInterval, minHlcDelta);
        if (newPruneBefore == null)
            return cfk;

        int pos = cfk.insertPos(newPruneBefore);
        if (pos == 0)
            return cfk;

        return pruneBefore(cfk, newPruneBefore, pos);
    }

    static CommandsForKey maximalPrune(CommandsForKey cfk)
    {
        TxnInfo pruneBefore = newPruneBefore(cfk, 0, 0);
        if (pruneBefore == null)
        {
            int prunedCount = 0;
            for (int i = 0 ; i < cfk.size() ; ++i)
                prunedCount += cfk.get(i).is(PRUNED) ? 1 : 0;

            if (prunedCount == 0)
                return cfk;

            TxnInfo[] newById = new TxnInfo[cfk.byId.length - prunedCount];
            int count = 0;
            for (int i = 0 ; i < cfk.size() ; ++i)
            {
                TxnInfo txn = cfk.get(i);
                if (!txn.is(PRUNED))
                    newById[count++] = txn;
            }
            int newMinUndecidedManagedById = nextUndecided(true, newById, 0, cfk);
            int newMinUndecidedUnmanagedById = nextUndecided(false, newById, 0, cfk);
            int newMaxAppliedUnreadyWriteById = recomputeMaxAppliedUnreadyWriteById(cfk.bounds, newById, cfk.maxAppliedUnreadyWriteById);
            int newPrunedBeforeId = cfk.prunedBeforeById - prunedCount;
            return new CommandsForKey(cfk.key, cfk.bounds, false, newById,
                                      newMinUndecidedManagedById, newMinUndecidedUnmanagedById, newMaxAppliedUnreadyWriteById,
                                      cfk.committedByExecuteAt, cfk.maxAppliedWriteByExecuteAt,
                                      cfk.maxUniqueHlc, cfk.loadingPruned, newPrunedBeforeId,
                                      cfk.unmanageds, true);
        }
        int pos = cfk.insertPos(pruneBefore);
        if (pos == 0)
            return cfk;

        return pruneBefore(cfk, pruneBefore, pos);
    }

    private static TxnInfo newPruneBefore(CommandsForKey cfk, int pruneInterval, long minHlcDelta)
    {
        if (cfk.maxAppliedWriteByExecuteAt < pruneInterval)
            return null;

        int i = cfk.maxAppliedWriteByExecuteAt;
        long maxPruneHlc = cfk.committedByExecuteAt[i].executeAt.hlc() - minHlcDelta;
        while (--i >= 0)
        {
            TxnInfo txn = cfk.committedByExecuteAt[i];
            if (txn.is(Write) && txn.executeAt.hlc() <= maxPruneHlc && txn.is(APPLIED) && txn.epoch() == txn.executeAt.epoch())
                break;
        }

        if (i < 0)
            return null;

        TxnInfo newPrunedBefore = cfk.committedByExecuteAt[i];
        if (newPrunedBefore.compareSimultaneousEpochAndHlc(cfk.prunedBefore()) <= 0)
            return null;
        if (newPrunedBefore.executeAt.compareSimultaneousEpochAndHlc(cfk.prunedBefore().executeAt) <= 0)
            return null;
        return newPrunedBefore;
    }

    /**
     * We can prune anything transitively applied where some later stable command replicates each of its missing array entries.
     * These later commands can durably stand in for any recovery or dependency calculations.
     *
     * TODO (desired): we could limit this restriction to epochs where ownership changes; introduce some global summary info to facilitate this
     * TODO (desired): we may be able prune more transactions that cross epochs if we have a prune point in both epochs,
     *   where the execution epoch prune point as ahead of the executeAt, and the coordination epoch prune point is ahead of the TxnId
     * TODO (expected): remove any unmanaged transactions that precede the prune point
     *  this requires updating txn.missing collections, so might be preferable as a separate pass
     */
    static CommandsForKey pruneBefore(CommandsForKey cfk, TxnInfo newPrunedBefore, int pos)
    {
        Invariants.requireArgument(newPrunedBefore.compareSimultaneousEpochAndHlc(cfk.prunedBefore()) > 0, "Expect new prunedBefore to have > HLC and >= epoch (%s vs %s)", newPrunedBefore, cfk.prunedBefore());
        Invariants.requireArgument(newPrunedBefore.executeAt.compareSimultaneousEpochAndHlc(cfk.prunedBefore().executeAt) > 0, "Expect new prunedBefore.executeAt to have > HLC and >= epoch (%s vs %s)", newPrunedBefore.executeAt, cfk.prunedBefore().executeAt);
        Invariants.requireArgument(newPrunedBefore.mayExecute());

        TxnInfo[] byId = cfk.byId;
        TxnInfo[] committedByExecuteAt = cfk.committedByExecuteAt;
        int minUndecidedManagedById, minUndecidedUnmanagedById;
        int retainCount = 0, removedCommittedCount = 0;
        // a store of committed executeAts we have removed where we cannot otherwise cheaply infer it
        Object[] removedExecuteAts = NO_TXNIDS;
        int removedExecuteAtCount = 0;
        Long2ObjectHashMap<TxnInfo> epochPrunedBefores = buildEpochPrunedBefores(byId, committedByExecuteAt, newPrunedBefore);
        TxnInfo[] newById;
        {
            minUndecidedManagedById = cfk.minUndecidedManagedById;
            minUndecidedUnmanagedById = cfk.minUndecidedUnmanagedById;
            int minUndecidedManagedByIdDelta = 0, minUndecidedUnmanagedByIdDelta = 0;
            RecursiveObjectBuffers<TxnId> missingBuffers = new RecursiveObjectBuffers<>(cachedTxnIds());
            TxnId[] mergedMissing = newPrunedBefore.missing();
            int mergedMissingCount = mergedMissing.length;
            TxnInfo activePruneEpochBefore = newPrunedBefore; // note that if activePrunedBefore != newPrunedBefore, we compare with executeAt
            long activePruneEpoch = activePruneEpochBefore.epoch();
            Object[] newByIdBuffer = cachedAny().get(pos);

            for (int i = pos - 1 ; i >= 0 ; --i)
            {
                TxnInfo txn = byId[i];
                switch (txn.status())
                {
                    default: throw new UnhandledEnum(txn.status());
                    case COMMITTED:
                    case STABLE:
                        if (txn.mayExecute())
                        {
                            newByIdBuffer[pos - ++retainCount] = txn;
                        }
                        else
                        {
                            long epoch = txn.epoch();
                            if (epoch != activePruneEpoch && epochPrunedBefores != null)
                            {
                                activePruneEpochBefore = epochPrunedBefores.get(epoch);
                                activePruneEpoch = epoch;
                            }

                            if (activePruneEpochBefore == txn || (activePruneEpochBefore == newPrunedBefore && activePruneEpochBefore.executeAt.compareTo(txn.executeAt) <= 0))
                                newByIdBuffer[pos - ++retainCount] = txn;
                        }
                        break;

                    case TRANSITIVE:
                    case TRANSITIVE_VISIBLE:
                    case PREACCEPTED_WITHOUT_DEPS:
                    case PREACCEPTED_WITH_DEPS:
                    case PREACCEPTED_COORD:
                    case PREACCEPTED_COORD_RECOVERY:
                    case NOTACCEPTED:
                    case ACCEPTED:
                    case ACCEPTED_COORD:
                    case ACCEPTED_COORD_RECOVERY:
                    case APPLIED_NOT_DURABLE:
                    case APPLIED_NOT_EXECUTED:
                        newByIdBuffer[pos - ++retainCount] = txn;
                        if (i == minUndecidedManagedById)
                            minUndecidedManagedByIdDelta = retainCount;
                        if (i == minUndecidedUnmanagedById)
                            minUndecidedUnmanagedByIdDelta = retainCount;
                        break;

                    case APPLIED_DURABLE:
                        long epoch = txn.epoch();
                        if (epoch != activePruneEpoch && epochPrunedBefores != null)
                        {
                            activePruneEpochBefore = epochPrunedBefores.get(epoch);
                            activePruneEpoch = epoch;
                        }

                        boolean tryPrune = activePruneEpochBefore != null
                                           && txn.executeAt.compareTo(activePruneEpochBefore.executeAt) < 0
                                           && txn.compareTo(activePruneEpochBefore) < 0;
                        if (tryPrune)
                        {
                            TxnId[] missing = txn.missing();
                            if (missing == NO_TXNIDS || SortedArrays.isSubset(missing, 0, missing.length, mergedMissing, 0, mergedMissingCount))
                            {
                                if (txn.mayExecute())
                                {   // if we don't execute, we don't track in committedByExecuteAt, so don't need to update bookkeeping for removing from there
                                    if (missing != NO_TXNIDS)
                                    {
                                        if (removedExecuteAtCount == removedExecuteAts.length)
                                            removedExecuteAts = cachedAny().resize(removedExecuteAts, removedExecuteAtCount, Math.max(8, removedExecuteAtCount + (removedExecuteAtCount >> 1)));
                                        removedExecuteAts[removedExecuteAtCount++] = txn.executeAt;
                                    }
                                    ++removedCommittedCount;
                                }
                                continue;
                            }

                            if (txn.executeAt == txn)
                            {
                                mergedMissing = SortedArrays.linearUnion(missing, missing.length, mergedMissing, mergedMissingCount, missingBuffers);
                                mergedMissingCount = missingBuffers.sizeOfLast(mergedMissing);
                            }
                        }
                        newByIdBuffer[pos - ++retainCount] = txn;

                    case INVALIDATED:
                    case PRUNED:
                    case ERASED:
                        break;
                }
            }

            if (pos == retainCount)
                return cfk;

            int removedByIdCount = pos - retainCount;
            if (minUndecidedManagedById >= 0)
            {
                if (minUndecidedManagedById >= pos)
                    minUndecidedManagedById -= removedByIdCount;
                else
                    minUndecidedManagedById = retainCount - minUndecidedManagedByIdDelta;
            }
            if (minUndecidedUnmanagedById >= 0)
            {
                if (minUndecidedUnmanagedById >= pos)
                    minUndecidedUnmanagedById -= removedByIdCount;
                else
                    minUndecidedUnmanagedById = retainCount - minUndecidedUnmanagedByIdDelta;
            }
            newById = new TxnInfo[byId.length - removedByIdCount];
            System.arraycopy(newByIdBuffer, pos - retainCount, newById, 0, retainCount);
            System.arraycopy(byId, pos, newById, retainCount, byId.length - pos);
            missingBuffers.discardBuffers();
        }

        TxnInfo[] newCommittedByExecuteAt;
        {   // copy to new committedByExecuteAt array
            Arrays.sort(removedExecuteAts, 0, removedExecuteAtCount);
            newCommittedByExecuteAt = new TxnInfo[committedByExecuteAt.length - removedCommittedCount];
            int sourcePos = Arrays.binarySearch(committedByExecuteAt, newPrunedBefore, TxnInfo::compareExecuteAt);
            int insertPos = sourcePos - removedCommittedCount;
            int removedExecuteAtPos = removedExecuteAtCount - 1;
            TxnInfo activePruneEpochBefore = newPrunedBefore; // note that if activePrunedBefore != newPrunedBefore, we compare with executeAt
            long activePruneEpoch = newPrunedBefore.epoch();
            for (int i = sourcePos - 1; i >= 0 ; --i)
            {
                TxnInfo txn = committedByExecuteAt[i];
                if (txn.is(APPLIED_DURABLE))
                {
                    long epoch = txn.epoch();
                    if (epoch != activePruneEpoch && epochPrunedBefores != null)
                    {
                        activePruneEpochBefore = epochPrunedBefores.get(epoch);
                        activePruneEpoch = epoch;
                    }

                    boolean tryPrune = activePruneEpochBefore != null
                                       && txn.compareTo(activePruneEpochBefore) < 0
                                       && txn.executeAt.compareTo(activePruneEpochBefore.executeAt) < 0;
                    if (tryPrune)
                    {
                        TxnId[] missing = txn.missing();
                        if (missing == NO_TXNIDS)
                            continue;

                        if (removedExecuteAtPos >= 0 && removedExecuteAts[removedExecuteAtPos] == txn.executeAt)
                        {
                            --removedExecuteAtPos;
                            continue;
                        }
                    }
                }

                newCommittedByExecuteAt[--insertPos] = txn;
            }
            System.arraycopy(committedByExecuteAt, sourcePos, newCommittedByExecuteAt, sourcePos - removedCommittedCount, committedByExecuteAt.length - sourcePos);
        }

        cachedAny().forceDiscard(removedExecuteAts, removedExecuteAtCount);
        int newMaxAppliedWriteByExecuteAt = cfk.maxAppliedWriteByExecuteAt - removedCommittedCount;
        int newMaxAppliedUnreadyWriteById = recomputeMaxAppliedUnreadyWriteById(cfk.bounds, newById, cfk.maxAppliedUnreadyWriteById);
        Invariants.require(newById[retainCount].equals(newPrunedBefore));
        return new CommandsForKey(cfk.key, cfk.bounds, newById, minUndecidedManagedById, minUndecidedUnmanagedById, newMaxAppliedUnreadyWriteById, newCommittedByExecuteAt, newMaxAppliedWriteByExecuteAt, cfk.maxUniqueHlc, cfk.loadingPruned, retainCount, cfk.unmanageds, true);
    }

    /**
     * Pruning operates on the assumption it is safe to return a future transaction as an execution dependency for sync points and exclusive sync points;
     * due to the way we execute these transactions, by stabilising all of their dependencies (transitively) before taking our Apply point,
     * this is true _so long_ as we participate in the epochs of the future transactions.
     * So, to facilitate this we retain the highest committed transaction for each epoch, that would otherwise be pruned.
     *
     * TODO (formalise): We filter dependencies by TxnId and execute by executeAt, so our prune points need to be based on both
     *  so we don't filter a dependency by TxnId that we need to see by executeAt (or vice-versa).
     *  We might just want to strengthen the replica dependency processing anyway.
     */
    private static Long2ObjectHashMap<TxnInfo> buildEpochPrunedBefores(TxnInfo[] byId, TxnInfo[] committedByExecuteAt, TxnInfo newPrunedBefore)
    {
        if (byId[0].epoch() == committedByExecuteAt[committedByExecuteAt.length - 1].executeAt.epoch())
            return null;

        Long2ObjectHashMap<TxnInfo> epochPrunedBefores = new Long2ObjectHashMap<>();
        epochPrunedBefores.put(newPrunedBefore.epoch(), newPrunedBefore);

        int maxi = -1 - Arrays.binarySearch(committedByExecuteAt, newPrunedBefore, (a, b) -> a.compareExecuteAtEpoch(b) >= 0 ? 1 : -1);
        int i = 0;
        while (i < maxi)
        {
            TxnInfo test = committedByExecuteAt[i];
            if (!test.is(Write) || !test.is(APPLIED) || test.epoch() != test.executeAt.epoch())
            {
                ++i;
                continue;
            }

            Object prev = epochPrunedBefores.putIfAbsent(test.epoch(), test);
            Invariants.require(prev == null);

            i = SortedArrays.exponentialSearch(committedByExecuteAt, i + 1, committedByExecuteAt.length, test, TxnInfo::compareExecuteAtEpoch, FLOOR);
            if (i < 0) i = -1 - i;
            else i = i + 1;
        }
        return epochPrunedBefores;
    }

    static TxnInfo[] removeRedundantById(TxnInfo[] byId, boolean hasRedundantLoadingPruned, QuickBounds prevBounds, QuickBounds newBounds, boolean expectUpToDate)
    {
        TxnId newAppliedBefore = appliedBefore(newBounds);
        TxnId newGcBefore = redundantBefore(newBounds);
        TxnId newReadyAt = readyAt(newBounds);
        TxnId prevGcBefore = redundantBefore(prevBounds);
        TxnId prevReadyAt = readyAt(prevBounds);
        Invariants.requireArgument(newGcBefore.compareTo(prevGcBefore) >= 0, "Expect new RedundantBefore.Entry gcBefore to be ahead of existing one");
        Invariants.requireArgument(prevReadyAt == null || newGcBefore.compareTo(prevReadyAt) >= 0 || (newReadyAt != null && newReadyAt.compareTo(prevReadyAt) >= 0), "Expect new RedundantBefore.Entry readyAt to be ahead of existing one");

        TxnInfo[] newById = byId;
        int pos = insertPos(byId, newGcBefore);
        int appliedPos = Arrays.binarySearch(byId, pos, byId.length, newAppliedBefore);
        if (appliedPos < 0) appliedPos = -1 - appliedPos;
        if (pos != 0 || appliedPos != 0 || hasRedundantLoadingPruned)
        {
            if (Invariants.isParanoid() && expectUpToDate && testParanoia(LINEAR, NONE, LOW))
            {
                int startPos = prevReadyAt == null ? 0 : insertPos(byId, prevReadyAt);
                for (int i = startPos ; i < pos ; ++i)
                    Invariants.require((byId[i].isNot(COMMITTED) && byId[i].isNot(STABLE)) || !byId[i].mayExecute() || !reportLinearizabilityViolations(), "%s redundant; expected to be applied, undecided or to execute in a future epoch", byId[i]);
            }

            int removeUnappliedCount = 0;
            if (appliedPos > pos)
            {
                // we apply additional filtering to remove any transactions we know would apply locally, but haven't executed
                // so we know they will not execute. note: we cannot do this safely for any transactions we don't execute locally!
                // this is used to handle consistent restore on replay, where we may have some transaction that is logically
                // invalidated, but we may only record the invalidation after the snapshot is created because the RX doesn't
                // record it as a dependency (and so it doesn't have to be decided for the RX to execute).
                // We may then later invalidate and update the CFK, but since this is not reflected in the snapshot,
                // after restoring from snapshot we may have an inconsistency between the command state and the CFK state,
                // and won't know that we should update the CFK because the command is already invalidated.
                // So, to avoid having to read all CFK on startup and process any potentially invalidated transactions,
                // we instead apply this filtering aggressively whenever we know the transaction cannot apply,
                // and rely on the applied RX to correctly reject recovery of the transaction.
                for (int i = pos ; i < appliedPos ; ++i)
                {
                    if (byId[i].compareTo(APPLIED) < 0 && byId[i].mayExecute())
                    {
                        Invariants.require(!expectUpToDate || (byId[i].isNot(COMMITTED) && byId[i].isNot(STABLE)) || !reportLinearizabilityViolations(), "%s redundant; expected to be applied, undecided or to execute in a future epoch", byId[i]);
                        // we only filter those that would apply locally
                        removeUnappliedCount++;
                    }
                }
            }

            int newAppliedBeforeIndex;
            if (removeUnappliedCount > 0)
            {
                newAppliedBeforeIndex = (appliedPos - pos) - removeUnappliedCount;
                newById = new TxnInfo[(byId.length - appliedPos) + newAppliedBeforeIndex];
                removeUnappliedCount = 0;
                for (int i = pos ; i < appliedPos ; ++i)
                {
                    if (byId[i].compareTo(APPLIED) < 0 && byId[i].mayExecute()) ++removeUnappliedCount;
                    else newById[i - (pos + removeUnappliedCount)] = byId[i];
                }
                System.arraycopy(byId, appliedPos, newById, newAppliedBeforeIndex, newById.length - newAppliedBeforeIndex);
            }
            else
            {
                newAppliedBeforeIndex = -1;
                newById = Arrays.copyOfRange(byId, pos, byId.length);
            }

            for (int i = 0 ; i < newById.length ; ++i)
            {
                TxnInfo txn = newById[i];
                TxnId[] missing = txn.missing();
                if (missing == NO_TXNIDS) continue;
                missing = removeRedundantMissing(missing, newGcBefore, newAppliedBefore, newAppliedBeforeIndex, newById);
                newById[i] = txn.withMissing(missing);
            }
        }

        if (newBounds.startEpoch != prevBounds.startEpoch
            || newBounds.endEpoch != prevBounds.endEpoch
            || !newBounds.readyAt.equals(prevBounds.readyAt))
        {
            for (int i = 0 ; i < newById.length ; ++i)
            {
                TxnInfo txn = newById[i];
                txn = txn.withMayExecute(mayExecute(newBounds, txn));
                if (txn != newById[i])
                {
                    if (newById == byId)
                        newById = byId.clone();
                    newById[i] = txn;
                }
            }
        }
        return newById;
    }

    static int prunedBeforeId(TxnInfo[] byId, TxnId prunedBefore, TxnId newRedundantBefore)
    {
        if (prunedBefore.compareTo(newRedundantBefore) <= 0)
            return -1;

        int i = Arrays.binarySearch(byId, prunedBefore);
        Invariants.require(i >= 0);
        return i;
    }

    static Object[] removeRedundantLoadingPruned(Object[] loadingPruned, TxnId newRedundantBefore)
    {
        int newLoadingPrunedLowBound = BTree.findIndex(loadingPruned, TxnId::compareTo, newRedundantBefore);
        if (newLoadingPrunedLowBound < 0) newLoadingPrunedLowBound = -1 - newLoadingPrunedLowBound;
        if (newLoadingPrunedLowBound <= 0)
            return loadingPruned;

        int size = BTree.size(loadingPruned);
        return BTree.build(BulkIterator.of(BTree.iterator(loadingPruned, newLoadingPrunedLowBound, size, ASC)), size - newLoadingPrunedLowBound, noOp());
    }
}
