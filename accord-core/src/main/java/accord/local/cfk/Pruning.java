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

import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.btree.BTree;
import accord.utils.btree.BTreeRemoval;
import accord.utils.btree.UpdateFunction;

import static accord.local.cfk.CommandsForKey.InternalStatus.APPLIED;
import static accord.local.cfk.CommandsForKey.InternalStatus.INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED;
import static accord.local.cfk.CommandsForKey.NO_TXNIDS;
import static accord.local.cfk.CommandsForKey.managesExecution;
import static accord.local.cfk.Pruning.LoadingPruned.LOADINGF;
import static accord.utils.ArrayBuffers.cachedTimestamps;
import static accord.utils.ArrayBuffers.cachedTxnIds;

public class Pruning
{
    /**
     * A TxnId that we have witnessed as a dependency that predates {@link #prunedBefore}, so we must load its
     * Command state to determine if this is a new transaction to track, or if it is an already-applied transaction
     * we have pruned.
     *
     * Intended to be stored in a BTree, and includes static methods for managing the BTree.
     */
    static class LoadingPruned extends TxnId
    {
        static final UpdateFunction.Simple<LoadingPruned> LOADINGF = UpdateFunction.Simple.of(LoadingPruned::merge);

        /**
         * Transactions that had witnessed this pre-pruned TxnId and are therefore waiting for the load to complete
         */
        final TxnId[] witnessedBy;

        public LoadingPruned(TxnId copy, TxnId[] witnessedBy)
        {
            super(copy);
            this.witnessedBy = witnessedBy;
        }

        LoadingPruned merge(LoadingPruned that)
        {
            return new LoadingPruned(this, SortedArrays.linearUnion(witnessedBy, that.witnessedBy, cachedTxnIds()));
        }

        static Object[] empty()
        {
            return BTree.empty();
        }
    }

    /**
     * Updating {@code loadingPruned} to register that each element of {@code toLoad} is being loaded for {@code loadingFor}
     */
    static Object[] loadPruned(Object[] loadingPruned, TxnId[] toLoad, TxnId loadingFor)
    {
        return loadPruned(loadingPruned, toLoad, new TxnId[]{ loadingFor });
    }

    static Object[] loadPruned(Object[] loadingPruned, TxnId[] toLoad, TxnId[] loadingForAsList)
    {
        Object[] toLoadAsTree;
        try (BTree.FastBuilder<LoadingPruned> fastBuilder = BTree.fastBuilder())
        {
            for (TxnId txnId : toLoad)
                fastBuilder.add(new LoadingPruned(txnId, loadingForAsList));
            toLoadAsTree = fastBuilder.build();
        }
        return BTree.update(loadingPruned, toLoadAsTree, LoadingPruned::compareTo, LOADINGF);
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
     * Updating {@code loadingPruned} to remove {@code find}, as it has been loaded
     */
    static Object[] removeLoadingPruned(Object[] loadingPruned, TxnId find)
    {
        return BTreeRemoval.remove(loadingPruned, TxnId::compareTo, find);
    }

    /**
     * Return true if {@code waitingId} is waiting for any transaction with a lower TxnId than waitingExecuteAt
     */
    static boolean isWaitingOnPruned(Object[] loadingPruned, TxnId waitingId, Timestamp waitingExecuteAt)
    {
        if (BTree.isEmpty(loadingPruned))
            return false;

        int ceilIndex = BTree.ceilIndex(loadingPruned, Timestamp::compareTo, waitingExecuteAt);
        // TODO (desired): this is O(n.lg n), whereas we could import the accumulate function and perform in O(max(m, lg n))
        for (int i = 0; i < ceilIndex; ++i)
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
        TxnInfo newPrunedBefore;
        {
            if (cfk.maxAppliedWriteByExecuteAt < pruneInterval)
                return cfk;

            int i = cfk.maxAppliedWriteByExecuteAt;
            long maxPruneHlc = cfk.committedByExecuteAt[i].executeAt.hlc() - minHlcDelta;
            while (--i >= 0)
            {
                TxnInfo txn = cfk.committedByExecuteAt[i];
                if (txn.kind().isWrite() && txn.executeAt.hlc() <= maxPruneHlc && txn.status == APPLIED)
                    break;
            }

            if (i < 0)
                return cfk;

            newPrunedBefore = cfk.committedByExecuteAt[i];
            if (newPrunedBefore.compareTo(cfk.prunedBefore) <= 0)
                return cfk;
        }

        int pos = cfk.insertPos(newPrunedBefore);
        if (pos == 0)
            return cfk;

        return pruneBefore(cfk, newPrunedBefore, pos);
    }

    /**
     * We can prune anything transitively applied where some later stable command replicates each of its missing array entries.
     * These later commands can durably stand in for any recovery or dependency calculations.
     */
    static CommandsForKey pruneBefore(CommandsForKey cfk, TxnInfo newPrunedBefore, int pos)
    {
        Invariants.checkArgument(newPrunedBefore.compareTo(cfk.prunedBefore) >= 0, "Expect new prunedBefore to be ahead of existing one");

        TxnInfo[] byId = cfk.byId;
        int minUndecidedById = -1;
        int retainCount = 0, removedCommittedCount = 0;
        Timestamp[] removedExecuteAts = NO_TXNIDS;
        int removedExecuteAtCount = 0;
        TxnInfo[] newInfos;
        {
            ArrayBuffers.RecursiveObjectBuffers<TxnId> missingBuffers = new ArrayBuffers.RecursiveObjectBuffers<>(cachedTxnIds());
            TxnId[] mergedMissing = newPrunedBefore.missing();
            int mergedMissingCount = mergedMissing.length;
            for (int i = 0 ; i < pos ; ++i)
            {
                TxnInfo txn = byId[i];
                switch (txn.status)
                {
                    default: throw new AssertionError("Unhandled status: " + txn.status);
                    case COMMITTED:
                    case STABLE:
                        ++retainCount;
                        break;

                    case HISTORICAL:
                    case TRANSITIVELY_KNOWN:
                    case PREACCEPTED_OR_ACCEPTED_INVALIDATE:
                    case ACCEPTED:
                        if (minUndecidedById < 0 && managesExecution(txn))
                            minUndecidedById = retainCount;
                        ++retainCount;
                        break;

                    case APPLIED:
                        if (txn.executeAt.compareTo(newPrunedBefore.executeAt) < 0)
                        {
                            TxnId[] missing = txn.missing();
                            if (missing == NO_TXNIDS || SortedArrays.isSubset(missing, 0, missing.length, mergedMissing, 0, mergedMissingCount))
                            {
                                if (missing != NO_TXNIDS)
                                {
                                    if (removedExecuteAtCount == removedExecuteAts.length)
                                        removedExecuteAts = cachedTimestamps().resize(removedExecuteAts, removedExecuteAtCount, Math.max(8, removedExecuteAtCount + (removedExecuteAtCount >> 1)));
                                    removedExecuteAts[removedExecuteAtCount++] = txn.executeAt;
                                }
                                ++removedCommittedCount;
                                continue;
                            }
                            if (txn.executeAt == txn)
                            {
                                mergedMissing = SortedArrays.linearUnion(missing, missing.length, mergedMissing, mergedMissingCount, missingBuffers);
                                mergedMissingCount = missingBuffers.sizeOfLast(mergedMissing);
                            }
                        }
                        ++retainCount;

                    case INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED:
                        break;
                }
            }

            if (pos == retainCount)
                return cfk;

            int removedByIdCount = pos - retainCount;
            newInfos = new TxnInfo[byId.length - removedByIdCount];
            if (minUndecidedById < 0 && cfk.minUndecidedById >= 0) // must occur later, so deduct all removals
                minUndecidedById = cfk.minUndecidedById - removedByIdCount;
            missingBuffers.discardBuffers();
        }

        {   // copy to new byTxnId array
            int insertPos = retainCount;
            int removedExecuteAtPos = removedExecuteAtCount - 1;
            for (int i = pos - 1; i >= 0 ; --i)
            {
                TxnInfo txn = byId[i];
                if (txn.status == INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED)
                    continue;

                if (txn.status == APPLIED && txn.executeAt.compareTo(newPrunedBefore.executeAt) < 0)
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

                newInfos[--insertPos] = txn;
            }
            Invariants.checkState(retainCount + byId.length - pos == newInfos.length);
            System.arraycopy(byId, pos, newInfos, retainCount, byId.length - pos);
        }

        TxnInfo[] committedByExecuteAt = cfk.committedByExecuteAt;
        TxnInfo[] newCommittedByExecuteAt;
        {   // copy to new committedByExecuteAt array
            Arrays.sort(removedExecuteAts, 0, removedExecuteAtCount);
            newCommittedByExecuteAt = new TxnInfo[committedByExecuteAt.length - removedCommittedCount];
            int sourcePos = Arrays.binarySearch(committedByExecuteAt, newPrunedBefore, TxnInfo::compareExecuteAt);
            int insertPos = sourcePos - removedCommittedCount;
            int removedExecuteAtPos = removedExecuteAtCount - 1;
            for (int i = sourcePos - 1; i >= 0 ; --i)
            {
                TxnInfo txn = committedByExecuteAt[i];
                if (txn.status == APPLIED && txn.compareTo(newPrunedBefore) < 0)
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

                newCommittedByExecuteAt[--insertPos] = txn;
            }
            System.arraycopy(committedByExecuteAt, sourcePos, newCommittedByExecuteAt, sourcePos - removedCommittedCount, committedByExecuteAt.length - sourcePos);
        }

        cachedTimestamps().forceDiscard(removedExecuteAts, removedExecuteAtCount);
        int newMaxAppliedWriteByExecuteAt = cfk.maxAppliedWriteByExecuteAt - removedCommittedCount;
        return new CommandsForKey(cfk.key, cfk.redundantBefore, newPrunedBefore, cfk.loadingPruned, newInfos, newCommittedByExecuteAt, minUndecidedById, newMaxAppliedWriteByExecuteAt, cfk.unmanageds);
    }
}
