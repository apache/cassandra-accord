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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import net.nicoulaj.compilecommand.annotations.Inline;

import static accord.local.cfk.CommandsForKey.InternalStatus.ACCEPTED;
import static accord.local.cfk.CommandsForKey.InternalStatus.COMMITTED;
import static accord.local.cfk.CommandsForKey.Unmanaged.Pending.COMMIT;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static accord.utils.SortedArrays.Search.FAST;

class Utils
{
    static void validateMissing(TxnInfo[] byId, TxnId[] additions, int additionCount, TxnInfo curInfo, TxnInfo newInfo, @Nonnull TxnId[] shouldNotHaveMissing)
    {
        for (TxnInfo txn : byId)
        {
            if (txn == newInfo) continue;
            if (!txn.status.hasDeps()) continue;
            int additionIndex = Arrays.binarySearch(additions, 0, additionCount, txn.depsKnownBefore());
            if (additionIndex < 0) additionIndex = -1 - additionIndex;
            TxnId[] missing = txn.missing();
            int j = 0;
            for (int i = 0 ; i < additionIndex ; ++i)
            {
                if (!txn.kind().witnesses(additions[i])) continue;
                j = SortedArrays.exponentialSearch(missing, j, missing.length, additions[i]);
                if (shouldNotHaveMissing != NO_TXNIDS && Arrays.binarySearch(shouldNotHaveMissing, txn) >= 0) Invariants.checkState(j < 0);
                else Invariants.checkState(j >= 0);
            }
            if (curInfo == null && newInfo.status.compareTo(COMMITTED) < 0 && txn.kind().witnesses(newInfo) && txn.depsKnownBefore().compareTo(newInfo) > 0 && (shouldNotHaveMissing == NO_TXNIDS || Arrays.binarySearch(shouldNotHaveMissing, txn) < 0))
                Invariants.checkState(Arrays.binarySearch(missing, newInfo) >= 0);
        }
    }

    /**
     * {@code removeTxnId} no longer needs to be tracked in missing arrays;
     * remove it from byId and committedByExecuteAt, ensuring both arrays still reference the same TxnInfo where updated
     */
    static void removeFromMissingArrays(TxnInfo[] byId, TxnInfo[] committedByExecuteAt, TxnId removeTxnId)
    {
        int startIndex = SortedArrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, removeTxnId, (id, info) -> id.compareTo(info.executeAt), FAST);
        if (startIndex < 0) startIndex = -1 - startIndex;
        else ++startIndex;

        int minSearchIndex = Arrays.binarySearch(byId, removeTxnId) + 1;
        for (int i = startIndex ; i < committedByExecuteAt.length ; ++i)
        {
            int newMinSearchIndex;
            {
                TxnInfo txn = committedByExecuteAt[i];
                if (txn.getClass() == TxnInfo.class) continue;
                if (!txn.kind().witnesses(removeTxnId)) continue;

                TxnId[] missing = txn.missing();
                TxnId[] newMissing = removeOneMissing(missing, removeTxnId);
                if (missing == newMissing) continue;

                newMinSearchIndex = updateInfoArraysByExecuteAt(i, txn, txn.update(newMissing), minSearchIndex, byId, committedByExecuteAt);
            }

            minSearchIndex = removeFromMissingArraysById(byId, minSearchIndex, newMinSearchIndex, removeTxnId);
        }

        removeFromMissingArraysById(byId, minSearchIndex, byId.length, removeTxnId);
    }

    /**
     * {@code removeTxnId} no longer needs to be tracked in missing arrays;
     * remove it from a range of byId ACCEPTED status entries only, that could not be tracked via committedByExecuteAt
     */
    static int removeFromMissingArraysById(TxnInfo[] byId, int from, int to, TxnId removeTxnId)
    {
        for (int i = from ; i < to ; ++i)
        {
            TxnInfo txn = byId[i];
            if (txn.getClass() == TxnInfo.class) continue;
            if (!txn.status.hasExecuteAtOrDeps) continue;
            if (!txn.kind().witnesses(removeTxnId)) continue;
            if (txn.status != ACCEPTED) continue;

            TxnId[] missing = txn.missing();
            TxnId[] newMissing = removeOneMissing(missing, removeTxnId);
            if (missing == newMissing) continue;
            byId[i] = txn.update(newMissing);
        }
        return to;
    }

    /**
     * {@code insertTxnId} needs to be tracked in missing arrays;
     * add it to byId and committedByExecuteAt, ensuring both arrays still reference the same TxnInfo where updated
     * Do not insert it into any members of {@code doNotInsert} as these are known to have witnessed {@code insertTxnId}
     */
    static void addToMissingArrays(TxnInfo[] byId, TxnInfo[] committedByExecuteAt, TxnInfo newInfo, TxnId insertTxnId, @Nonnull TxnId[] doNotInsert)
    {
        TxnId[] oneMissing = null;

        int startIndex = SortedArrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, insertTxnId, (id, info) -> id.compareTo(info.executeAt), FAST);
        if (startIndex < 0) startIndex = -1 - startIndex;
        else ++startIndex;

        int minByIdSearchIndex = Arrays.binarySearch(byId, insertTxnId) + 1;
        int minDoNotInsertSearchIndex = 0;
        for (int i = startIndex ; i < committedByExecuteAt.length ; ++i)
        {
            int newMinSearchIndex;
            {
                TxnInfo txn = committedByExecuteAt[i];
                if (txn == newInfo) continue;
                if (!txn.kind().witnesses(insertTxnId)) continue;

                if (doNotInsert != NO_TXNIDS)
                {
                    if (txn.executeAt == txn)
                    {
                        int j = SortedArrays.exponentialSearch(doNotInsert, 0, doNotInsert.length, txn);
                        if (j >= 0)
                        {
                            minDoNotInsertSearchIndex = j;
                            continue;
                        }
                        minDoNotInsertSearchIndex = -1 -j;
                    }
                    else
                    {
                        if (Arrays.binarySearch(doNotInsert, txn) >= 0)
                            continue;
                    }
                }

                TxnId[] missing = txn.missing();
                if (missing == NO_TXNIDS) missing = oneMissing = ensureOneMissing(insertTxnId, oneMissing);
                else missing = SortedArrays.insert(missing, insertTxnId, TxnId[]::new);

                newMinSearchIndex = updateInfoArraysByExecuteAt(i, txn, txn.update(missing), minByIdSearchIndex, byId, committedByExecuteAt);
            }

            for (; minByIdSearchIndex < newMinSearchIndex ; ++minByIdSearchIndex)
            {
                TxnInfo txn = byId[minByIdSearchIndex];
                if (txn == newInfo) continue;
                if (!txn.status.hasExecuteAtOrDeps) continue;
                if (!txn.kind().witnesses(insertTxnId)) continue;
                if (txn.status != ACCEPTED) continue;
                if (minDoNotInsertSearchIndex < doNotInsert.length && doNotInsert[minDoNotInsertSearchIndex].equals(txn))
                {
                    ++minDoNotInsertSearchIndex;
                    continue;
                }

                TxnId[] missing = txn.missing();
                if (missing == NO_TXNIDS) missing = oneMissing = ensureOneMissing(insertTxnId, oneMissing);
                else missing = SortedArrays.insert(missing, insertTxnId, TxnId[]::new);
                byId[minByIdSearchIndex] = txn.update(missing);
            }
        }

        for (; minByIdSearchIndex < byId.length ; ++minByIdSearchIndex)
        {
            TxnInfo txn = byId[minByIdSearchIndex];
            if (txn == newInfo) continue;
            if (!txn.status.hasExecuteAtOrDeps) continue;
            if (!txn.kind().witnesses(insertTxnId)) continue;
            if (txn.status != ACCEPTED) continue;
            if (minDoNotInsertSearchIndex < doNotInsert.length && doNotInsert[minDoNotInsertSearchIndex].equals(txn))
            {
                ++minDoNotInsertSearchIndex;
                continue;
            }

            TxnId[] missing = txn.missing();
            if (missing == NO_TXNIDS) missing = oneMissing = ensureOneMissing(insertTxnId, oneMissing);
            else missing = SortedArrays.insert(missing, insertTxnId, TxnId[]::new);
            byId[minByIdSearchIndex] = txn.update(missing);
        }
    }

    /**
     * Take an index in {@code committedByExecuteAt}, find the companion entry in {@code byId}, and update both of them.
     * Return the updated minSearchIndex used for querying {@code byId} - this will be updated only if txnId==executeAt
     */
    @Inline
    static int updateInfoArraysByExecuteAt(int i, TxnInfo prevTxn, TxnInfo newTxn, int minSearchIndex, TxnInfo[] byId, TxnInfo[] committedByExecuteAt)
    {
        int j;
        if (prevTxn.executeAt == prevTxn)
        {
            j = SortedArrays.exponentialSearch(byId, minSearchIndex, byId.length, prevTxn, TxnInfo::compareTo, FAST);
            minSearchIndex = 1 + j;
        }
        else
        {
            j = Arrays.binarySearch(byId, prevTxn);
        }
        Invariants.checkState(byId[j] == prevTxn);
        byId[j] = committedByExecuteAt[i] = newTxn;
        return minSearchIndex;
    }

    static TxnId[] removePrunedAdditions(TxnId[] additions, int additionCount, TxnId prunedBefore)
    {
        if (additions[0].compareTo(prunedBefore) >= 0)
            return NO_TXNIDS;

        int prunedIndex = Arrays.binarySearch(additions, 1, additionCount, prunedBefore);
        if (prunedIndex < 0) prunedIndex = -1 - prunedIndex;
        if (prunedIndex == 0)
            return NO_TXNIDS;

        TxnId[] prunedIds = new TxnId[prunedIndex];
        System.arraycopy(additions, 0, prunedIds, 0, prunedIndex);
        System.arraycopy(additions, prunedIndex, additions, 0, additionCount - prunedIndex);
        return prunedIds;
    }

    /**
     * If a {@code missing} contains {@code removeTxnId}, return a new array without it (or NO_TXNIDS if the only entry)
     */
    static TxnId[] removeOneMissing(TxnId[] missing, TxnId removeTxnId)
    {
        if (missing == NO_TXNIDS) return NO_TXNIDS;

        int j = Arrays.binarySearch(missing, removeTxnId);
        if (j < 0) return missing;

        if (missing.length == 1)
            return NO_TXNIDS;

        int length = missing.length;
        TxnId[] newMissing = new TxnId[length - 1];
        System.arraycopy(missing, 0, newMissing, 0, j);
        System.arraycopy(missing, j + 1, newMissing, j, length - (1 + j));
        return newMissing;
    }

    static TxnId[] removeRedundantMissing(TxnId[] missing, TxnId removeBefore)
    {
        if (missing == NO_TXNIDS)
            return NO_TXNIDS;

        int j = Arrays.binarySearch(missing, removeBefore);
        if (j < 0) j = -1 - j;
        if (j <= 0) return missing;
        if (j == missing.length) return NO_TXNIDS;
        return Arrays.copyOfRange(missing, j, missing.length);
    }

    static TxnId[] ensureOneMissing(TxnId txnId, TxnId[] oneMissing)
    {
        return oneMissing != null ? oneMissing : new TxnId[] { txnId };
    }

    static TxnId[] insertMissing(TxnId[] additions, int additionCount, TxnId updateTxnId, int additionInsertPos)
    {
        TxnId[] result = new TxnId[additionCount + 1];
        System.arraycopy(additions, 0, result, 0, additionInsertPos);
        System.arraycopy(additions, additionInsertPos, result, additionInsertPos + 1, additionCount - additionInsertPos);
        result[additionInsertPos] = updateTxnId;
        return result;
    }

    static int missingTo(TxnId txnId, Timestamp depsKnownBefore, TxnId[] missingSource, int missingCount, int missingLimit)
    {
        if (depsKnownBefore == txnId) return missingCount;
        int to = Arrays.binarySearch(missingSource, 0, missingLimit, depsKnownBefore);
        if (to < 0) to = -1 - to;
        return to;
    }

    /**
     * Insert the contents of {@code additions} up to {@code additionCount} into {@code current}, ignoring {@code skipAddition} if not null.
     * Only insert entries that would be witnessed by {@code owner}.
     */
    static TxnId[] mergeAndFilterMissing(TxnId owner, TxnId[] current, TxnId[] additions, int additionCount, @Nullable TxnId skipAddition)
    {
        Txn.Kind.Kinds kinds = owner.kind().witnesses();
        int additionLength = additionCount;
        for (int i = additionCount - 1 ; i >= 0 ; --i)
        {
            if (!kinds.test(additions[i].kind()))
                --additionCount;
        }

        if (additionCount == (skipAddition == null ? 0 : 1))
            return current;

        TxnId[] buffer = cachedTxnIds().get(current.length + (skipAddition == null ? additionCount : additionCount - 1));
        int i = 0, j = 0, count = 0;
        while (i < additionLength && j < current.length)
        {
            if (kinds.test(additions[i].kind()))
            {
                int c = additions[i].compareTo(current[j]);
                if (c < 0)
                {
                    TxnId addition = additions[i++];
                    if (addition != skipAddition)
                        buffer[count++] = addition;
                }
                else
                {
                    buffer[count++] = current[j++];
                }
            }
            else i++;
        }
        while (i < additionLength)
        {
            if (kinds.test(additions[i].kind()))
            {
                TxnId addition = additions[i];
                if (addition != skipAddition)
                    buffer[count++] = addition;
            }
            i++;
        }
        while (j < current.length)
        {
            buffer[count++] = current[j++];
        }
        Invariants.checkState(count == additionCount + current.length);
        return cachedTxnIds().completeAndDiscard(buffer, count);
    }

    static TxnId[] selectUnmanaged(CommandsForKey.Unmanaged[] unmanageds, int start, int end)
    {
        TxnId[] notifyNotWaiting = new TxnId[end - start];
        for (int i = start ; i < end ; ++i)
        {
            CommandsForKey.Unmanaged unmanaged = unmanageds[i];
            TxnId txnId = unmanaged.txnId;
            notifyNotWaiting[i - start] = txnId;
        }
        return notifyNotWaiting;
    }

    static CommandsForKey.Unmanaged[] removeUnmanaged(CommandsForKey.Unmanaged[] unmanageds, int start, int end)
    {
        CommandsForKey.Unmanaged[] newUnmanageds = new CommandsForKey.Unmanaged[unmanageds.length - (end - start)];
        System.arraycopy(unmanageds, 0, newUnmanageds, 0, start);
        System.arraycopy(unmanageds, end, newUnmanageds, start, unmanageds.length - end);
        return newUnmanageds;
    }

    static int findCommit(CommandsForKey.Unmanaged[] unmanageds, Timestamp exclusive)
    {
        return -1 - SortedArrays.exponentialSearch(unmanageds, 0, unmanageds.length, exclusive, (f, v) -> {
            if (v.pending != COMMIT) return -1;
            return f.compareTo(v.waitingUntil) > 0 ? 1 : -1;
        }, FAST);
    }

    static int findFirstApply(CommandsForKey.Unmanaged[] unmanageds)
    {
        return -1 - SortedArrays.binarySearch(unmanageds, 0, unmanageds.length, null, (f, v) -> v.pending == COMMIT ? 1 : -1, FAST);
    }

    static int findApply(CommandsForKey.Unmanaged[] unmanageds, int start, Timestamp inclusive)
    {
        return -1 - SortedArrays.binarySearch(unmanageds, start, unmanageds.length, inclusive, (f, v) -> {
            return f.compareTo(v.waitingUntil) >= 0 ? 1 : -1;
        }, FAST);
    }
}
