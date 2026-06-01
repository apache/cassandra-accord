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
import accord.local.cfk.CommandsForKey.Unmanaged;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import net.nicoulaj.compilecommand.annotations.Inline;

import static accord.local.cfk.CommandsForKey.InternalStatus.COMMITTED;
import static accord.local.cfk.CommandsForKey.Unmanaged.Pending.COMMIT;
import static accord.local.cfk.CommandsForKey.manages;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static accord.utils.SortedArrays.Search.FAST;

class Utils
{
    static void validateMissing(TxnInfo[] byId, TxnId[] additions, int additionCount, TxnInfo curInfo, TxnInfo newInfo, @Nonnull TxnId[] shouldNotHaveMissing)
    {
        int newInfoAdditionIndex = Arrays.binarySearch(additions, 0, additionCount, newInfo);
        for (TxnInfo txn : byId)
        {
            if (txn == newInfo) continue;
            if (!txn.hasDeps()) continue;
            int additionIndex = Arrays.binarySearch(additions, 0, additionCount, txn.depsKnownBefore());
            if (additionIndex < 0) additionIndex = -1 - additionIndex;
            TxnId[] missing = txn.missing();
            int j = 0;
            for (int i = 0 ; i < additionIndex ; ++i)
            {
                if (!txn.witnesses(additions[i])) continue;
                j = SortedArrays.exponentialSearch(missing, j, missing.length, additions[i]);
                if (shouldNotHaveMissing != NO_TXNIDS && i == newInfoAdditionIndex && Arrays.binarySearch(shouldNotHaveMissing, txn) >= 0) Invariants.require(j < 0);
                else Invariants.require(j >= 0);
            }
            if (curInfo == null && newInfo.compareTo(COMMITTED) < 0 && txn.witnesses(newInfo) && txn.depsKnownBefore().compareTo(newInfo) > 0 && (shouldNotHaveMissing == NO_TXNIDS || Arrays.binarySearch(shouldNotHaveMissing, txn) < 0))
                Invariants.require(Arrays.binarySearch(missing, newInfo) >= 0);
        }
    }

    /**
     * {@code removeTxnId} no longer needs to be tracked in missing arrays;
     * remove it from byId and committedByExecuteAt, ensuring both arrays still reference the same TxnInfo where updated.
     */
    static TxnInfo[] removeFromMissingArrays(TxnInfo[] byId, TxnInfo[] newCommittedByExecuteAt, TxnId removeTxnId, @Nullable TxnInfo[] committedByExecuteAt)
    {
        int startIndex = SortedArrays.binarySearch(newCommittedByExecuteAt, 0, newCommittedByExecuteAt.length, removeTxnId, (id, info) -> id.compareTo(info.executeAt), FAST);
        if (startIndex < 0) startIndex = -1 - startIndex;
        else ++startIndex;

        int minSearchIndex = Arrays.binarySearch(byId, removeTxnId) + 1;
        removeFromMissingArraysById(byId, 0, minSearchIndex, removeTxnId);
        for (int i = startIndex ; i < newCommittedByExecuteAt.length ; ++i)
        {
            int newMinSearchIndex;
            {
                TxnInfo txn = newCommittedByExecuteAt[i];
                if (txn.getClass() == TxnInfo.class) continue;
                if (!txn.witnesses(removeTxnId)) continue;

                TxnId[] missing = txn.missing();
                TxnId[] newMissing = removeOneMissing(missing, removeTxnId);
                if (missing == newMissing) continue;

                newCommittedByExecuteAt = copyIfShared(newCommittedByExecuteAt, committedByExecuteAt);
                newMinSearchIndex = updateInfoArraysByExecuteAt(i, txn, txn.withMissing(newMissing), minSearchIndex, byId, newCommittedByExecuteAt);
            }

            minSearchIndex = removeFromMissingArraysById(byId, minSearchIndex, newMinSearchIndex, removeTxnId);
        }

        removeFromMissingArraysById(byId, minSearchIndex, byId.length, removeTxnId);
        return newCommittedByExecuteAt;
    }

    /**
     * {@code removeTxnId} no longer needs to be tracked in missing arrays;
     * remove it from byId and committedByExecuteAt, ensuring both arrays still reference the same TxnInfo where updated
     */
    static TxnInfo[] removeFromWitnessMissingArrays(TxnInfo[] byId, TxnInfo[] newCommittedByExecuteAt, TxnId removeTxnId, TxnId[] witnessedBy, @Nullable TxnInfo[] committedByExecuteAt)
    {
        if (witnessedBy.length == 0)
            return newCommittedByExecuteAt;

        int byIdIndex = Arrays.binarySearch(byId, witnessedBy[0]);
        if (byIdIndex < 0)
            byIdIndex = -1 - byIdIndex;

        for (TxnId txnId : witnessedBy)
        {
            byIdIndex = SortedArrays.exponentialSearch(byId, byIdIndex, byId.length, txnId);
            if (byIdIndex < 0)
            {
                byIdIndex = -1 - byIdIndex;
                continue;
            }
            TxnInfo curTxn = byId[byIdIndex];
            TxnId[] curMissing = curTxn.missing();
            if (curMissing == NO_TXNIDS) continue;
            TxnId[] newMissing = removeOneMissing(curMissing, removeTxnId);
            if (newMissing == curMissing) continue;
            TxnInfo newTxn = curTxn.withMissing(newMissing);
            byId[byIdIndex] = newTxn;
            if (!curTxn.isCommittedAndExecutes()) continue;
            int byExecuteAtIndex = Arrays.binarySearch(newCommittedByExecuteAt, curTxn, TxnInfo::compareExecuteAt);
            newCommittedByExecuteAt = copyIfShared(newCommittedByExecuteAt, committedByExecuteAt);
            newCommittedByExecuteAt[byExecuteAtIndex] = newTxn;
        }
        return newCommittedByExecuteAt;
    }

    /**
     * {@code removeTxnId} no longer needs to be tracked in missing arrays;
     * remove it from a range of byId ACCEPTED status entries only, that could not be tracked via committedByExecuteAt
     */
    static int removeFromMissingArraysById(TxnInfo[] byId, int from, int to, TxnId removeTxnId)
    {
        for (int i = from ; i < to ; ++i)
        {
            // TODO (expected): optimise with flag bits
            TxnInfo txn = byId[i];
            if (txn.getClass() == TxnInfo.class) continue;
            if (!txn.hasDeps()) continue;
            if (!txn.witnesses(removeTxnId)) continue;
            if (txn.isCommittedAndExecutes()) continue;

            TxnId[] missing = txn.missing();
            TxnId[] newMissing = removeOneMissing(missing, removeTxnId);
            if (missing == newMissing) continue;
            byId[i] = txn.withMissing(newMissing);
        }
        return to;
    }

    /**
     * {@code insertTxnId} needs to be tracked in missing arrays;
     * add it to byId and committedByExecuteAt, ensuring both arrays still reference the same TxnInfo where updated
     * Do not insert it into any members of {@code doNotInsert} as these are known to have witnessed {@code insertTxnId}
     */
    static TxnInfo[] addToMissingArrays(TxnInfo[] byId, TxnInfo[] newCommittedByExecuteAt, TxnInfo newInfo, TxnId insertTxnId, @Nonnull TxnId[] doNotInsert, @Nullable TxnInfo[] committedByExecuteAt)
    {
        TxnId[] oneMissing = null;

        int startIndex = SortedArrays.binarySearch(newCommittedByExecuteAt, 0, newCommittedByExecuteAt.length, insertTxnId, (id, info) -> id.compareTo(info.executeAt), FAST);
        if (startIndex < 0) startIndex = -1 - startIndex;
        else ++startIndex;

        int minByIdSearchIndex = Arrays.binarySearch(byId, insertTxnId) + 1;
        for (int i = 0 ; i < minByIdSearchIndex ; ++i)
        {
            TxnInfo txn = byId[i];
            if (txn == newInfo) continue;
            if (!txn.hasDeps()) continue;
            if (txn.isCommittedAndExecutes()) continue;
            if (!txn.witnesses(insertTxnId)) continue;
            if (txn.executeAt == txn) continue;
            if (txn.depsKnownBefore().compareTo(newInfo) < 0) continue;

            TxnId[] missing = txn.missing();
            if (missing == NO_TXNIDS) missing = oneMissing = ensureOneMissing(insertTxnId, oneMissing);
            else missing = SortedArrays.insert(missing, insertTxnId, TxnId[]::new);
            byId[i] = txn.withMissing(missing);
        }

        int minDoNotInsertSearchIndex = 0;
        for (int i = startIndex ; i < newCommittedByExecuteAt.length ; ++i)
        {
            int newMinSearchIndex;
            {
                TxnInfo txn = newCommittedByExecuteAt[i];
                if (txn == newInfo) continue;
                if (!txn.witnesses(insertTxnId)) continue;

                if (doNotInsert != NO_TXNIDS)
                {
                    if (txn.executeAt == txn)
                    {
                        minDoNotInsertSearchIndex = linearScan(doNotInsert, minDoNotInsertSearchIndex, txn);
                        if (minDoNotInsertSearchIndex >= 0) continue;
                        minDoNotInsertSearchIndex = -1 - minDoNotInsertSearchIndex;
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

                newCommittedByExecuteAt = copyIfShared(newCommittedByExecuteAt, committedByExecuteAt);
                newMinSearchIndex = updateInfoArraysByExecuteAt(i, txn, txn.withMissing(missing), minByIdSearchIndex, byId, newCommittedByExecuteAt);
            }

            if (newMinSearchIndex == minByIdSearchIndex) continue;
            for (; minByIdSearchIndex < newMinSearchIndex ; ++minByIdSearchIndex)
            {
                TxnInfo txn = byId[minByIdSearchIndex];
                if (txn == newInfo) continue;
                // TODO (expected): we can perform these three checks in parallel with bit masks
                if (!txn.hasDeps()) continue;
                if (txn.isCommittedAndExecutes()) continue;
                if (!txn.witnesses(insertTxnId)) continue;
                if (doNotInsert != NO_TXNIDS)
                {
                    minDoNotInsertSearchIndex = linearScan(doNotInsert, minDoNotInsertSearchIndex, txn);
                    if (minDoNotInsertSearchIndex >= 0) continue;
                    minDoNotInsertSearchIndex = -1 - minDoNotInsertSearchIndex;
                }

                TxnId[] missing = txn.missing();
                if (missing == NO_TXNIDS) missing = oneMissing = ensureOneMissing(insertTxnId, oneMissing);
                else missing = SortedArrays.insert(missing, insertTxnId, TxnId[]::new);
                byId[minByIdSearchIndex] = txn.withMissing(missing);
            }
        }

        for (; minByIdSearchIndex < byId.length ; ++minByIdSearchIndex)
        {
            TxnInfo txn = byId[minByIdSearchIndex];
            if (txn == newInfo) continue;
            if (!txn.hasDeps()) continue;
            if (!txn.witnesses(insertTxnId)) continue;
            if (txn.isCommittedAndExecutes()) continue;
            if (doNotInsert != NO_TXNIDS)
            {
                minDoNotInsertSearchIndex = linearScan(doNotInsert, minDoNotInsertSearchIndex, txn);
                if (minDoNotInsertSearchIndex >= 0) continue;
                minDoNotInsertSearchIndex = -1 - minDoNotInsertSearchIndex;
            }

            TxnId[] missing = txn.missing();
            if (missing == NO_TXNIDS) missing = oneMissing = ensureOneMissing(insertTxnId, oneMissing);
            else missing = SortedArrays.insert(missing, insertTxnId, TxnId[]::new);
            byId[minByIdSearchIndex] = txn.withMissing(missing);
        }
        return newCommittedByExecuteAt;
    }

    @Inline
    static TxnInfo[] copyIfShared(TxnInfo[] copy, @Nullable TxnInfo[] shared)
    {
        return copy == shared ? copy.clone() : copy;
    }

    private static int linearScan(TxnId[] array, int from, TxnId find)
    {
        while (from < array.length)
        {
            int c = find.compareTo(array[from]);
            if (c > 0) from++;
            else if (c == 0) return from;
            else break;
        }
        return -1 - from;
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
            j = SortedArrays.<TxnInfo, TxnInfo>exponentialSearch(byId, minSearchIndex, byId.length, prevTxn, TxnInfo::compareTo, FAST);
            minSearchIndex = 1 + j;
        }
        else
        {
            j = Arrays.binarySearch(byId, prevTxn);
        }
        Invariants.require(byId[j] == prevTxn);
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

    static TxnId[] removeUnmanaged(TxnId[] ids)
    {
        int count = 0;
        for (TxnId id : ids)
            count += manages(id) ? 1 : 0;

        if (count == ids.length)
            return ids;

        if (count == 0)
            return NO_TXNIDS;

        TxnId[] result = new TxnId[count];
        count = 0;
        for (TxnId id : ids)
        {
            if (manages(id))
                result[count++] = id;
        }
        return result;
    }

    static void removeNonIdentityFlags(TxnId[] ids, int count)
    {
        while (--count >= 0)
        {
            TxnId cur = ids[count];
            TxnId upd = cur.withoutNonIdentityFlags();
            if (upd != cur)
                ids[count] = upd;
        }
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

    static TxnId[] removeRedundantMissing(TxnId[] missing, TxnId removeBefore, TxnId appliedBefore, int appliedBeforeIndex, TxnInfo[] newById)
    {
        if (missing == NO_TXNIDS)
            return NO_TXNIDS;

        int j = Arrays.binarySearch(missing, removeBefore);
        if (j < 0) j = -1 - j;
        if (j > 0)
        {
            if (j == missing.length) return NO_TXNIDS;
            missing = Arrays.copyOfRange(missing, j, missing.length);
        }

        if (appliedBeforeIndex < 0)
            return missing;

        int maybeRemovedBefore = Arrays.binarySearch(missing, appliedBefore);
        if (maybeRemovedBefore < 0) maybeRemovedBefore = -1 - maybeRemovedBefore;
        if (maybeRemovedBefore == 0)
            return missing;

        int removed = 0;
        int i = 0;
        j = SortedArrays.binarySearch(newById, 0, appliedBeforeIndex, missing[0], TxnId::compareTo, FAST);
        while (true)
        {
            if (j < 0)
            {
                ++removed;
                j = -1 - j;
            }
            else if (removed > 0)
            {
                missing[i - removed] = missing[i];
            }
            if (++i == maybeRemovedBefore)
                break;
            j = SortedArrays.exponentialSearch(newById, j, appliedBeforeIndex, missing[i], TxnId::compareTo, FAST);
        }

        if (removed == 0) return missing;
        else if (removed == missing.length) return NO_TXNIDS;
        else if (i == missing.length) return Arrays.copyOf(missing, missing.length - removed);
        else
        {
            TxnId[] newMissing = new TxnId[missing.length - removed];
            System.arraycopy(missing, 0, newMissing, 0, i - removed);
            System.arraycopy(missing, i, newMissing, i - removed, missing.length - i);
            return newMissing;
        }
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

    static int missingTo(TxnId txnId, Timestamp depsKnownBefore, TxnId[] missingSource, int missingCount, int missingPosition, int missingLimit)
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
        Txn.Kind.Kinds kinds = owner.witnesses();
        int additionLength = additionCount;
        for (int i = additionCount - 1 ; i >= 0 ; --i)
        {
            if (!kinds.test(additions[i]))
                --additionCount;
        }

        if (additionCount == (skipAddition == null ? 0 : 1))
            return current;

        TxnId[] buffer = cachedTxnIds().get(current.length + (skipAddition == null ? additionCount : additionCount - 1));
        int i = 0, j = 0, count = 0;
        while (i < additionLength && j < current.length)
        {
            if (kinds.test(additions[i]))
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
            if (kinds.test(additions[i]))
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
        Invariants.require(count == additionCount + current.length);
        return cachedTxnIds().completeAndDiscard(buffer, count);
    }

    static TxnId[] selectUnmanaged(Unmanaged[] unmanageds, int start, int end)
    {
        Invariants.requireArgument(end >= start);
        TxnId[] notifyNotWaiting = new TxnId[end - start];
        for (int i = start ; i < end ; ++i)
        {
            Unmanaged unmanaged = unmanageds[i];
            TxnId txnId = unmanaged.txnId;
            notifyNotWaiting[i - start] = txnId;
        }
        return notifyNotWaiting;
    }

    static Unmanaged[] removeUnmanaged(Unmanaged[] unmanageds, int start, int end)
    {
        Unmanaged[] newUnmanageds = new Unmanaged[unmanageds.length - (end - start)];
        System.arraycopy(unmanageds, 0, newUnmanageds, 0, start);
        System.arraycopy(unmanageds, end, newUnmanageds, start, unmanageds.length - end);
        return newUnmanageds;
    }

    static int findCommit(Unmanaged[] unmanageds, Timestamp exclusive)
    {
        return -1 - SortedArrays.<Unmanaged, Timestamp>exponentialSearch(unmanageds, 0, unmanageds.length, exclusive, (f, v) -> {
            if (v.pending != COMMIT) return -1;
            return f.compareTo(v.waitingUntil) > 0 ? 1 : -1;
        }, FAST);
    }

    static int findFirstApply(Unmanaged[] unmanageds)
    {
        return -1 - SortedArrays.<Unmanaged, Object>binarySearch(unmanageds, 0, unmanageds.length, null, (f, v) -> v.pending == COMMIT ? 1 : -1, FAST);
    }

    static int findApply(Unmanaged[] unmanageds, int start, Timestamp inclusive)
    {
        return -1 - SortedArrays.<Unmanaged, Timestamp>binarySearch(unmanageds, start, unmanageds.length, inclusive, (f, v) -> {
            return f.compareTo(v.waitingUntil) >= 0 ? 1 : -1;
        }, FAST);
    }
}
