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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.Status;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.Ballot;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.RelationMultiMap;
import accord.utils.SortedArrays;
import accord.utils.SortedCursor;

import static accord.local.KeyHistory.COMMANDS;
import static accord.local.cfk.CommandsForKey.InternalStatus.APPLIED;
import static accord.local.cfk.CommandsForKey.InternalStatus.COMMITTED;
import static accord.local.cfk.CommandsForKey.InternalStatus.HISTORICAL;
import static accord.local.cfk.CommandsForKey.InternalStatus.INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED;
import static accord.local.cfk.CommandsForKey.InternalStatus.TRANSITIVELY_KNOWN;
import static accord.local.cfk.CommandsForKey.managesExecution;
import static accord.local.cfk.CommandsForKey.Unmanaged.Pending.APPLY;
import static accord.local.cfk.CommandsForKey.Unmanaged.Pending.COMMIT;
import static accord.local.cfk.Pruning.loadPruned;
import static accord.local.cfk.Pruning.loadingPrunedFor;
import static accord.local.cfk.Utils.insertMissing;
import static accord.local.cfk.Utils.mergeAndFilterMissing;
import static accord.local.cfk.Utils.missingTo;
import static accord.local.cfk.Utils.removeOneMissing;
import static accord.local.cfk.Utils.removePrunedAdditions;
import static accord.local.cfk.Utils.validateMissing;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static accord.utils.Invariants.Paranoia.NONE;
import static accord.utils.Invariants.Paranoia.SUPERLINEAR;
import static accord.utils.Invariants.ParanoiaCostFactor.LOW;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.testParanoia;
import static accord.utils.SortedArrays.Search.FAST;

class Updating
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsForKey.class);

    /**
     * A TxnInfo to insert alongside any dependencies we did not already know about
     */
    static class InfoWithAdditions
    {
        final TxnInfo info;
        // a cachedTxnIds() array that should be returned once done
        final TxnId[] additions;
        final int additionCount;

        InfoWithAdditions(TxnInfo info, TxnId[] additions, int additionCount)
        {
            this.info = info;
            this.additions = additions;
            this.additionCount = additionCount;
        }
    }

    static CommandsForKeyUpdate insertOrUpdate(CommandsForKey cfk, int insertPos, int updatePos, TxnId plainTxnId, TxnInfo curInfo, CommandsForKey.InternalStatus newStatus, Command command)
    {
        // TODO (now): do not calculate any deps or additions if we're transitioning from Stable to Applied; wasted effort and might trigger LoadPruned
        Object newInfoObj = computeInfoAndAdditions(cfk, insertPos, updatePos, plainTxnId, newStatus, command);
        if (newInfoObj.getClass() != InfoWithAdditions.class)
            return insertOrUpdate(cfk, insertPos, plainTxnId, curInfo, (TxnInfo)newInfoObj, false, null);

        InfoWithAdditions newInfoWithAdditions = (InfoWithAdditions) newInfoObj;
        TxnId[] additions = newInfoWithAdditions.additions;
        int additionCount = newInfoWithAdditions.additionCount;
        TxnInfo newInfo = newInfoWithAdditions.info;

        TxnId[] prunedIds = removePrunedAdditions(additions, additionCount, cfk.prunedBefore());
        Object[] newLoadingPruned = cfk.loadingPruned;
        if (prunedIds != NO_TXNIDS)
        {
            additionCount -= prunedIds.length;
            newLoadingPruned = Pruning.loadPruned(cfk.loadingPruned, prunedIds, plainTxnId);
        }

        int committedByExecuteAtUpdatePos = committedByExecuteAtUpdatePos(cfk.committedByExecuteAt, curInfo, newInfo);
        TxnInfo[] newCommittedByExecuteAt = updateCommittedByExecuteAt(cfk, committedByExecuteAtUpdatePos, newInfo, false);
        int newMaxAppliedWriteByExecuteAt = updateMaxAppliedWriteByExecuteAt(cfk, committedByExecuteAtUpdatePos, newInfo, newCommittedByExecuteAt, false);

        TxnInfo[] byId = cfk.byId;
        TxnInfo[] newById = new TxnInfo[byId.length + additionCount + (updatePos < 0 ? 1 : 0)];
        insertOrUpdateWithAdditions(byId, insertPos, updatePos, plainTxnId, newInfo, additions, additionCount, newById, newCommittedByExecuteAt, NO_TXNIDS);
        if (testParanoia(SUPERLINEAR, NONE, LOW))
            validateMissing(newById, additions, additionCount, curInfo, newInfo, NO_TXNIDS);

        int newMinUndecidedById = cfk.minUndecidedById;
        {
            TxnInfo curMinUndecided = cfk.minUndecided();
            int additionsBeforeOldUndecided = additionCount;
            if (curMinUndecided != null && additionCount > 0)
            {
                additionsBeforeOldUndecided = Arrays.binarySearch(additions, 0, additionCount, curMinUndecided);
                if (additionsBeforeOldUndecided < 0)
                    additionsBeforeOldUndecided = -1 - additionsBeforeOldUndecided;
            }

            TxnId newMinUndecided = null;
            for (int i = 0 ; i < additionsBeforeOldUndecided ; ++i)
            {
                TxnId addition = additions[i];
                if (!managesExecution(addition) || cfk.isPreBootstrap(addition))
                    continue;

                newMinUndecided = addition;
                break;
            }

            if ((newMinUndecided == null || newMinUndecided.compareTo(newInfo) > 0) && newInfo.status.compareTo(COMMITTED) < 0 && managesExecution(newInfo) && !cfk.isPreBootstrap(newInfo))
                newMinUndecided = newInfo;

            if (newMinUndecided != null && curMinUndecided != null && curMinUndecided.compareTo(newMinUndecided) < 0)
                newMinUndecided = null;

            if (updatePos < 0 && newMinUndecided == null && curMinUndecided != null && newInfo.compareTo(curMinUndecided) < 0)
                ++additionsBeforeOldUndecided;

            if (newMinUndecided == null && curMinUndecided == curInfo)
                newMinUndecidedById = nextUndecided(newById, insertPos + 1, cfk);
            else if (newMinUndecided == null && newMinUndecidedById >= 0)
                newMinUndecidedById += additionsBeforeOldUndecided;
            else if (newMinUndecided == newInfo)
                newMinUndecidedById = insertPos + (-1 - Arrays.binarySearch(additions, 0, additionCount, newInfo));
            else if (newMinUndecided != null)
                newMinUndecidedById = Arrays.binarySearch(newById, 0, newById.length, newMinUndecided);
        }

        cachedTxnIds().forceDiscard(additions, additionCount + prunedIds.length);

        int newPrunedBeforeById = cfk.prunedBeforeById;
        if (curInfo == null && insertPos <= cfk.prunedBeforeById)
            ++newPrunedBeforeById;

        return PostProcess.LoadPruned.load(prunedIds, cfk.update(newById, newMinUndecidedById, newCommittedByExecuteAt, newMaxAppliedWriteByExecuteAt, newLoadingPruned, newPrunedBeforeById, curInfo, newInfo));
    }

    static Object computeInfoAndAdditions(CommandsForKey cfk, int insertPos, int updatePos, TxnId txnId, CommandsForKey.InternalStatus newStatus, Command command)
    {
        Invariants.checkState(newStatus.hasExecuteAtOrDeps);
        Timestamp executeAt = command.executeAt();
        if (executeAt.equals(txnId)) executeAt = txnId;
        Ballot ballot = Ballot.ZERO;
        if (newStatus.hasBallot)
            ballot = command.acceptedOrCommitted();

        Timestamp depsKnownBefore = newStatus.depsKnownBefore(txnId, executeAt);
        SortedCursor<TxnId> deps = command.partialDeps().txnIds(cfk.key());
        deps.find(cfk.redundantBefore());

        return Updating.computeInfoAndAdditions(cfk.byId, insertPos, updatePos, txnId, newStatus, ballot, executeAt, depsKnownBefore, deps);
    }

    /**
     * We return an Object here to avoid wasting allocations; most of the time we expect a new TxnInfo to be returned,
     * but if we have transitive dependencies to insert we return an InfoWithAdditions
     */
    static Object computeInfoAndAdditions(TxnInfo[] byId, int insertPos, int updatePos, TxnId plainTxnId, CommandsForKey.InternalStatus newStatus, Ballot ballot, Timestamp executeAt, Timestamp depsKnownBefore, SortedCursor<TxnId> deps)
    {
        TxnId[] additions = NO_TXNIDS, missing = NO_TXNIDS;
        int additionCount = 0, missingCount = 0;

        // the position until which we should have witnessed transactions, i.e. for computing the missing collection
        // *NOT* to be used for terminating *inserts* from deps parameter, as this may see the future
        // (due to pruning sometimes including a later transaction where it cannot include all earlier ones)
        int depsKnownBeforePos = insertPos;
        if (depsKnownBefore != plainTxnId)
        {
            depsKnownBeforePos = Arrays.binarySearch(byId, insertPos, byId.length, depsKnownBefore);
            Invariants.checkState(depsKnownBeforePos < 0);
            depsKnownBeforePos = -1 - depsKnownBeforePos;
        }

        int txnIdsIndex = 0;
        while (txnIdsIndex < byId.length && deps.hasCur())
        {
            TxnInfo t = byId[txnIdsIndex];
            TxnId d = deps.cur();
            int c = t.compareTo(d);
            if (c == 0)
            {
                ++txnIdsIndex;
                deps.advance();
            }
            else if (c < 0)
            {
                // we expect to be missing ourselves
                // we also permit any transaction we have recorded as COMMITTED or later to be missing, as recovery will not need to consult our information
                if (txnIdsIndex != updatePos && txnIdsIndex < depsKnownBeforePos && t.status.compareTo(COMMITTED) < 0 && plainTxnId.kind().witnesses(t))
                {
                    if (missingCount == missing.length)
                        missing = cachedTxnIds().resize(missing, missingCount, Math.max(8, missingCount * 2));
                    missing[missingCount++] = t.plainTxnId();
                }
                txnIdsIndex++;
            }
            else
            {
                if (plainTxnId.kind().witnesses(d))
                {
                    if (additionCount >= additions.length)
                        additions = cachedTxnIds().resize(additions, additionCount, Math.max(8, additionCount * 2));

                    additions[additionCount++] = d;
                }
                else
                {
                    // we can take dependencies on ExclusiveSyncPoints to represent a GC point in the log
                    // if we don't ordinarily witness a transaction it is meaningless to include it as a dependency
                    // as we will not logically be able to work with it (the missing collection will not correctly represent it anyway)
                    Invariants.checkState(d.kind() == ExclusiveSyncPoint);
                }
                deps.advance();
            }
        }

        if (deps.hasCur())
        {
            do
            {
                if (additionCount >= additions.length)
                    additions = cachedTxnIds().resize(additions, additionCount, Math.max(8, additionCount * 2));
                additions[additionCount++] = deps.cur();
                deps.advance();
            }
            while (deps.hasCur());
        }
        else if (txnIdsIndex < byId.length)
        {
            while (txnIdsIndex < depsKnownBeforePos)
            {
                if (txnIdsIndex != updatePos && byId[txnIdsIndex].status.compareTo(COMMITTED) < 0)
                {
                    TxnId txnId = byId[txnIdsIndex].plainTxnId();
                    if ((plainTxnId.kind().witnesses(txnId)))
                    {
                        if (missingCount == missing.length)
                            missing = cachedTxnIds().resize(missing, missingCount, Math.max(8, missingCount * 2));
                        missing[missingCount++] = txnId;
                    }
                }
                txnIdsIndex++;
            }
        }

        TxnInfo info = TxnInfo.create(plainTxnId, newStatus, executeAt, cachedTxnIds().completeAndDiscard(missing, missingCount), ballot);
        if (additionCount == 0)
            return info;

        return new InfoWithAdditions(info, additions, additionCount);
    }

    static CommandsForKeyUpdate insertOrUpdate(CommandsForKey cfk, int pos, TxnId plainTxnId, TxnInfo curInfo, TxnInfo newInfo, boolean wasPruned, @Nullable TxnId[] loadingAsPrunedFor)
    {
        if (curInfo == newInfo)
            return cfk;

        Object[] newLoadingPruned = cfk.loadingPruned;
        if (loadingAsPrunedFor != null) newLoadingPruned = Pruning.removeLoadingPruned(newLoadingPruned, plainTxnId);

        int committedByExecuteAtUpdatePos = committedByExecuteAtUpdatePos(cfk.committedByExecuteAt, curInfo, newInfo);
        TxnInfo[] newCommittedByExecuteAt = updateCommittedByExecuteAt(cfk, committedByExecuteAtUpdatePos, newInfo, wasPruned);
        int newMaxAppliedWriteByExecuteAt = updateMaxAppliedWriteByExecuteAt(cfk, committedByExecuteAtUpdatePos, newInfo, newCommittedByExecuteAt, wasPruned);

        int newMinUndecidedById = cfk.minUndecidedById;
        TxnInfo[] byId = cfk.byId;
        TxnInfo[] newById;
        if (curInfo == null)
        {
            newById = new TxnInfo[byId.length + 1];
            System.arraycopy(byId, 0, newById, 0, pos);
            newById[pos] = newInfo;
            System.arraycopy(byId, pos, newById, pos + 1, byId.length - pos);
            if (managesExecution(newInfo) && cfk.isPostBootstrap(newInfo))
            {
                if (newInfo.status.compareTo(COMMITTED) >= 0)
                {
                    if (newMinUndecidedById < 0 || pos <= newMinUndecidedById)
                    {
                        if (pos < newMinUndecidedById) ++newMinUndecidedById;
                        else newMinUndecidedById = nextUndecided(newById, pos + 1, cfk);
                    }
                }
                else
                {
                    if (newMinUndecidedById < 0 || pos < newMinUndecidedById)
                        newMinUndecidedById = pos;
                }
            }
            else if (pos <= newMinUndecidedById)
                ++newMinUndecidedById;
        }
        else
        {
            newById = byId.clone();
            newById[pos] = newInfo;
            if (pos == newMinUndecidedById && curInfo.status.compareTo(COMMITTED) < 0 && newInfo.status.compareTo(COMMITTED) >= 0)
                newMinUndecidedById = nextUndecided(newById, pos + 1, cfk);
        }

        if (loadingAsPrunedFor == null)
            loadingAsPrunedFor = NO_TXNIDS;

        if (newCommittedByExecuteAt.length > cfk.committedByExecuteAt.length)
        {
            Utils.removeFromMissingArrays(newById, newCommittedByExecuteAt, plainTxnId);
        }
        else if (curInfo != null && curInfo.status.compareTo(COMMITTED) < 0 && newInfo.status == INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED)
        {
            Utils.removeFromMissingArrays(newById, newCommittedByExecuteAt, plainTxnId);
        }
        else if (curInfo == null && newInfo.status != INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED)
        {
            // TODO (desired): for consistency, move this to insertOrUpdate (without additions), while maintaining the efficiency
            Utils.addToMissingArrays(newById, newCommittedByExecuteAt, newInfo, plainTxnId, loadingAsPrunedFor);
        }

        if (testParanoia(SUPERLINEAR, NONE, LOW) && curInfo == null && newInfo.status.compareTo(COMMITTED) < 0)
            validateMissing(newById, NO_TXNIDS, 0, curInfo, newInfo, loadingAsPrunedFor);

        int newPrunedBeforeById = cfk.prunedBeforeById;
        if (curInfo == null && pos <= cfk.prunedBeforeById)
            ++newPrunedBeforeById;

        return cfk.update(newById, newMinUndecidedById, newCommittedByExecuteAt, newMaxAppliedWriteByExecuteAt, newLoadingPruned, newPrunedBeforeById, curInfo, newInfo);
    }

    /**
     * Update newById to insert or update newInfo; insert any additions, and update any relevant missing arrays in
     * both newById and newCommittedByExecuteAt (for both deletions and insertions).
     */
    static void insertOrUpdateWithAdditions(TxnInfo[] byId, int sourceInsertPos, int sourceUpdatePos, TxnId plainTxnId, TxnInfo newInfo, TxnId[] additions, int additionCount, TxnInfo[] newById, TxnInfo[] newCommittedByExecuteAt, @Nonnull TxnId[] witnessedBy)
    {
        int additionInsertPos = Arrays.binarySearch(additions, 0, additionCount, plainTxnId);
        additionInsertPos = Invariants.checkArgument(-1 - additionInsertPos, additionInsertPos < 0);
        int targetInsertPos = sourceInsertPos + additionInsertPos;

        // we may need to insert or remove ourselves, depending on the new and prior status
        boolean insertSelfMissing = sourceUpdatePos < 0 && newInfo.status.compareTo(COMMITTED) < 0;
        boolean removeSelfMissing = sourceUpdatePos >= 0 && newInfo.status.compareTo(COMMITTED) >= 0 && byId[sourceUpdatePos].status.compareTo(COMMITTED) < 0;
        // missingSource starts as additions, but if we insertSelfMissing at the relevant moment it becomes the merge of additions and plainTxnId
        TxnId[] missingSource = additions;

        // the most recently constructed pure insert missing array, so that it may be reused if possible
        int i = 0, j = 0, missingCount = 0, missingLimit = additionCount, count = 0;
        int minByExecuteAtSearchIndex = 0;
        while (i < byId.length)
        {
            if (count == targetInsertPos)
            {
                newById[count] = newInfo;
                if (i == sourceUpdatePos) ++i;
                else if (insertSelfMissing) ++missingCount;
                ++count;
                continue;
            }

            int c = j == additionCount ? -1 : byId[i].compareTo(additions[j]);
            if (c < 0)
            {
                TxnInfo txn = byId[i];
                if (i == sourceUpdatePos)
                {
                    txn = newInfo;
                }
                else if (txn.status.hasDeps())
                {
                    Timestamp depsKnownBefore = txn.depsKnownBefore();
                    if (insertSelfMissing && missingSource == additions && (missingCount != j || (depsKnownBefore != txn && depsKnownBefore.compareTo(plainTxnId) > 0)))
                    {
                        // add ourselves to the missing collection
                        missingSource = insertMissing(additions, additionCount, plainTxnId, additionInsertPos);
                        ++missingLimit;
                    }

                    int to = missingTo(txn, depsKnownBefore, missingSource, missingCount, missingLimit);
                    if (to > 0 || removeSelfMissing)
                    {
                        TxnId[] curMissing = txn.missing();
                        TxnId[] newMissing = curMissing;
                        if (to > 0)
                        {
                            TxnId skipInsertMissing = null;
                            if (Arrays.binarySearch(witnessedBy, plainTxnId) >= 0)
                                skipInsertMissing = plainTxnId;

                            newMissing = mergeAndFilterMissing(txn, curMissing, missingSource, to, skipInsertMissing);
                        }

                        if (removeSelfMissing)
                            newMissing = removeOneMissing(newMissing, plainTxnId);

                        if (newMissing != curMissing)
                        {
                            TxnInfo newTxn = txn.update(newMissing);

                            if (txn.status.isCommitted())
                            {
                                // update newCommittedByExecuteAt
                                int ci;
                                if (txn.executeAt == txn)
                                {
                                    ci = SortedArrays.exponentialSearch(newCommittedByExecuteAt, minByExecuteAtSearchIndex, newCommittedByExecuteAt.length, txn, TxnInfo::compareExecuteAt, FAST);
                                    minByExecuteAtSearchIndex = 1 + ci;
                                }
                                else
                                {
                                    ci = Arrays.binarySearch(newCommittedByExecuteAt, minByExecuteAtSearchIndex, newCommittedByExecuteAt.length, txn, TxnInfo::compareExecuteAt);
                                }
                                Invariants.checkState(newCommittedByExecuteAt[ci] == txn);
                                newCommittedByExecuteAt[ci] = newTxn;
                            }

                            txn = newTxn;
                        }
                    }
                }
                newById[count] = txn;
                i++;
            }
            else if (c > 0)
            {
                TxnId txnId = additions[j++];
                newById[count] = TxnInfo.create(txnId, TRANSITIVELY_KNOWN, txnId, Ballot.ZERO);
                ++missingCount;
            }
            else
            {
                throw illegalState(byId[i] + " should be an insertion, but found match when merging with origin");
            }
            count++;
        }

        if (j < additionCount)
        {
            if (count <= targetInsertPos)
            {
                while (count < targetInsertPos)
                {
                    TxnId txnId = additions[j++];
                    newById[count++] = TxnInfo.create(txnId, TRANSITIVELY_KNOWN, txnId, Ballot.ZERO);
                }
                newById[targetInsertPos] = newInfo;
                count = targetInsertPos + 1;
            }
            while (j < additionCount)
            {
                TxnId txnId = additions[j++];
                newById[count++] = TxnInfo.create(txnId, TRANSITIVELY_KNOWN, txnId, Ballot.ZERO);
            }
        }
        else if (count == targetInsertPos)
        {
            newById[targetInsertPos] = newInfo;
        }
    }

    /**
     * Similar to insertOrUpdateWithAdditions, but when we only need to insert some additions (i.e. when calling registerUnmanaged)
     */
    static TxnInfo[] insertAdditionsOnly(TxnInfo[] byId, TxnInfo[] committedByExecuteAt, TxnInfo[] newInfos, TxnId[] additions, int additionCount)
    {
        // the most recently constructed pure insert missing array, so that it may be reused if possible
        int minByExecuteAtSearchIndex = 0;
        int i = 0, j = 0, missingCount = 0, count = 0;
        TxnInfo[] newCommittedByExecuteAt = null;
        while (i < byId.length)
        {
            int c = j == additionCount ? -1 : byId[i].compareTo(additions[j]);
            if (c < 0)
            {
                TxnInfo txn = byId[i];
                if (txn.status.hasDeps())
                {
                    Timestamp depsKnownBefore = txn.depsKnownBefore();

                    int to = missingTo(txn, depsKnownBefore, additions, missingCount, additionCount);
                    if (to > 0)
                    {
                        TxnId[] prevMissing = txn.missing();
                        TxnId[] newMissing = mergeAndFilterMissing(txn, prevMissing, additions, to, null);
                        if (newMissing != prevMissing)
                        {
                            TxnInfo newTxn = txn.update(newMissing);
                            if (txn.status.isCommitted())
                            {
                                if (newCommittedByExecuteAt == null)
                                    newCommittedByExecuteAt = committedByExecuteAt.clone();

                                int ci;
                                if (txn.executeAt == txn)
                                {
                                    ci = SortedArrays.exponentialSearch(newCommittedByExecuteAt, minByExecuteAtSearchIndex, newCommittedByExecuteAt.length, txn, TxnInfo::compareExecuteAt, FAST);
                                    minByExecuteAtSearchIndex = 1 + ci;
                                }
                                else
                                {
                                    ci = Arrays.binarySearch(newCommittedByExecuteAt, minByExecuteAtSearchIndex, newCommittedByExecuteAt.length, txn, TxnInfo::compareExecuteAt);
                                }
                                Invariants.checkState(newCommittedByExecuteAt[ci] == txn);
                                newCommittedByExecuteAt[ci] = newTxn;
                            }
                            txn = newTxn;
                        }
                    }
                }
                newInfos[count] = txn;
                i++;
            }
            else if (c > 0)
            {
                TxnId txnId = additions[j++];
                newInfos[count] = TxnInfo.create(txnId, TRANSITIVELY_KNOWN, txnId, Ballot.ZERO);
                ++missingCount;
            }
            else
            {
                throw illegalState(byId[i] + " should be an insertion, but found match when merging with origin");
            }
            count++;
        }

        while (j < additionCount)
        {
            TxnId txnId = additions[j++];
            newInfos[count++] = TxnInfo.create(txnId, TRANSITIVELY_KNOWN, txnId, Ballot.ZERO);
        }

        if (newCommittedByExecuteAt == null)
            newCommittedByExecuteAt = committedByExecuteAt;
        return newCommittedByExecuteAt;
    }

    static int committedByExecuteAtUpdatePos(TxnInfo[] committedByExecuteAt, @Nullable TxnInfo curInfo, TxnInfo newInfo)
    {
        if (newInfo.status.isCommitted())
        {
            return Arrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, newInfo, TxnInfo::compareExecuteAt);
        }
        else if (curInfo != null && curInfo.status.isCommitted() && newInfo.status == INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED)
        {
            return Arrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, curInfo, TxnInfo::compareExecuteAt);
        }
        return Integer.MAX_VALUE;
    }

    private static TxnInfo[] updateCommittedByExecuteAt(CommandsForKey cfk, int pos, TxnInfo newInfo, boolean wasPruned)
    {
        TxnInfo[] committedByExecuteAt = cfk.committedByExecuteAt;
        if (pos == Integer.MAX_VALUE)
            return committedByExecuteAt;

        if (newInfo.status != INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED)
        {
            if (pos >= 0 && committedByExecuteAt[pos].equals(newInfo))
            {
                TxnInfo[] result = committedByExecuteAt.clone();
                result[pos] = newInfo;
                return result;
            }
            else
            {
                if (pos >= 0) logger.error("Execution timestamp clash on key {}: {} and {} both have executeAt {}", cfk.key(), newInfo.plainTxnId(), committedByExecuteAt[pos].plainTxnId(), newInfo.executeAt);
                else pos = -1 - pos;

                TxnInfo[] result = new TxnInfo[committedByExecuteAt.length + 1];
                System.arraycopy(committedByExecuteAt, 0, result, 0, pos);
                result[pos] = newInfo;
                System.arraycopy(committedByExecuteAt, pos, result, pos + 1, committedByExecuteAt.length - pos);

                int maxAppliedWriteByExecuteAt = cfk.maxAppliedWriteByExecuteAt;
                if (pos <= maxAppliedWriteByExecuteAt)
                {
                    if (pos < maxAppliedWriteByExecuteAt && !wasPruned && cfk.isPostBootstrap(newInfo))
                    {
                        for (int i = pos; i <= maxAppliedWriteByExecuteAt; ++i)
                        {
                            if (committedByExecuteAt[pos].kind().witnesses(newInfo))
                                logger.error("Linearizability violation on key {}: {} is committed to execute (at {}) before {} that should witness it but has already applied (at {})", cfk.key, newInfo.plainTxnId(), newInfo.plainExecuteAt(), committedByExecuteAt[i].plainTxnId(), committedByExecuteAt[i].plainExecuteAt());
                        }
                    }
                }

                return result;
            }
        }
        else
        {
            // we can transition from COMMITTED, STABLE or APPLIED to INVALID_OR_TRUNCATED if the local command store ERASES a command
            // in this case, we not only need to update committedByExecuteAt, we also need to update any unmanaged transactions that
            // might have been waiting for this command's execution, to either wait for the preceding committed command or else to stop waiting
            TxnInfo[] newInfos = new TxnInfo[committedByExecuteAt.length - 1];
            System.arraycopy(committedByExecuteAt, 0, newInfos, 0, pos);
            System.arraycopy(committedByExecuteAt, pos + 1, newInfos, pos, newInfos.length - pos);
            return newInfos;
        }
    }

    private static int updateMaxAppliedWriteByExecuteAt(CommandsForKey cfk, int pos, TxnInfo newInfo, TxnInfo[] newCommittedByExecuteAt, boolean wasPruned)
    {
        int maxAppliedWriteByExecuteAt = cfk.maxAppliedWriteByExecuteAt;
        if (pos == Integer.MAX_VALUE) // this is a sentinel pos value returned by committedByExecuteAtUpdatePos to indicate no update
            return maxAppliedWriteByExecuteAt;

        boolean inserted = pos < 0;
        if (inserted) pos = -1 - pos;
        if (pos <= maxAppliedWriteByExecuteAt)
        {
            if (inserted)
            {
                return maxAppliedWriteByExecuteAt + 1;
            }
            else if (newInfo.status == INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED)
            {
                // deleted
                int newMaxAppliedWriteByExecuteAt = maxAppliedWriteByExecuteAt - 1;
                while (newMaxAppliedWriteByExecuteAt >= 0 && newCommittedByExecuteAt[newMaxAppliedWriteByExecuteAt].kind() != Write)
                    --newMaxAppliedWriteByExecuteAt;
                return newMaxAppliedWriteByExecuteAt;
            }
        }
        else if (newInfo.status == APPLIED || (cfk.isPreBootstrap(newInfo) && pos - 1 == maxAppliedWriteByExecuteAt))
        {
            return maybeAdvanceMaxAppliedAndCheckForLinearizabilityViolations(cfk, pos, newInfo.kind(), newInfo, wasPruned);
        }
        return maxAppliedWriteByExecuteAt;
    }

    private static int maybeAdvanceMaxAppliedAndCheckForLinearizabilityViolations(CommandsForKey cfk, int appliedPos, Txn.Kind appliedKind, TxnInfo applied, boolean wasPruned)
    {
        if (!wasPruned && cfk.isPostBootstrap(applied))
        {
            TxnInfo[] committedByExecuteAt = cfk.committedByExecuteAt;
            for (int i = cfk.maxAppliedWriteByExecuteAt + 1; i < appliedPos ; ++i)
            {
                if (committedByExecuteAt[i].status != APPLIED && appliedKind.witnesses(committedByExecuteAt[i]))
                    logger.error("Linearizability violation on key {}: {} is committed to execute (at {}) before {} that should witness it but has already applied (at {})", cfk.key, committedByExecuteAt[i].plainTxnId(), committedByExecuteAt[i].plainExecuteAt(), applied.plainTxnId(), applied.plainExecuteAt());
            }
        }

        return appliedKind == Txn.Kind.Write ? appliedPos : cfk.maxAppliedWriteByExecuteAt;
    }

    static int nextUndecided(TxnInfo[] infos, int pos, CommandsForKey cfk)
    {
        while (true)
        {
            if (pos == infos.length)
                return -1;

            if (infos[pos].status.compareTo(COMMITTED) < 0 && managesExecution(infos[pos]) && cfk.isPostBootstrap(infos[pos]))
                return pos;

            ++pos;
        }
    }

    static void updateUnmanagedAsync(CommandStore commandStore, TxnId txnId, Key key, NotifySink notifySink)
    {
        PreLoadContext context = PreLoadContext.contextFor(txnId, Keys.of(key), COMMANDS);
        commandStore.execute(context, safeStore -> {
            SafeCommandsForKey safeCommandsForKey = safeStore.get(key);
            CommandsForKey cur = safeCommandsForKey.current();
            CommandsForKeyUpdate next = Updating.updateUnmanaged(cur, safeStore.unsafeGet(txnId));
            if (cur != next)
            {
                if (cur != next.cfk())
                    safeCommandsForKey.set(next.cfk());

                PostProcess postProcess = next.postProcess();
                if (postProcess != null)
                    postProcess.postProcess(safeStore, key, notifySink);
            }
        }).begin(commandStore.agent());
    }

    static CommandsForKeyUpdate updateUnmanaged(CommandsForKey cfk, SafeCommand safeCommand)
    {
        return Updating.updateUnmanaged(cfk, safeCommand, false, null);
    }

    /**
     * Three modes of operation:
     *  - {@code register}: inserts any missing dependencies from safeCommand into the collection; may return CommandsForKeyUpdate
     *  - {@code !register, update == null}: fails if any dependencies are missing; always returns a CommandsForKey
     *  - {@code !register && update != null}: fails if any dependencies are missing; always returns the original CommandsForKey, and maybe adds a new Unmanaged to {@code update}
     */
    static CommandsForKeyUpdate updateUnmanaged(CommandsForKey cfk, SafeCommand safeCommand, boolean register, @Nullable List<CommandsForKey.Unmanaged> update)
    {
        Invariants.checkArgument(!register || update == null);
        if (safeCommand.current().hasBeen(Status.Truncated))
            return cfk;

        Command.Committed command = safeCommand.current().asCommitted();
        TxnId waitingTxnId = command.txnId();
        Timestamp waitingExecuteAt = command.executeAt();

        TxnInfo[] byId = cfk.byId;
        RelationMultiMap.SortedRelationList<TxnId> txnIds = command.partialDeps().keyDeps.txnIds(cfk.key());
        TxnId[] missing = NO_TXNIDS;
        int missingCount = 0;
        // we only populate dependencies to facilitate execution, not for any distributed decision,
        // so we can filter to only transactions we need to execute locally
        int i = txnIds.find(cfk.redundantOrBootstrappedBefore());
        if (i < 0) i = -1 - i;
        if (i < txnIds.size())
        {
            Txn.Kind waitingKind = waitingTxnId.kind();
            boolean readyToApply = true; // our dependencies have applied, so we are ready to apply
            boolean waitingToApply = true; // our dependencies have committed, so we know when we execute and are waiting
            Timestamp executesAt = null;
            int j = SortedArrays.binarySearch(byId, 0, byId.length, txnIds.get(i), Timestamp::compareTo, FAST);

            if (j < 0) j = -1 -j;
            while (i < txnIds.size())
            {
                int c = j == byId.length ? -1 : txnIds.get(i).compareTo(byId[j]);
                if (c == 0)
                {
                    TxnInfo txn = byId[j];
                    if (txn.status.compareTo(COMMITTED) < 0) waitingToApply = readyToApply = false;
                    else if (txn.status != INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED
                             && (txn.executeAt.compareTo(waitingExecuteAt) < 0
                                 || waitingKind == EphemeralRead
                                 || (waitingKind == ExclusiveSyncPoint && txn.compareTo(waitingTxnId) < 0)))
                    {
                        readyToApply &= txn.status == APPLIED;
                        executesAt = Timestamp.nonNullOrMax(executesAt, txn.executeAt);
                    }
                    ++i;
                    ++j;
                }
                else if (c > 0)
                {
                    if (waitingKind.isSyncPoint())
                    {
                        // we special-case sync points to handle pruning of transactions they should have witnessed as dependencies.
                        // We require all earlier TxnId to be decided before we can execute; we must be informed of all the necessary TxnId
                        // transitively by the "future" dependency provided for this purpose in mapReduceActive, so simply ensuring
                        // all earlier TxnId are committed is sufficient to compute the correct executeAt.
                        TxnInfo txn = byId[j];
                        if (managesExecution(txn))
                        {
                            if (txn.status.compareTo(COMMITTED) < 0) readyToApply = waitingToApply = false;
                            else if (txn.status != INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED
                                     && (txn.executeAt.compareTo(waitingExecuteAt) < 0
                                         || waitingKind == EphemeralRead
                                         || (waitingKind == ExclusiveSyncPoint && txn.compareTo(waitingTxnId) < 0)))
                            {
                                readyToApply &= txn.status == APPLIED;
                                executesAt = Timestamp.nonNullOrMax(executesAt, txn.executeAt);
                            }
                        }
                    }
                    ++j;
                }
                else if (!managesExecution(txnIds.get(i))) ++i;
                else if (register)
                {
                    readyToApply = waitingToApply = false;
                    if (missingCount == missing.length)
                        missing = cachedTxnIds().resize(missing, missingCount, Math.max(8, missingCount + missingCount/2));
                    missing[missingCount++] = txnIds.get(i++);
                }
                else
                {
                    Invariants.checkState(txnIds.get(i++).compareTo(cfk.prunedBefore()) < 0);
                }
            }

            if (waitingKind.isSyncPoint() && Pruning.isAnyPredecessorWaitingOnPruned(cfk.loadingPruned, waitingTxnId))
                readyToApply = waitingToApply = false;

            if (!readyToApply)
            {
                TxnInfo[] newById = byId, newCommittedByExecuteAt = cfk.committedByExecuteAt;
                int newMinUndecidedById = cfk.minUndecidedById;
                Object[] newLoadingPruned = cfk.loadingPruned;
                TxnId[] loadPruned = NO_TXNIDS;
                int clearMissingCount = missingCount;
                if (missingCount > 0)
                {
                    int prunedIndex = Arrays.binarySearch(missing, 0, missingCount, cfk.prunedBefore());
                    if (prunedIndex < 0) prunedIndex = -1 - prunedIndex;
                    if (prunedIndex > 0)
                    {
                        loadPruned = Arrays.copyOf(missing, prunedIndex);
                        newLoadingPruned = Pruning.loadPruned(cfk.loadingPruned, loadPruned, waitingTxnId);
                    }

                    if (prunedIndex != missingCount)
                    {
                        missingCount -= prunedIndex;
                        System.arraycopy(missing, prunedIndex, missing, 0, missingCount);
                        newById = new TxnInfo[byId.length + missingCount];
                        newCommittedByExecuteAt = insertAdditionsOnly(byId, cfk.committedByExecuteAt, newById, missing, missingCount);
                        // we can safely use missing[prunedIndex] here because we only fill missing with transactions for which we manage execution
                        newMinUndecidedById = Arrays.binarySearch(newById, TxnId.nonNullOrMin(missing[0], cfk.minUndecided()));
                    }
                }
                cachedTxnIds().discard(missing, clearMissingCount);

                CommandsForKey.Unmanaged newPendingRecord;
                if (waitingToApply)
                {
                    if (executesAt instanceof TxnInfo)
                        executesAt = ((TxnInfo) executesAt).plainExecuteAt();

                    if (waitingTxnId.kind().awaitsOnlyDeps() && executesAt != null)
                    {
                        if (executesAt.compareTo(command.waitingOn.executeAtLeast(Timestamp.NONE)) > 0)
                        {
                            Command.WaitingOn.Update waitingOn = new Command.WaitingOn.Update(command.waitingOn);
                            waitingOn.updateExecuteAtLeast(executesAt);
                            safeCommand.updateWaitingOn(waitingOn);
                        }
                    }

                    newPendingRecord = new CommandsForKey.Unmanaged(APPLY, command.txnId(), executesAt);
                }
                else newPendingRecord = new CommandsForKey.Unmanaged(COMMIT, command.txnId(), txnIds.get(txnIds.size() - 1));

                if (update != null)
                {
                    update.add(newPendingRecord);
                    return cfk;
                }

                CommandsForKey.Unmanaged[] newUnmanaged = SortedArrays.insert(cfk.unmanageds, newPendingRecord, CommandsForKey.Unmanaged[]::new);

                CommandsForKey result;
                if (newById == byId) result = new CommandsForKey(cfk, newLoadingPruned, newUnmanaged);
                else
                {
                    int prunedBeforeById = cfk.prunedBeforeById;
                    Invariants.checkState(prunedBeforeById < 0 || newById[prunedBeforeById].equals(cfk.prunedBefore()));
                    result = new CommandsForKey(cfk.key(), cfk.redundantBefore, cfk.bootstrappedAt, newById, newCommittedByExecuteAt, newMinUndecidedById, cfk.maxAppliedWriteByExecuteAt, newLoadingPruned, prunedBeforeById, cfk.safelyPrunedBefore, newUnmanaged);
                }

                if (loadPruned == NO_TXNIDS)
                    return result;

                return new CommandsForKeyUpdate.CommandsForKeyUpdateWithPostProcess(result, new PostProcess.LoadPruned(null, loadPruned));
            }
        }

        return new CommandsForKeyUpdate.CommandsForKeyUpdateWithPostProcess(cfk, new PostProcess.NotifyNotWaiting(null, new TxnId[] { safeCommand.txnId() }));
    }

    static CommandsForKeyUpdate registerHistorical(CommandsForKey cfk, TxnId txnId)
    {
        if (txnId.compareTo(cfk.redundantBefore()) < 0)
            return cfk;

        int i = Arrays.binarySearch(cfk.byId, txnId);
        if (i >= 0)
        {
            if (cfk.byId[i].status.compareTo(HISTORICAL) >= 0)
                return cfk;
            return cfk.update(i, txnId, cfk.byId[i], TxnInfo.create(txnId, HISTORICAL, txnId, Ballot.ZERO), false, null);
        }
        else if (txnId.compareTo(cfk.prunedBefore()) >= 0)
        {
            return cfk.insert(-1 - i, txnId, TxnInfo.create(txnId, HISTORICAL, txnId, Ballot.ZERO), false, null);
        }
        else if (txnId.compareTo(cfk.safelyPrunedBefore) < 0)
        {
            return cfk;
        }
        else
        {
            TxnId[] loadingPrunedFor = loadingPrunedFor(cfk.loadingPruned, txnId, null);
            if (loadingPrunedFor != null && Arrays.binarySearch(loadingPrunedFor, txnId) >= 0)
                return cfk;

            TxnId[] txnIdArray = new TxnId[] { txnId };
            Object[] newLoadingPruned = loadPruned(cfk.loadingPruned, txnIdArray, NO_TXNIDS);
            return PostProcess.LoadPruned.load(txnIdArray, cfk.update(newLoadingPruned));
        }
    }
}
