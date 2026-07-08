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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.ExecutionContext;
import accord.local.RedundantBefore.QuickBounds;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey.InternalStatus;
import accord.local.cfk.CommandsForKeyUpdate.CommandsForKeyUpdateWithPostProcess;
import accord.local.cfk.PostProcess.LoadPruned;
import accord.local.cfk.PostProcess.NotifyNotWaiting;
import accord.primitives.Deps.DepList;
import accord.primitives.Status;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.Ballot;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers.ObjectBuffers;
import accord.utils.Invariants;
import accord.utils.RelationMultiMap;
import accord.utils.SortedArrays;
import accord.utils.SortedList.MergeCursor;

import static accord.api.ProtocolModifiers.isTransitiveDependencyVisible;
import static accord.local.CommandSummaries.SummaryStatus.APPLIED;
import static accord.local.cfk.CommandsForKey.InternalStatus.COMMITTED;
import static accord.local.cfk.CommandsForKey.InternalStatus.INVALIDATED;
import static accord.local.cfk.CommandsForKey.InternalStatus.PRUNED;
import static accord.local.cfk.CommandsForKey.InternalStatus.TRANSITIVE;
import static accord.local.cfk.CommandsForKey.InternalStatus.TRANSITIVE_VISIBLE;
import static accord.local.cfk.CommandsForKey.NOT_LOADING_PRUNED;
import static accord.local.cfk.CommandsForKey.NO_INFOS;
import static accord.local.cfk.CommandsForKey.Unmanaged.Pending.APPLY;
import static accord.local.cfk.CommandsForKey.Unmanaged.Pending.COMMIT;
import static accord.local.cfk.CommandsForKey.executesIgnoringBootstrap;
import static accord.local.cfk.CommandsForKey.manages;
import static accord.local.cfk.CommandsForKey.reportLinearizabilityViolation;
import static accord.local.cfk.CommandsForKey.mayExecute;
import static accord.local.cfk.Pruning.removeLoadingPruned;
import static accord.local.cfk.UpdateUnmanagedMode.REGISTER;
import static accord.local.cfk.UpdateUnmanagedMode.REGISTER_DEPS_ONLY;
import static accord.local.cfk.UpdateUnmanagedMode.UPDATE;
import static accord.local.cfk.Utils.insertMissing;
import static accord.local.cfk.Utils.mergeAndFilterMissing;
import static accord.local.cfk.Utils.missingTo;
import static accord.local.cfk.Utils.removeNonIdentityFlags;
import static accord.local.cfk.Utils.removeOneMissing;
import static accord.local.cfk.Utils.removePrunedAdditions;
import static accord.local.cfk.Utils.removeUnmanaged;
import static accord.local.cfk.Utils.validateMissing;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.Timestamp.Flag.UNSTABLE;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static accord.utils.Invariants.Paranoia.CONSTANT;
import static accord.utils.Invariants.Paranoia.LINEAR;
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

    static CommandsForKeyUpdate insertOrUpdate(CommandsForKey cfk, int insertPos, int updatePos, TxnId plainTxnId, TxnInfo curInfo, InternalStatus newStatus, boolean mayExecute, Command command, @Nonnull TxnId[] witnessedBy)
    {
        Object newInfoObj = computeInfoAndAdditions(cfk, insertPos, updatePos, plainTxnId, newStatus, mayExecute, command);
        if (newInfoObj.getClass() != InfoWithAdditions.class)
            return insertOrUpdate(cfk, insertPos, plainTxnId, curInfo, (TxnInfo)newInfoObj, command, witnessedBy);

        InfoWithAdditions newInfoWithAdditions = (InfoWithAdditions) newInfoObj;
        TxnId[] additions = newInfoWithAdditions.additions;
        int additionCount = newInfoWithAdditions.additionCount;
        int additionAndPrunedCount = additionCount;
        TxnInfo newInfo = newInfoWithAdditions.info;

        TxnId[] prunedIds = removePrunedAdditions(additions, additionCount, cfk.prunedBefore());
        Object[] newLoadingPruned = cfk.loadingPruned;
        if (witnessedBy != NOT_LOADING_PRUNED)
            newLoadingPruned = removeLoadingPruned(newLoadingPruned, plainTxnId);

        if (prunedIds != NO_TXNIDS)
        {
            additionCount -= prunedIds.length;
            prunedIds = removeUnmanaged(prunedIds);
            if (prunedIds != NO_TXNIDS)
            {
                List<TxnId> insertLoadPruned = new ArrayList<>();
                newLoadingPruned = Pruning.loadPruned(newLoadingPruned, prunedIds, plainTxnId, insertLoadPruned);
                prunedIds = insertLoadPruned.isEmpty() ? NO_TXNIDS : insertLoadPruned.toArray(TxnId[]::new);
            }
        }
        removeNonIdentityFlags(additions, additionCount);

        int committedByExecuteAtUpdatePos = committedByExecuteAtUpdatePos(cfk.committedByExecuteAt, curInfo, newInfo);
        TxnInfo[] newCommittedByExecuteAt = updateCommittedByExecuteAt(cfk, committedByExecuteAtUpdatePos, newInfo);
        int newMaxAppliedWriteByExecuteAt = updateMaxAppliedWriteByExecuteAt(cfk, committedByExecuteAtUpdatePos, newInfo, newCommittedByExecuteAt);

        TxnInfo[] byId = cfk.byId;
        TxnInfo[] newById = new TxnInfo[byId.length + additionCount + (updatePos < 0 ? 1 : 0)];
        insertOrUpdateWithAdditions(byId, insertPos, updatePos, plainTxnId, newInfo, additions, additionCount, newById, newCommittedByExecuteAt, witnessedBy, cfk.bounds);
        if (testParanoia(SUPERLINEAR, NONE, LOW))
            validateMissing(newById, additions, additionCount, curInfo, newInfo, witnessedBy);

        int newMinUndecidedManagedById = updateMinUndecidedById(true, newInfo, insertPos, updatePos, cfk, byId, newById, additions, additionCount);
        int newMinUndecidedById = updateMinUndecidedById(false, newInfo, insertPos, updatePos, cfk, byId, newById, additions, additionCount);
        // we don't insert anything before prunedBeforeById (we LoadPruned instead), so we can simply bump it by 0 or 1
        int newPrunedBeforeById = advanceByIdIndex(cfk.prunedBeforeById, insertPos, updatePos);
        Invariants.paranoid(cfk.prunedBeforeById < 0 || newById[newPrunedBeforeById].equals(byId[cfk.prunedBeforeById]));
        int newMaxAppliedUnreadyWriteById = updateMaxAppliedUnreadyWriteById(newInfo, insertPos, curInfo == null, cfk, byId, additions, additionCount);
        long newMaxHlc = updateMaxUniqueHlc(cfk, newInfo, command);

        cachedTxnIds().forceDiscard(additions, additionAndPrunedCount);
        return LoadPruned.load(prunedIds, cfk.update(newById, newMinUndecidedManagedById, newMinUndecidedById, newMaxAppliedUnreadyWriteById, newCommittedByExecuteAt, newMaxAppliedWriteByExecuteAt, newMaxHlc, newLoadingPruned, newPrunedBeforeById, curInfo, newInfo));
    }

    static int advanceByIdIndex(int curIndex, TxnInfo[] byId, int insertPos, int updatePos, TxnId[] additions, int additionCount)
    {
        return advanceByIdIndex(curIndex, byId, insertPos, updatePos < 0, additions, additionCount);
    }

    static int advanceByIdIndex(int curIndex, TxnInfo[] byId, int insertPos, boolean inserted, TxnId[] additions, int additionCount)
    {
        if (curIndex < 0)
            return curIndex;

        int insertBeforeCurIndex = ((curIndex >= insertPos) & inserted) ? 1 : 0;
        if (additionCount == 0)
            return curIndex + insertBeforeCurIndex;

        TxnInfo cur = byId[curIndex];
        return insertBeforeCurIndex + advanceByIdIndexByAdditionsOnly(curIndex, cur, additions, additionCount);
    }

    static int advanceByIdIndexByAdditionsOnly(int curIndex, TxnInfo cur, TxnId[] additions, int additionCount)
    {
        if (additionCount == 0)
            return curIndex;

        int additionsBeforeCurIndex = Arrays.binarySearch(additions, 0, additionCount, cur);
        if (additionsBeforeCurIndex < 0)
            additionsBeforeCurIndex = -1 - additionsBeforeCurIndex;

        return curIndex + additionsBeforeCurIndex;
    }

    static int advanceByIdIndex(int curIndex, int insertPos, int updatePos)
    {
        return advanceByIdIndex(curIndex, insertPos, updatePos < 0);
    }

    static int advanceByIdIndex(int curIndex, int pos, boolean inserted)
    {
        return curIndex + ((inserted & (curIndex >= pos)) ? 1 : 0);
    }

    // TODO (expected): this method is a mess; clean it up.
    static int updateMinUndecidedById(boolean managed, TxnInfo newInfo, int insertPos, int updatePos, CommandsForKey cfk, TxnInfo[] byId, TxnInfo[] newById, TxnId[] additions, int additionCount)
    {
        int curIndex = managed ? cfk.minUndecidedManagedById : cfk.minUndecidedUnmanagedById;
        int additionsBeforeOldUndecided = additionCount;
        TxnInfo cur = null;
        if (curIndex >= 0)
        {
            cur = byId[curIndex];
            if (additionCount > 0)
            {
                additionsBeforeOldUndecided = Arrays.binarySearch(additions, 0, additionCount, cur);
                if (additionsBeforeOldUndecided < 0)
                    additionsBeforeOldUndecided = -1 - additionsBeforeOldUndecided;
            }
        }

        TxnId min = null;
        for (int i = 0 ; i < additionsBeforeOldUndecided ; ++i)
        {
            TxnId addition = additions[i];
            if (managed != cfk.mayExecute(addition) || cfk.isUnready(addition))
                continue;

            min = addition;
            break;
        }

        if ((min == null || min.compareTo(newInfo) > 0) && newInfo.compareTo(COMMITTED) < 0 && (managed == cfk.mayExecute(newInfo)) && !cfk.isUnready(newInfo))
            min = newInfo;

        if (min != null && cur != null && cur.compareTo(min) < 0)
            min = null;

        if (updatePos < 0 && min == null && cur != null && newInfo.compareTo(cur) < 0)
            ++additionsBeforeOldUndecided;

        int newIndex = curIndex;
        if (min == null && insertPos == newIndex)
            newIndex = nextUndecided(managed, newById, insertPos + 1, cfk);
        else if (min == null && newIndex >= 0)
            newIndex += additionsBeforeOldUndecided;
        else if (min == newInfo)
            newIndex = insertPos + (-1 - Arrays.binarySearch(additions, 0, additionCount, newInfo));
        else if (min != null)
            newIndex = Arrays.binarySearch(newById, 0, newById.length, min);
        return newIndex;
    }

    static int updateMaxAppliedUnreadyWriteById(TxnInfo newInfo, int insertPos, boolean inserted, CommandsForKey cfk, TxnInfo[] byId, TxnId[] additions, int additionCount)
    {
        int curIndex = cfk.maxAppliedUnreadyWriteById;
        if (!newInfo.mayExecute() && newInfo.is(APPLIED) && newInfo.is(Write) && cfk.isUnready(newInfo) && newInfo.executeAt.compareTo(cfk.bounds.readyAt) > 0
            && (curIndex < 0 || newInfo.executeAt.compareTo(byId[curIndex].executeAt) > 0))
            return advanceByIdIndexByAdditionsOnly(insertPos, newInfo, additions, additionCount);
        return advanceByIdIndex(curIndex, byId, insertPos, inserted, additions, additionCount);
    }

    static int recomputeMaxAppliedUnreadyWriteById(QuickBounds bounds, TxnInfo[] newById, int maxIndex)
    {
        maxIndex = Math.min(maxIndex, newById.length);
        int newMaxAppliedUnreadyWriteById = -1;
        for (int i = 0 ; i < maxIndex ; ++i)
            newMaxAppliedUnreadyWriteById = maybeUpdateMaxAppliedUnreadyWriteById(bounds, newById, newMaxAppliedUnreadyWriteById, i);
        return newMaxAppliedUnreadyWriteById;
    }

    static int maybeUpdateMaxAppliedUnreadyWriteById(QuickBounds bounds, TxnInfo[] byId, int newMaxAppliedUnreadyWriteById, int i)
    {
        TxnInfo txn = byId[i];
        if (!txn.mayExecute() && txn.is(APPLIED) && txn.isWrite() && CommandsForKey.isUnready(txn, bounds) && txn.executeAt.compareTo(bounds.readyAt) > 0
            && (newMaxAppliedUnreadyWriteById < 0 || txn.executeAt.compareTo(byId[newMaxAppliedUnreadyWriteById].executeAt) > 0))
            return i;
        return newMaxAppliedUnreadyWriteById;
    }

    static Object computeInfoAndAdditions(CommandsForKey cfk, int insertPos, int updatePos, TxnId plainTxnId, InternalStatus newStatus, boolean mayExecute, Command command)
    {
        Invariants.require(newStatus.hasDeps);
        Timestamp executeAt = command.executeAt();
        if (!newStatus.hasExecuteAt || executeAt.equals(plainTxnId)) executeAt = plainTxnId;
        Ballot ballot = Ballot.ZERO;
        if (newStatus.hasBallot)
            ballot = command.acceptedOrCommitted();

        // TODO (desired): is this the best place to encode this behaviour?
        if (command.saveStatus().is(Status.Truncated))
            return TxnInfo.create(plainTxnId, newStatus, mayExecute, executeAt, NO_TXNIDS, ballot);

        Timestamp depsKnownBefore = newStatus.depsKnownBefore(plainTxnId, executeAt);
        MergeCursor<TxnId, DepList> deps = command.partialDeps().txnIds(cfk.key());
        deps.find(cfk.redundantBefore());

        boolean durablyCommitted = command.durability().isDurablyCommitted();
        return computeInfoAndAdditions(cfk.byId, cfk.minUndecidedById(), insertPos, updatePos, plainTxnId, newStatus, mayExecute, durablyCommitted, ballot, executeAt, cfk.prunedBefore(), depsKnownBefore, deps);
    }

    /**
     * We return an Object here to avoid wasting allocations; most of the time we expect a new TxnInfo to be returned,
     * but if we have transitive dependencies to insert we return an InfoWithAdditions
     */
    static Object computeInfoAndAdditions(TxnInfo[] byId, int minUndecidedById, int insertPos, int updatePos, TxnId plainTxnId, InternalStatus newStatus, boolean mayExecute, boolean durablyCommitted, Ballot ballot, Timestamp executeAt, TxnInfo prunedBefore, Timestamp depsKnownBefore, MergeCursor<TxnId, DepList> deps)
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
            // depsKnownBeforePos can be positive if we have a uniqueHlc but agreed the TxnId as the executeAt
            if (depsKnownBeforePos < 0)
                depsKnownBeforePos = -1 - depsKnownBeforePos;
        }

        int txnIdsIndex;
        if (deps.hasCur())
        {
            txnIdsIndex = Arrays.binarySearch(byId, 0, minUndecidedById < 0 ? byId.length : minUndecidedById, deps.cur());
            if (txnIdsIndex < 0)
                txnIdsIndex = -1 - txnIdsIndex;
        }
        else
        {
            txnIdsIndex = minUndecidedById < 0 ? byId.length : minUndecidedById;
        }

        while (txnIdsIndex < byId.length && deps.hasCur())
        {
            TxnInfo t = byId[txnIdsIndex];
            TxnId d = deps.cur();
            int c = t.compareTo(d);
            if (c == 0)
            {
                // TODO (expected): if plainTxnId implies TRANSITIVE_VISIBLE dependencies,
                //  we should ensure any existing TRANSITIVE entries are upgraded.
                // OR we should remove TRANSITIVE for simplicity,
                // OR document/enforce that TRANSITIVE_VISIBLE can only be applied to dependencies of unmanaged transactions
                if (d.is(UNSTABLE)  && txnIdsIndex < depsKnownBeforePos && t.compareTo(COMMITTED) < 0 && plainTxnId.witnesses(d))
                    missing = append(missing, missingCount++, d, cachedTxnIds());

                ++txnIdsIndex;
                deps.advance();
            }
            else if (c < 0)
            {
                // we expect to be missing ourselves
                // we also permit any transaction we have recorded as COMMITTED or later to be missing, as recovery will not need to consult our information
                if (txnIdsIndex != updatePos && txnIdsIndex < depsKnownBeforePos && t.compareTo(COMMITTED) < 0 && plainTxnId.witnesses(t))
                    missing = append(missing, missingCount++, t.plainTxnId(), cachedTxnIds());
                txnIdsIndex++;
            }
            else
            {
                if (plainTxnId.witnesses(d))
                {
                    if (d.is(UNSTABLE))
                    {
                        if (d.compareTo(depsKnownBefore) < 0 && (manages(d) || d.compareTo(prunedBefore) > 0))
                            missing = append(missing, missingCount++, d, cachedTxnIds());
                        d = d.withoutNonIdentityFlags();
                    }
                    additions = append(additions, additionCount++, d, cachedTxnIds());
                }
                else
                {
                    // we can take dependencies on ExclusiveSyncPoints to represent a GC point in the log
                    // if we don't ordinarily witness a transaction it is meaningless to include it as a dependency
                    // as we will not logically be able to work with it (the missing collection will not correctly represent it anyway)
                    Invariants.require(d.isSyncPoint());
                }
                deps.advance();
            }
        }

        if (deps.hasCur())
        {
            do
            {
                TxnId d = deps.cur();
                if (plainTxnId.witnesses(d))
                {
                    if (d.is(UNSTABLE))
                    {
                        if (d.compareTo(depsKnownBefore) < 0 && (manages(d) || d.compareTo(prunedBefore) > 0))
                            missing = append(missing, missingCount++, d, cachedTxnIds());
                        d = d.withoutNonIdentityFlags();
                    }
                    additions = append(additions, additionCount++, d, cachedTxnIds());
                }
                deps.advance();
            }
            while (deps.hasCur());
        }
        else if (txnIdsIndex < byId.length)
        {
            while (txnIdsIndex < depsKnownBeforePos)
            {
                if (txnIdsIndex != updatePos && byId[txnIdsIndex].compareTo(COMMITTED) < 0)
                {
                    TxnInfo txn = byId[txnIdsIndex];
                    if (plainTxnId.witnesses(txn))
                        missing = append(missing, missingCount++, txn.plainTxnId(), cachedTxnIds());
                }
                txnIdsIndex++;
            }
        }

        TxnInfo info = TxnInfo.create(plainTxnId, newStatus, mayExecute, 0, durablyCommitted, executeAt, cachedTxnIds().completeAndDiscard(missing, missingCount), ballot);
        if (additionCount == 0)
            return info;

        return new InfoWithAdditions(info, additions, additionCount);
    }

    private static <T> T[] append(T[] array, int index, T add, ObjectBuffers<T> cached)
    {
        if (index == array.length)
            array = cached.resize(array, index, Math.max(8, index * 2));
        array[index] = add;
        return array;
    }

    static CommandsForKeyUpdate insertOrUpdate(CommandsForKey cfk, int pos, TxnId plainTxnId, TxnInfo curInfo, TxnInfo newInfo, Command command, @Nullable TxnId[] witnessedBy)
    {
        if (curInfo == newInfo)
            return cfk;

        Object[] newLoadingPruned = cfk.loadingPruned;
        if (witnessedBy != NOT_LOADING_PRUNED)
            newLoadingPruned = removeLoadingPruned(newLoadingPruned, plainTxnId);

        int committedByExecuteAtUpdatePos = committedByExecuteAtUpdatePos(cfk.committedByExecuteAt, curInfo, newInfo);
        TxnInfo[] newCommittedByExecuteAt = updateCommittedByExecuteAt(cfk, committedByExecuteAtUpdatePos, newInfo);
        int newMaxAppliedWriteByExecuteAt = updateMaxAppliedWriteByExecuteAt(cfk, committedByExecuteAtUpdatePos, newInfo, newCommittedByExecuteAt);

        int newMinUndecidedManagedById = cfk.minUndecidedManagedById;
        int newMinUndecidedUnmanagedById = cfk.minUndecidedUnmanagedById;
        TxnInfo[] byId = cfk.byId;
        TxnInfo[] newById;
        if (curInfo == null)
        {
            newById = new TxnInfo[byId.length + 1];
            System.arraycopy(byId, 0, newById, 0, pos);
            newById[pos] = newInfo;
            System.arraycopy(byId, pos, newById, pos + 1, byId.length - pos);
            if (newInfo.mayExecute())
            {
                if (newInfo.compareTo(COMMITTED) >= 0)
                {
                    if (pos <= newMinUndecidedManagedById)
                    {
                        if (pos < newMinUndecidedManagedById) ++newMinUndecidedManagedById;
                        else newMinUndecidedManagedById = nextUndecided(true, newById, pos + 1, cfk);
                    }
                }
                else
                {
                    if (newMinUndecidedManagedById < 0 || pos < newMinUndecidedManagedById)
                        newMinUndecidedManagedById = pos;
                }
                if (pos <= newMinUndecidedUnmanagedById)
                    ++newMinUndecidedUnmanagedById;
            }
            else
            {
                if (pos <= newMinUndecidedManagedById)
                    ++newMinUndecidedManagedById;
                if (pos <= newMinUndecidedUnmanagedById)
                {
                    if (pos < newMinUndecidedUnmanagedById) ++newMinUndecidedUnmanagedById;
                    else newMinUndecidedUnmanagedById = nextUndecided(false, newById, pos + 1, cfk);
                }
            }
        }
        else
        {
            newById = byId.clone();
            newById[pos] = newInfo;
            if (pos == newMinUndecidedManagedById && curInfo.compareTo(COMMITTED) < 0 && newInfo.compareTo(COMMITTED) >= 0)
                newMinUndecidedManagedById = nextUndecided(true, newById, pos + 1, cfk);
            if (pos == newMinUndecidedUnmanagedById && curInfo.compareTo(COMMITTED) < 0 && newInfo.compareTo(COMMITTED) >= 0)
                newMinUndecidedUnmanagedById = nextUndecided(false, newById, pos + 1, cfk);
        }

        if (curInfo != null && curInfo.compareTo(COMMITTED) < 0 && newInfo.compareTo(COMMITTED) >= 0)
        {
            Utils.removeFromMissingArrays(newById, newCommittedByExecuteAt, plainTxnId);
        }
        else if (curInfo == null && newInfo.compareTo(COMMITTED) < 0)
        {
            // TODO (desired): for consistency, move this to insertOrUpdate (without additions), while maintaining the efficiency
            Utils.addToMissingArrays(newById, newCommittedByExecuteAt, newInfo, plainTxnId, witnessedBy);
        }
        else if (witnessedBy != null && newInfo.compareTo(COMMITTED) >= 0)
        {
            Utils.removeFromWitnessMissingArrays(newById, newCommittedByExecuteAt, plainTxnId, witnessedBy);
        }

        if (testParanoia(SUPERLINEAR, NONE, LOW) && curInfo == null && newInfo.compareTo(COMMITTED) < 0)
            validateMissing(newById, NO_TXNIDS, 0, curInfo, newInfo, witnessedBy);

        int newPrunedBeforeById = advanceByIdIndex(cfk.prunedBeforeById, pos, curInfo == null);
        Invariants.paranoid(cfk.prunedBeforeById < 0 || newById[newPrunedBeforeById].equals(byId[cfk.prunedBeforeById]));
        int newMaxAppliedUnreadyWriteById = updateMaxAppliedUnreadyWriteById(newInfo, pos, curInfo == null, cfk, byId, NO_INFOS, 0);

        long newMaxHlc = updateMaxUniqueHlc(cfk, newInfo, command);
        return cfk.update(newById, newMinUndecidedManagedById, newMinUndecidedUnmanagedById, newMaxAppliedUnreadyWriteById, newCommittedByExecuteAt, newMaxAppliedWriteByExecuteAt, newMaxHlc, newLoadingPruned, newPrunedBeforeById, curInfo, newInfo);
    }

    /**
     * Update newById to insert or update newInfo; insert any additions, and update any relevant missing arrays in
     * both newById and newCommittedByExecuteAt (for both deletions and insertions).
     */
    static void insertOrUpdateWithAdditions(TxnInfo[] byId, int sourceInsertPos, int sourceUpdatePos, TxnId plainTxnId, TxnInfo newInfo, TxnId[] additions, int additionCount, TxnInfo[] newById, TxnInfo[] newCommittedByExecuteAt, @Nonnull TxnId[] witnessedBy, QuickBounds bounds)
    {
        int additionInsertPos = Arrays.binarySearch(additions, 0, additionCount, plainTxnId);
        additionInsertPos = Invariants.requireArgument(-1 - additionInsertPos, additionInsertPos < 0);
        int targetInsertPos = sourceInsertPos + additionInsertPos;

        // we may need to insert or remove ourselves, depending on the new and prior status
        boolean insertSelfMissing = sourceUpdatePos < 0 && newInfo.compareTo(COMMITTED) < 0;
        boolean removeSelfMissing = newInfo.compareTo(COMMITTED) >= 0 && sourceUpdatePos >= 0 && byId[sourceUpdatePos].compareTo(COMMITTED) < 0;
        boolean removeWitnessedMissing = newInfo.compareTo(COMMITTED) >= 0 && witnessedBy.length > 0;
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
                else if (txn.hasDeps())
                {
                    Timestamp depsKnownBefore = txn.depsKnownBefore();
                    if (insertSelfMissing && missingSource == additions && (missingCount != j || (depsKnownBefore != txn && depsKnownBefore.compareTo(plainTxnId) > 0)))
                    {
                        // add ourselves to the missing collection
                        missingSource = insertMissing(additions, additionCount, plainTxnId, additionInsertPos);
                        ++missingLimit;
                    }

                    int to = missingTo(txn, depsKnownBefore, missingSource, missingCount, missingLimit);
                    if (to > 0 || removeSelfMissing || removeWitnessedMissing)
                    {
                        int witnessedByIndex = -1;
                        if (witnessedBy != NOT_LOADING_PRUNED && (to > 0 || !removeSelfMissing))
                            witnessedByIndex = Arrays.binarySearch(witnessedBy, txn);

                        TxnId[] curMissing = txn.missing();
                        TxnId[] newMissing = curMissing;
                        if (to > 0)
                        {
                            TxnId skipInsertMissing = null;
                            if (insertSelfMissing && witnessedByIndex >= 0)
                                skipInsertMissing = plainTxnId;

                            newMissing = mergeAndFilterMissing(txn, curMissing, missingSource, to, skipInsertMissing);
                        }

                        if (removeSelfMissing || (removeWitnessedMissing && witnessedByIndex >= 0))
                            newMissing = removeOneMissing(newMissing, plainTxnId);

                        if (newMissing != curMissing)
                        {
                            TxnInfo newTxn = txn.withMissing(newMissing);

                            if (txn.isCommittedAndExecutes())
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
                                Invariants.require(newCommittedByExecuteAt[ci] == txn);
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
                newById[count] = TxnInfo.createTransitive(txnId, bounds, plainTxnId);
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
                    newById[count++] = TxnInfo.createTransitive(txnId, bounds, plainTxnId);
                }
                newById[targetInsertPos] = newInfo;
                count = targetInsertPos + 1;
            }
            while (j < additionCount)
            {
                TxnId txnId = additions[j++];
                newById[count++] = TxnInfo.createTransitive(txnId, bounds, plainTxnId);
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
    static TxnInfo[] insertAdditionsOnly(TxnInfo[] byId, TxnInfo[] committedByExecuteAt, TxnInfo[] newInfos, TxnId[] additions, int additionCount, QuickBounds bounds, TxnId witnessedBy)
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
                if (txn.hasDeps())
                {
                    Timestamp depsKnownBefore = txn.depsKnownBefore();

                    int to = missingTo(txn, depsKnownBefore, additions, missingCount, additionCount);
                    if (to > 0)
                    {
                        TxnId[] prevMissing = txn.missing();
                        TxnId[] newMissing = mergeAndFilterMissing(txn, prevMissing, additions, to, null);
                        if (newMissing != prevMissing)
                        {
                            TxnInfo newTxn = txn.withMissing(newMissing);
                            if (txn.isCommittedAndExecutes())
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
                                Invariants.require(newCommittedByExecuteAt[ci] == txn);
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
                TxnId txnId = additions[j];
                newInfos[count] = TxnInfo.createTransitive(txnId, bounds, witnessedBy);
                ++j;
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
            TxnId txnId = additions[j];
            newInfos[count++] = TxnInfo.create(txnId, TRANSITIVE_VISIBLE, mayExecute(bounds, txnId), txnId, Ballot.ZERO);
            j++;
        }

        if (newCommittedByExecuteAt == null)
            newCommittedByExecuteAt = committedByExecuteAt;
        return newCommittedByExecuteAt;
    }

    static int committedByExecuteAtUpdatePos(TxnInfo[] committedByExecuteAt, @Nullable TxnInfo curInfo, TxnInfo newInfo)
    {
        if (newInfo.isCommittedAndExecutes())
        {
            return Arrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, newInfo, TxnInfo::compareExecuteAt);
        }
        else if (curInfo != null && curInfo.isCommittedAndExecutes() && newInfo.isAtLeast(INVALIDATED))
        {
            return Arrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, curInfo, TxnInfo::compareExecuteAt);
        }
        return Integer.MAX_VALUE;
    }

    private static TxnInfo[] updateCommittedByExecuteAt(CommandsForKey cfk, int pos, TxnInfo newInfo)
    {
        TxnInfo[] committedByExecuteAt = cfk.committedByExecuteAt;
        if (pos == Integer.MAX_VALUE)
            return committedByExecuteAt;

        if (newInfo.compareTo(INVALIDATED) < 0)
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
                if (pos < maxAppliedWriteByExecuteAt && !newInfo.is(PRUNED))
                {
                    for (int i = pos; i <= maxAppliedWriteByExecuteAt; ++i)
                    {
                        if (committedByExecuteAt[pos].witnesses(newInfo))
                            reportLinearizabilityViolation(cfk.key, newInfo.plainTxnId(), newInfo.plainExecuteAt(), committedByExecuteAt[i].plainTxnId(), committedByExecuteAt[i].plainExecuteAt());
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

    private static int updateMaxAppliedWriteByExecuteAt(CommandsForKey cfk, int pos, TxnInfo newInfo, TxnInfo[] newCommittedByExecuteAt)
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
            else if (newInfo.isAtLeast(INVALIDATED))
            {
                // deleted
                int newMaxAppliedWriteByExecuteAt = maxAppliedWriteByExecuteAt - 1;
                while (newMaxAppliedWriteByExecuteAt >= 0 && newCommittedByExecuteAt[newMaxAppliedWriteByExecuteAt].kind() != Write)
                    --newMaxAppliedWriteByExecuteAt;
                return newMaxAppliedWriteByExecuteAt;
            }
        }
        else if (newInfo.is(APPLIED) || (cfk.isUnready(newInfo) && pos - 1 == maxAppliedWriteByExecuteAt))
        {
            return maybeAdvanceMaxAppliedAndCheckForLinearizabilityViolations(cfk, pos, newInfo.kind(), newInfo);
        }
        return maxAppliedWriteByExecuteAt;
    }

    private static int maybeAdvanceMaxAppliedAndCheckForLinearizabilityViolations(CommandsForKey cfk, int appliedPos, Txn.Kind appliedKind, TxnInfo applied)
    {
        TxnInfo[] committedByExecuteAt = cfk.committedByExecuteAt;
        for (int i = cfk.maxAppliedWriteByExecuteAt + 1; i < appliedPos ; ++i)
        {
            if (committedByExecuteAt[i].isNot(APPLIED) && appliedKind.witnesses(committedByExecuteAt[i]))
                reportLinearizabilityViolation(cfk.key, committedByExecuteAt[i].plainTxnId(), committedByExecuteAt[i].plainExecuteAt(), applied.plainTxnId(), applied.plainExecuteAt());
        }

        return appliedKind == Txn.Kind.Write ? appliedPos : cfk.maxAppliedWriteByExecuteAt;
    }

    static int nextUndecided(boolean managed, TxnInfo[] infos, int pos, CommandsForKey cfk)
    {
        while (true)
        {
            if (pos == infos.length)
                return -1;

            if (infos[pos].compareTo(COMMITTED) < 0 && (!managed ^ cfk.mayExecute(infos[pos])))
                return pos;

            ++pos;
        }
    }

    private static long updateMaxUniqueHlc(CommandsForKey cfk, TxnInfo newInfo, Command update)
    {
        long maxUniqueHlc = cfk.maxUniqueHlc;
        if (newInfo.is(APPLIED) && newInfo.is(Write) && (newInfo.mayExecute() || executesIgnoringBootstrap(cfk.bounds, newInfo, newInfo.executeAt)))
        {
            long newUniqueHlc = update.executeAt().uniqueHlc();
            if (maxUniqueHlc < newUniqueHlc)
                maxUniqueHlc = newUniqueHlc;
        }
        return maxUniqueHlc;
    }

    static void updateUnmanagedAsync(CommandStore commandStore, TxnId txnId, RoutingKey key, NotifySink notifySink)
    {
        ExecutionContext context = ExecutionContext.unsequencedWrite(txnId, RoutingKeys.of(key), "Update unmanaged CommandsForKey");
        commandStore.execute(context, safeStore -> {
            SafeCommandsForKey safeCommandsForKey = safeStore.get(key);
            CommandsForKey cur = safeCommandsForKey.current();
            CommandsForKeyUpdate next = Updating.updateUnmanaged(cur, safeStore, safeStore.unsafeGet(txnId));
            if (cur != next)
            {
                if (cur != next.cfk())
                    safeCommandsForKey.set(next.cfk());

                PostProcess postProcess = next.postProcess();
                if (postProcess != null)
                    postProcess.postProcess(safeStore, key, notifySink);
            }
        }, commandStore.agent());
    }

    static CommandsForKeyUpdate updateUnmanaged(CommandsForKey cfk, SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        return Updating.updateUnmanaged(cfk, safeStore, safeCommand, UPDATE, null);
    }

    static CommandsForKeyUpdate registerDependencies(CommandsForKey cfk, SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        return Updating.updateUnmanaged(cfk, safeStore, safeCommand, REGISTER_DEPS_ONLY, null);
    }

    /**
     * Four modes of operation:
     *  - {@code REGISTER_DEPS_ONLY}: inserts any missing dependencies into the collection; may return CommandsForKeyUpdate for loading pruned commands
     *  - {@code REGISTER}: inserts any missing dependencies into the collection and inserts the unmanaged command; may return CommandsForKeyUpdate
     *  - {@code UPDATE, update == null}: fails if any dependencies are missing; always returns a CommandsForKey
     *  - {@code UPDATE && update != null}: fails if any dependencies are missing; always returns the original CommandsForKey, and maybe adds a new Unmanaged to {@code update}
     */
    static CommandsForKeyUpdate updateUnmanaged(CommandsForKey cfk, SafeCommandStore safeStore, SafeCommand safeCommand, UpdateUnmanagedMode mode, @Nullable List<CommandsForKey.Unmanaged> update)
    {
        boolean register = mode != UPDATE;
        Invariants.requireArgument(mode == UPDATE || update == null);
        if (safeCommand.current().hasBeen(Status.Truncated))
            return cfk;

        Command orig = safeCommand.current();
        if (orig.saveStatus() == Uninitialised && orig.txnId().is(EphemeralRead))
            return cfk;

        Command.Committed command = safeCommand.current().asCommitted();
        TxnId waitingTxnId = command.txnId();
        // used only to decide if an executeAt is included _on the assumption the TxnId is_. For ?[EX] this is all timestamps
        Timestamp compareExecuteAt = waitingTxnId.awaitsOnlyDeps() ? Timestamp.MAX : command.executeAt();

        TxnInfo[] byId = cfk.byId, newById = null;
        RelationMultiMap.SortedRelationList<TxnId> txnIds = command.partialDeps().keyDeps.txnIdsWithFlags(cfk.key());
        TxnId[] missing = NO_TXNIDS;
        int missingCount = 0;
        // we only populate dependencies to facilitate execution, not for any distributed decision,
        // so we can filter to only transactions we need to execute locally
        int i = txnIds.find(cfk.redundantOrBootstrappedBefore());
        if (i < 0) i = -1 - i;
        int waitingFromIndex = i; // the min input index we expect to execute
        if (waitingTxnId.isSyncPoint() && waitingTxnId.is(Range) && mode == REGISTER)
        {
            // for RX we register all our transitive dependencies to make sure we can answer coordinated dependency calculations
            // in this case we separate out the position from which we insert missing txnId and where we compute readiness to execute
            i = txnIds.find(cfk.redundantBefore());
            if (i < 0) i = -1 - i;
        }

        // note that while we may directly insert transactions that are for a future epoch we don't own,
        // we don't do this for dependencies as we only want to know these transactions for answering
        // recovery decisions about transactions we do own
        // We make sure to compute dependencies that satisfy all participating epochs for RX/KX
        int toIndex = txnIds.size();
        while (toIndex > 0 && txnIds.get(toIndex - 1).epoch() >= cfk.bounds.endEpoch)
            --toIndex;

        if (i < toIndex)
        {
            // we may have dependencies that execute after us, but in this case we only wait for them to (transitively) commit
            // so we distinguish the input index we need to execute transactions to from the index we need to commit to
            int waitingToExecuteIndex = toIndex;
            if (waitingToExecuteIndex > waitingFromIndex)
            {
                TxnId maxWaiting = txnIds.get(waitingToExecuteIndex - 1);
                Timestamp includeBefore = waitingTxnId.is(EphemeralRead) ? Timestamp.MAX : command.executeAt();
                if (maxWaiting.compareTo(includeBefore) >= 0)
                {
                    waitingToExecuteIndex = txnIds.find(includeBefore);
                    if (waitingToExecuteIndex < 0)
                        waitingToExecuteIndex = -1 - waitingToExecuteIndex;
                }
            }

            boolean readyToApply = true; // our dependencies have applied, so we are ready to apply
            boolean waitingToApply = true; // our dependencies have committed, so we know when we execute and are waiting
            boolean hasFutureDependency = toIndex > waitingFromIndex
                                          && !waitingTxnId.is(EphemeralRead) // ephemeral reads wait for everything they witness, regardless of TxnId or executeAt, so the latest dependency is always enough
                                          && txnIds.get(toIndex - 1).compareTo(command.executeAt()) > 0;

            if (hasFutureDependency)
            {
                // This logic is to handle the case where we have pruned dependencies on the replicas we have used to calculate our dependencies
                // so we may be missing an execution dependency, and we require that we are transitively committed
                int waitingOnCommit = Arrays.binarySearch(byId, txnIds.get(toIndex - 1));
                if (waitingOnCommit < 0) waitingOnCommit = -1 - waitingOnCommit;
                if (cfk.minUndecidedManagedById >= 0 && cfk.minUndecidedManagedById < waitingOnCommit)
                    readyToApply = waitingToApply = false;
            }

            Timestamp waitingToExecuteAt = null; // when the CFK should notify not waiting
            Timestamp effectiveExecutesAt = null; // the executeAt we should report to WaitingOn for ephemeral reads

            int j = SortedArrays.binarySearch(byId, 0, byId.length, txnIds.get(i), Timestamp::compareTo, FAST);
            if (j < 0) j = -1 -j;

            while (i < toIndex)
            {
                int c = j == byId.length ? -1 : txnIds.get(i).compareTo(byId[j]);
                if (c >= 0)
                {
                    TxnInfo txn = byId[j];
                    if (c == 0 || txn.witnessedBy(waitingTxnId))
                    {
                        if (i >= waitingFromIndex && waitingToApply)
                        {
                            if (txn.mayExecute())
                            {
                                if (txn.compareTo(COMMITTED) < 0) waitingToApply = readyToApply = false;
                                else if (i < waitingToExecuteIndex)
                                {
                                    if (txn.compareTo(APPLIED) < 0 && txn.executeAt.compareTo(compareExecuteAt) < 0)
                                    {
                                        readyToApply = false;
                                        waitingToExecuteAt = Timestamp.nonNullOrMax(waitingToExecuteAt, txn.executeAt);
                                    }
                                    else if (txn.is(APPLIED))
                                    {
                                        effectiveExecutesAt = Timestamp.nonNullOrMax(effectiveExecutesAt, txn.executeAt);
                                    }
                                }
                            }
                            else if (waitingTxnId.awaitsOnlyDeps())
                            {
                                // only really need to track epoch, but track max executeAt to support retryInLatestEpoch
                                effectiveExecutesAt = Timestamp.nonNullOrMax(effectiveExecutesAt, txn.executeAt);
                            }
                        }
                        if (c == 0)
                        {
                            if (txn.is(TRANSITIVE) && isTransitiveDependencyVisible(waitingTxnId))
                            {
                                if (newById == null)
                                    newById = byId.clone();
                                newById[j] = TxnInfo.create(txn, TRANSITIVE_VISIBLE, txn.mayExecute(), txn.statusOverrides(), false, txn.executeAt, txn.missing(), txn.ballot());
                            }
                            ++i;
                        }
                    }
                    ++j;
                }
                else if (register)
                {
                    TxnId insert = txnIds.get(i);
                    if (i >= waitingFromIndex)
                        readyToApply = waitingToApply = false;
                    if (missingCount == missing.length)
                        missing = cachedTxnIds().resize(missing, missingCount, Math.max(8, missingCount + missingCount/2));
                    missing[missingCount++] = insert;
                    ++i;
                }
                else
                {
                    Invariants.require(txnIds.get(i).compareTo(TxnId.max(cfk.bounds.locallyAppliedBefore, cfk.prunedBefore())) < 0);
                    ++i;
                }
            }

            if (toIndex < txnIds.size() && waitingTxnId.awaitsOnlyDeps())
                effectiveExecutesAt = Timestamp.nonNullOrMax(effectiveExecutesAt, txnIds.get(txnIds.size() - 1));

            // TODO (required): document why we can restrict this test to sync points
            if (waitingToApply && waitingTxnId.isSyncPoint() && Pruning.isAnyPredecessorWaitingOnPruned(cfk.loadingPruned, waitingTxnId))
                readyToApply = waitingToApply = false;

            if (waitingToApply && hasFutureDependency)
            {
                // This logic is to handle the case where we have pruned dependencies on the replicas we have used to calculate our dependencies
                // so we may be missing an execution dependency, and we require that we are transitively committed
                int w = Arrays.binarySearch(byId, command.executeAt());
                if (w < 0) w = -1 - w;
                // TODO (desired): consider moving this logic inline to the main loop body
                while (--w >= 0)
                {
                    TxnInfo txn = byId[w];
                    if (!txn.mayExecute()
                        || !txn.witnessedBy(waitingTxnId)
                        || txn.isAtLeast(APPLIED))
                        continue;

                    Invariants.require(txn.compareTo(COMMITTED) >= 0);
                    if (txn.executeAt.compareTo(compareExecuteAt) >= 0)
                        continue;

                    readyToApply = false;
                    waitingToExecuteAt = Timestamp.nonNullOrMax(waitingToExecuteAt, txn.executeAt);
                }
            }

            waitingToExecuteAt = updateExecuteAtLeast(waitingToExecuteAt, effectiveExecutesAt, safeStore, safeCommand);
            if (!readyToApply || missingCount > 0 || newById != null)
            {
                if (newById == null) newById = byId;
                TxnInfo[] newCommittedByExecuteAt = cfk.committedByExecuteAt;
                int newMinUndecidedManagedById = cfk.minUndecidedManagedById;
                int newMinUndecidedUnmanagedById = cfk.minUndecidedUnmanagedById;
                int newMaxAppliedUnreadyWriteById = cfk.maxAppliedUnreadyWriteById;
                Object[] newLoadingPruned = cfk.loadingPruned;
                TxnId[] prunedIds = NO_TXNIDS;
                int clearMissingCount = missingCount;
                if (missingCount > 0)
                {
                    int prunedIndex = Arrays.binarySearch(missing, 0, missingCount, cfk.prunedBefore());
                    if (prunedIndex < 0) prunedIndex = -1 - prunedIndex;
                    if (prunedIndex > 0)
                    {
                        prunedIds = Arrays.copyOf(missing, prunedIndex);
                        List<TxnId> insertLoadPruned = new ArrayList<>();
                        newLoadingPruned = Pruning.loadPruned(cfk.loadingPruned, prunedIds, waitingTxnId, insertLoadPruned);
                        prunedIds = insertLoadPruned.isEmpty() ? NO_TXNIDS : insertLoadPruned.toArray(TxnId[]::new);
                    }

                    if (prunedIndex != missingCount)
                    {
                        missingCount -= prunedIndex;
                        System.arraycopy(missing, prunedIndex, missing, 0, missingCount);
                        removeNonIdentityFlags(missing, missingCount);
                        TxnId minUndecidedManaged, minUndecidedUnmanaged;
                        {
                            int minManagedIndex = 0, minUnmanagedIndex = 0;
                            while (minManagedIndex < missingCount && !cfk.mayExecute(missing[minManagedIndex]))
                                ++minManagedIndex;

                            if (minManagedIndex == 0)
                            {
                                do { ++minUnmanagedIndex; }
                                while (minUnmanagedIndex < missingCount && cfk.mayExecute(missing[minUnmanagedIndex]));
                            }

                            TxnId minManaged = minManagedIndex == missingCount ? null : missing[minManagedIndex];
                            TxnId minUnmanaged = minUnmanagedIndex == missingCount ? null : missing[minUnmanagedIndex];

                            minUndecidedManaged = TxnId.nonNullOrMin(minManaged, cfk.minUndecidedManaged());
                            minUndecidedUnmanaged = TxnId.nonNullOrMin(minUnmanaged, cfk.minUndecidedUnmanaged());
                        }
                        TxnInfo[] copyById = newById;
                        newById = new TxnInfo[byId.length + missingCount];
                        newCommittedByExecuteAt = insertAdditionsOnly(copyById, cfk.committedByExecuteAt, newById, missing, missingCount, cfk.bounds, waitingTxnId);
                        // we can safely use missing[prunedIndex] here because we only fill missing with transactions for which we manage execution
                        if (minUndecidedManaged != null)
                            newMinUndecidedManagedById = Arrays.binarySearch(newById, minUndecidedManaged);
                        if (minUndecidedUnmanaged != null)
                            newMinUndecidedUnmanagedById = Arrays.binarySearch(newById, minUndecidedUnmanaged);
                        newMaxAppliedUnreadyWriteById = advanceByIdIndex(newMaxAppliedUnreadyWriteById, byId, Integer.MAX_VALUE, false, missing, missingCount);
                    }
                }
                cachedTxnIds().discard(missing, clearMissingCount);

                CommandsForKey.Unmanaged[] newUnmanaged = cfk.unmanageds;
                if (!readyToApply && mode != REGISTER_DEPS_ONLY)
                {
                    CommandsForKey.Unmanaged newPendingRecord;
                    if (waitingToApply) newPendingRecord = new CommandsForKey.Unmanaged(APPLY, command.txnId(), waitingToExecuteAt);
                    else newPendingRecord = new CommandsForKey.Unmanaged(COMMIT, command.txnId(), txnIds.get(toIndex - 1));

                    if (update != null)
                    {
                        update.add(newPendingRecord);
                        return cfk;
                    }

                    newUnmanaged = SortedArrays.insert(cfk.unmanageds, newPendingRecord, CommandsForKey.Unmanaged[]::new);
                }

                CommandsForKey newCfk;
                if (newById == byId) newCfk = new CommandsForKey(cfk, newLoadingPruned, newUnmanaged);
                else
                {
                    int prunedBeforeById = cfk.prunedBeforeById;
                    Invariants.require(prunedBeforeById < 0 || newById[prunedBeforeById].equals(cfk.prunedBefore()));
                    newCfk = new CommandsForKey(cfk.key(), cfk.bounds, newById, newMinUndecidedManagedById, newMinUndecidedUnmanagedById, newMaxAppliedUnreadyWriteById, newCommittedByExecuteAt, cfk.maxAppliedWriteByExecuteAt, cfk.maxUniqueHlc, newLoadingPruned, prunedBeforeById, newUnmanaged, true);
                }

                CommandsForKeyUpdate result = newCfk;
                if (prunedIds != NO_TXNIDS)
                    result = new CommandsForKeyUpdateWithPostProcess(newCfk, new LoadPruned(result.postProcess(), prunedIds));

                if (readyToApply)
                {
                    paranoidCheckExecutesAtLeast(newCfk, waitingTxnId, effectiveExecutesAt, waitingToExecuteAt);
                    result = new CommandsForKeyUpdateWithPostProcess(newCfk, new NotifyNotWaiting(result.postProcess(), new TxnId[] { safeCommand.txnId() }));
                }

                return result;
            }
        }

        return new CommandsForKeyUpdateWithPostProcess(cfk, new NotifyNotWaiting(null, new TxnId[] { safeCommand.txnId() }));
    }

    private static Timestamp updateExecuteAtLeast(Timestamp waitingToExecuteAt, Timestamp effectiveExecutesAt, SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        if (waitingToExecuteAt instanceof TxnInfo)
            waitingToExecuteAt = ((TxnInfo) waitingToExecuteAt).plainExecuteAt();

        TxnId txnId = safeCommand.txnId();
        if (txnId.awaitsOnlyDeps())
        {
            Command.Committed command = safeCommand.current().asCommitted();
            if (effectiveExecutesAt != null && effectiveExecutesAt.compareTo(command.waitingOn.executeAtLeast(txnId)) > 0)
            {
                Command.WaitingOn.Update waitingOn = new Command.WaitingOn.Update(command.waitingOn);
                if (effectiveExecutesAt instanceof TxnInfo)
                    effectiveExecutesAt = ((TxnInfo) effectiveExecutesAt).plainExecuteAt();
                waitingOn.updateExecuteAtLeast(txnId, effectiveExecutesAt);
                safeCommand.updateWaitingOn(safeStore, waitingOn);
            }
        }

        return waitingToExecuteAt;
    }

    private static void paranoidCheckExecutesAtLeast(CommandsForKey cfk, TxnId waitingId, Timestamp effectiveExecutesAt, Timestamp waitingToExecuteAt)
    {
        if (!Invariants.isParanoid() || !Invariants.testParanoia(LINEAR, CONSTANT, LOW))
            return;

        Timestamp maxExecutesAt = null;
        int maxi = cfk.indexOf(waitingId);
        if (maxi < 0) maxi = -1 - maxi;
        for (int i = 0 ; i < maxi ; ++i)
            maxExecutesAt = Timestamp.nonNullOrMax(maxExecutesAt, cfk.byId[i].executeAt);

        if (maxExecutesAt == null || maxExecutesAt.compareTo(waitingId) < 0)
            return;

        effectiveExecutesAt = Timestamp.nonNullOrMax(effectiveExecutesAt, waitingToExecuteAt);
        Invariants.require(Objects.equals(effectiveExecutesAt, maxExecutesAt));
    }
}
