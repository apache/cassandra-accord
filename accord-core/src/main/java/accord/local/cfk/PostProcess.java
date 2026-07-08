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
import java.util.function.Predicate;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.RedundantBefore.QuickBounds;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.CommandsForKey.Unmanaged;
import accord.local.cfk.CommandsForKeyUpdate.CommandsForKeyUpdateWithPostProcess;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.btree.BTree;

import static accord.local.CommandSummaries.SummaryStatus.APPLIED;
import static accord.local.ExecutionContext.unsequencedWrite;
import static accord.local.cfk.CommandsForKey.InternalStatus.INVALIDATED;
import static accord.local.cfk.CommandsForKey.InternalStatus.STABLE;
import static accord.local.cfk.CommandsForKey.Unmanaged.Pending.APPLY;
import static accord.local.cfk.CommandsForKey.maxContiguousManagedAppliedIndex;
import static accord.local.cfk.UpdateUnmanagedMode.UPDATE;
import static accord.local.cfk.Updating.updateUnmanaged;
import static accord.local.cfk.Updating.updateUnmanagedAsync;
import static accord.local.cfk.Utils.findApply;
import static accord.local.cfk.Utils.findCommit;
import static accord.local.cfk.Utils.findFirstApply;
import static accord.local.cfk.Utils.removeUnmanaged;
import static accord.local.cfk.Utils.selectUnmanaged;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static accord.utils.SortedArrays.Search.FAST;

abstract class PostProcess
{
    final PostProcess prev;

    protected PostProcess(PostProcess prev)
    {
        this.prev = prev;
    }

    void postProcess(SafeCommandStore safeStore, RoutingKey key, NotifySink notifySink)
    {
        doNotify(safeStore, key, notifySink);
        if (prev != null)
            prev.postProcess(safeStore, key, notifySink);
    }

    abstract void doNotify(SafeCommandStore safeStore, RoutingKey key, NotifySink notifySink);

    static class LoadPruned extends PostProcess
    {
        final TxnId[] load;

        LoadPruned(PostProcess prev, TxnId[] load)
        {
            super(prev);
            this.load = load;
        }

        void doNotify(SafeCommandStore safeStore, RoutingKey key, NotifySink notifySink)
        {
            SafeCommandsForKey safeCfk = safeStore.get(key);
            for (TxnId txnId : load)
            {
                safeStore = safeStore; // make it unsafe for use in lambda
                SafeCommand safeCommand = safeStore.ifLoadedAndInitialised(txnId);
                if (safeCommand != null && safeStore.tryRecurse())
                {
                    try { load(safeStore, safeCommand, safeCfk, notifySink); }
                    finally { safeStore.unrecurse(); }
                }
                else safeStore.commandStore().execute(unsequencedWrite(txnId, RoutingKeys.of(key), "Load Pruned CommandsForKey"), safeStore0 -> {
                    load(safeStore0, safeStore0.unsafeGet(txnId), safeStore0.get(key), notifySink);
                }, safeStore.agent());
            }
        }

        static void load(SafeCommandStore safeStore, SafeCommand safeCommand, SafeCommandsForKey safeCfk, NotifySink notifySink)
        {
            Command command = safeCommand.current();
            StoreParticipants participants = command.participants();
            if (participants.owns().domain() == Routable.Domain.Key && !participants.hasTouched(safeCfk.key()))
                command = safeCommand.updateParticipants(safeStore, participants.supplementHasTouched(RoutingKeys.of(safeCfk.key())));
            safeCfk.callback(safeStore, command, notifySink, false);
        }

        static CommandsForKeyUpdate load(TxnId[] txnIds, CommandsForKeyUpdate result)
        {
            if (txnIds.length == 0)
                return result;
            return new CommandsForKeyUpdateWithPostProcess(result.cfk(), new LoadPruned(result.postProcess(), txnIds));
        }
    }

    static class NotifyNotWaiting extends PostProcess
    {
        final TxnId[] notify;

        NotifyNotWaiting(PostProcess prev, TxnId[] notify)
        {
            super(prev);
            this.notify = notify;
        }

        void doNotify(SafeCommandStore safeStore, RoutingKey key, NotifySink notifySink)
        {
            for (TxnId txnId : notify)
                notifySink.notWaiting(safeStore, txnId, key, 0);
        }
    }

    /**
     * Anything that is pre-bootstrap can execute immediately, as
     * logically it will be included in the snapshot we materialise during bootstrap.
     *
     * In practice this means that transactions which include a bootstrap range and a range not covered by bootstrap
     * will not wait for the bootstrapping key.
     */
    static CommandsForKeyUpdate notifyManagedUnready(CommandsForKey prev, QuickBounds newBounds, CommandsForKeyUpdate update)
    {
        TxnId[] notify = NO_TXNIDS;
        int notifyCount = 0;
        // <= because maxAppliedWrite is actually maxAppliedOrUnreadyWrite
        for (TxnInfo txn : prev.committedByExecuteAt)
        {
            if (txn.compareTo(newBounds.readyAt) >= 0)
                continue;

            if (txn.is(STABLE))
            {
                if (notifyCount == notify.length)
                    notify = cachedTxnIds().resize(notify, notifyCount, Math.max(notifyCount * 2, 8));
                notify[notifyCount++] = txn.plainTxnId();
            }
        }

        if (notifyCount == 0)
            return update;

        notify = cachedTxnIds().completeAndDiscard(notify, notifyCount);
        PostProcess newPostProcess = new NotifyNotWaiting(update.postProcess(), notify);
        return new CommandsForKeyUpdateWithPostProcess(update.cfk(), newPostProcess);
    }

    static class NotifyUnmanagedOfCommit extends PostProcess
    {
        final TxnId[] notify;

        NotifyUnmanagedOfCommit(PostProcess prev, TxnId[] notify)
        {
            super(prev);
            this.notify = notify;
        }

        void doNotify(SafeCommandStore safeStore, RoutingKey key, NotifySink notifySink)
        {
            SafeCommandsForKey safeCfk = safeStore.get(key);
            CommandsForKey cfk = safeCfk.current();
            List<Unmanaged> addUnmanageds = new ArrayList<>();
            List<PostProcess> nestedNotify = new ArrayList<>();
            for (TxnId txnId : notify)
            {
                SafeCommand safeCommand = safeStore.ifLoadedAndInitialised(txnId);
                if (safeCommand != null && safeStore.tryRecurse())
                {
                    try
                    {
                        CommandsForKeyUpdate update = updateUnmanaged(cfk, safeStore, safeCommand, UPDATE, addUnmanageds);
                        if (update != cfk)
                        {
                            Invariants.require(update.cfk() == cfk);
                            nestedNotify.add(update.postProcess());
                        }
                    }
                    finally
                    {
                        safeStore.unrecurse();
                    }
                }
                else
                {
                    updateUnmanagedAsync(safeStore.commandStore(), txnId, key, notifySink);
                }
            }

            if (!addUnmanageds.isEmpty())
            {
                CommandsForKey cur = safeCfk.current();
                addUnmanageds.sort(Unmanaged::compareTo);
                Unmanaged[] newUnmanageds = addUnmanageds.toArray(new Unmanaged[0]);
                newUnmanageds = SortedArrays.linearUnion(cur.unmanageds, 0, cur.unmanageds.length, newUnmanageds, 0, newUnmanageds.length, Unmanaged::compareTo, ArrayBuffers.uncached(Unmanaged[]::new));
                safeCfk.set(cur.update(newUnmanageds));
            }

            for (PostProcess postProcess : nestedNotify)
                postProcess.postProcess(safeStore, key, notifySink);
        }
    }


    static class NotifyUnmanagedResult
    {
        final Unmanaged[] newUnmanaged;
        final PostProcess postProcess;

        NotifyUnmanagedResult(Unmanaged[] newUnmanaged, PostProcess postProcess)
        {
            this.newUnmanaged = newUnmanaged;
            this.postProcess = postProcess;
        }
    }

    static NotifyUnmanagedResult notifyUnmanaged(Unmanaged[] unmanageds,
                                                 TxnInfo[] byId,
                                                 int minUndecidedById,
                                                 TxnInfo[] committedByExecuteAt,
                                                 int maxAppliedWriteByExecuteAt,
                                                 Object[] loadingPruned,
                                                 QuickBounds bounds,
                                                 boolean isNewBoundsInfo,
                                                 @Nullable TxnInfo curInfo,
                                                 @Nullable TxnInfo newInfo)
    {
        // TODO (expected): can we relax this to shardRedundantBefore?
        TxnId redundantBefore = bounds.cleanCfkBefore();
        TxnId readyAt = bounds.readyAt;
        if (readyAt.compareTo(redundantBefore) <= 0) readyAt = null;

        PostProcess notifier = null;
        {
            // notify commit uses exclusive bounds, as we use minUndecided
            Timestamp minUndecided = minUndecidedById < 0 ? Timestamp.MAX : byId[minUndecidedById];
            if (!BTree.isEmpty(loadingPruned))
            {
                TxnId minLoadingPruned = readyAt == null ? BTree.findByIndex(loadingPruned, 0)
                                                                : BTree.ceil(loadingPruned, TxnId::compareTo, readyAt);

                minUndecided = Timestamp.nonNullOrMin(minUndecided, minLoadingPruned);
            }
            int end = findCommit(unmanageds, minUndecided);
            if (end > 0)
            {
                TxnId[] notifyUnmanaged = new TxnId[end];
                for (int i = 0 ; i < end ; ++i)
                    notifyUnmanaged[i] = unmanageds[i].txnId;

                unmanageds = Arrays.copyOfRange(unmanageds, end, unmanageds.length);
                notifier = new PostProcess.NotifyUnmanagedOfCommit(null, notifyUnmanaged);
            }
        }

        {
            if (newInfo != null && newInfo.is(APPLIED))
            {
                TxnInfo maxContiguousApplied = CommandsForKey.maxContiguousManagedApplied(committedByExecuteAt, maxAppliedWriteByExecuteAt, readyAt);
                if (maxContiguousApplied != null && maxContiguousApplied.compareExecuteAt(newInfo) >= 0)
                {
                    Timestamp applyTo = maxContiguousApplied.executeAt;
                    int start = findFirstApply(unmanageds);
                    int end = findApply(unmanageds, start, applyTo);
                    if (start != end)
                    {
                        TxnId[] notifyNotWaiting = selectUnmanaged(unmanageds, start, end);
                        unmanageds = removeUnmanaged(unmanageds, start, end);
                        notifier = new PostProcess.NotifyNotWaiting(notifier, notifyNotWaiting);
                    }
                }
            }
            else if (newInfo == null && isNewBoundsInfo)
            {
                int firstApply = findFirstApply(unmanageds);
                {   // process unmanaged waiting on applies that may now not occur
                    int start = firstApply;
                    while (start > 0 && unmanageds[start - 1].waitingUntil.epoch() >= bounds.endEpoch)
                        --start;

                    if (start != firstApply)
                    {
                        // TODO (desired): we can recompute the waitingUntil here, instead of relying on notify unmanaged to do it for us
                        //  however this should be rare, and probably fine to rely on renotifying
                        TxnId[] notifyOfCommit = selectUnmanaged(unmanageds, start, firstApply);
                        unmanageds = removeUnmanaged(unmanageds, start, firstApply);
                        notifier = new PostProcess.NotifyUnmanagedOfCommit(notifier, notifyOfCommit);
                        firstApply = start;
                    }
                }
                {   // process unmanaged waiting on applies that may now not occur
                    int start = firstApply;
                    int end = start;
                    int j = 1 + maxContiguousManagedAppliedIndex(committedByExecuteAt, maxAppliedWriteByExecuteAt, readyAt);
                    boolean hasUnapplied = false;
                    while (end < unmanageds.length && j < committedByExecuteAt.length)
                    {
                        hasUnapplied |= committedByExecuteAt[j].compareTo(APPLIED) < 0;
                        int c = committedByExecuteAt[j].executeAt.compareTo(unmanageds[end].waitingUntil);
                        if (c == 0)
                        {
                            if (start != end)
                            {
                                TxnId[] notify = selectUnmanaged(unmanageds, start, end);
                                unmanageds = removeUnmanaged(unmanageds, start, end);
                                end = start;
                                notifier = hasUnapplied ? new PostProcess.NotifyUnmanagedOfCommit(notifier, notify)
                                                        : new PostProcess.NotifyNotWaiting(notifier, notify);
                            }
                            start = ++end;
                        }
                        else if (c < 0)
                        {
                            ++j;
                        }
                        else
                        {
                            ++end;
                        }
                    }
                    if (start != unmanageds.length)
                    {
                        TxnId[] notify = selectUnmanaged(unmanageds, start, unmanageds.length);
                        unmanageds = removeUnmanaged(unmanageds, start, unmanageds.length);
                        notifier = hasUnapplied ? new PostProcess.NotifyUnmanagedOfCommit(notifier, notify)
                                                : new PostProcess.NotifyNotWaiting(notifier, notify);
                    }
                }
            }
        }

        Predicate<Timestamp> rescheduleOrNotifyIf  = null;
        if ((newInfo != null && newInfo.isAtLeast(INVALIDATED) && curInfo != null && curInfo.isCommittedToExecute()))
        {
            rescheduleOrNotifyIf = curInfo.executeAt::equals;
        }

        if (isNewBoundsInfo && readyAt != null)
        {
            Timestamp maxUnready;
            {
                Timestamp tmp = readyAt;
                for (TxnInfo txn : byId)
                {
                    if (txn.compareTo(readyAt) > 0)
                        break;
                    // while we can in principle exclude all transactions with a lower txnId regardless of their executeAt
                    // for consistent handling with other transactions we don't leap ahead by executeAt as this permits
                    // us to also exclude transactions with a higher txnId which is not consistent with other validity checks
                    // which don't have this additional context
                    if (txn.executeAt.compareTo(readyAt) > 0)
                        continue;
                    tmp = Timestamp.nonNullOrMax(tmp, txn.executeAt);
                }
                maxUnready = tmp;
            }
            if (rescheduleOrNotifyIf == null)
                rescheduleOrNotifyIf = test -> test.compareTo(maxUnready) <= 0;
            else
                rescheduleOrNotifyIf = test -> curInfo.executeAt.equals(test) || test.compareTo(maxUnready) <= 0;
        }

        if (rescheduleOrNotifyIf != null)
        {
            // this is a rare edge case, but we might have unmanaged transactions waiting on this command we must re-schedule or notify
            boolean clone = true;
            int start = findFirstApply(unmanageds);
            while (start < unmanageds.length)
            {
                if (rescheduleOrNotifyIf.test(unmanageds[start].waitingUntil))
                {
                    int end = start + 1;
                    while (end < unmanageds.length && rescheduleOrNotifyIf.test(unmanageds[end].waitingUntil))
                        ++end;

                    // find committed predecessor, if any
                    int predecessor = SortedArrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, unmanageds[start].waitingUntil, (t, i) -> t.compareTo(i.executeAt), FAST);
                    if (predecessor < 0) predecessor = -2 - predecessor;
                    else predecessor = predecessor - 1;

                    while (predecessor >= 0 && rescheduleOrNotifyIf.test(committedByExecuteAt[predecessor].executeAt))
                        --predecessor;

                    if (predecessor >= 0)
                    {
                        int maxContiguousApplied = maxContiguousManagedAppliedIndex(committedByExecuteAt, maxAppliedWriteByExecuteAt, readyAt);
                        if (maxContiguousApplied >= predecessor)
                            predecessor = -1;
                    }

                    if (predecessor >= 0)
                    {
                        Timestamp waitingUntil = committedByExecuteAt[predecessor].plainExecuteAt();
                        if (clone) unmanageds = unmanageds.clone();
                        clone = false;
                        for (int i = start ; i < end ; ++i)
                            unmanageds[i] = new Unmanaged(APPLY, unmanageds[i].txnId, waitingUntil);
                    }
                    else
                    {
                        TxnId[] notifyNotWaiting = selectUnmanaged(unmanageds, start, end);
                        unmanageds = removeUnmanaged(unmanageds, start, end);
                        notifier = new PostProcess.NotifyNotWaiting(notifier, notifyNotWaiting);
                        clone = false;
                    }
                    start = end - 1;
                }
                ++start;
            }
        }

        if (notifier == null)
            return null;

        return new NotifyUnmanagedResult(unmanageds, notifier);
    }

}
