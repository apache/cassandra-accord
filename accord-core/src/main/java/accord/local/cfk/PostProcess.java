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

import javax.annotation.Nullable;

import accord.api.Key;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.CommandsForKey.Unmanaged;
import accord.local.cfk.CommandsForKeyUpdate.CommandsForKeyUpdateWithPostProcess;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.btree.BTree;

import static accord.local.KeyHistory.COMMANDS;
import static accord.local.cfk.CommandsForKey.InternalStatus.APPLIED;
import static accord.local.cfk.CommandsForKey.InternalStatus.INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED;
import static accord.local.cfk.CommandsForKey.InternalStatus.STABLE;
import static accord.local.cfk.CommandsForKey.Unmanaged.Pending.APPLY;
import static accord.local.cfk.CommandsForKey.maxContiguousManagedAppliedIndex;
import static accord.local.cfk.Updating.updateUnmanaged;
import static accord.local.cfk.Updating.updateUnmanagedAsync;
import static accord.local.cfk.Utils.findApply;
import static accord.local.cfk.Utils.findCommit;
import static accord.local.cfk.Utils.findFirstApply;
import static accord.local.cfk.Utils.removeUnmanaged;
import static accord.local.cfk.Utils.selectUnmanaged;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.ArrayBuffers.cachedTxnIds;

abstract class PostProcess
{
    final PostProcess prev;

    protected PostProcess(PostProcess prev)
    {
        this.prev = prev;
    }

    void postProcess(SafeCommandStore safeStore, Key key, NotifySink notifySink)
    {
        doNotify(safeStore, key, notifySink);
        if (prev != null)
            prev.postProcess(safeStore, key, notifySink);
    }

    abstract void doNotify(SafeCommandStore safeStore, Key key, NotifySink notifySink);

    static class LoadPruned extends PostProcess
    {
        final TxnId[] load;

        LoadPruned(PostProcess prev, TxnId[] load)
        {
            super(prev);
            this.load = load;
        }

        void doNotify(SafeCommandStore safeStore, Key key, NotifySink notifySink)
        {
            SafeCommandsForKey safeCfk = safeStore.get(key);
            for (TxnId txnId : load)
            {
                safeStore = safeStore; // make it unsafe for use in lambda
                SafeCommand safeCommand = safeStore.ifLoadedAndInitialised(txnId);
                if (safeCommand != null) load(safeStore, safeCommand, safeCfk, notifySink);
                else
                    safeStore.commandStore().execute(PreLoadContext.contextFor(txnId, Keys.of(key), COMMANDS), safeStore0 -> {
                        load(safeStore0, safeStore0.unsafeGet(txnId), safeStore0.get(key), notifySink);
                    }).begin(safeStore.agent());
            }
        }

        static void load(SafeCommandStore safeStore, SafeCommand safeCommand, SafeCommandsForKey safeCfk, NotifySink notifySink)
        {
            safeCfk.updatePruned(safeStore, safeCommand.current(), notifySink);
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

        void doNotify(SafeCommandStore safeStore, Key key, NotifySink notifySink)
        {
            for (TxnId txnId : notify)
                notifySink.notWaiting(safeStore, txnId, key);
        }
    }

    /**
     * Anything that is pre-bootstrap can execute immediately, as
     * logically it will be included in the snapshot we materialise during bootstrap.
     *
     * In practice this means that transactions which include a bootstrap range and a range not covered by bootstrap
     * will not wait for the bootstrapping key.
     */
    static CommandsForKeyUpdate notifyPreBootstrap(CommandsForKeyUpdate update)
    {
        CommandsForKey cfk = update.cfk();
        TxnId[] notify = NO_TXNIDS;
        int notifyCount = 0;
        // <= because maxAppliedWrite is actually maxAppliedOrPreBootstrapWrite
        for (int i = 0 ; i <= cfk.maxAppliedWriteByExecuteAt ; ++i)
        {
            TxnInfo txn = cfk.committedByExecuteAt[i];
            if (txn.status == STABLE)
            {
                if (notifyCount == notify.length)
                    notify = cachedTxnIds().get(Math.max(notifyCount * 2, 8));
                notify[notifyCount++] = txn.plainTxnId();
            }
        }

        if (notifyCount == 0)
            return update;

        notify = cachedTxnIds().completeAndDiscard(notify, notifyCount);
        PostProcess newPostProcess = new NotifyNotWaiting(update.postProcess(), notify);
        return new CommandsForKeyUpdateWithPostProcess(cfk, newPostProcess);
    }

    static class NotifyUnmanagedOfCommit extends PostProcess
    {
        final TxnId[] notify;

        NotifyUnmanagedOfCommit(PostProcess prev, TxnId[] notify)
        {
            super(prev);
            this.notify = notify;
        }

        void doNotify(SafeCommandStore safeStore, Key key, NotifySink notifySink)
        {
            SafeCommandsForKey safeCfk = safeStore.get(key);
            CommandsForKey cfk = safeCfk.current();
            List<Unmanaged> addUnmanageds = new ArrayList<>();
            List<PostProcess> nestedNotify = new ArrayList<>();
            for (TxnId txnId : notify)
            {
                SafeCommand safeCommand = safeStore.ifLoadedAndInitialised(txnId);
                if (safeCommand != null)
                {
                    CommandsForKeyUpdate update = updateUnmanaged(cfk, safeCommand, false, addUnmanageds);
                    if (update != cfk)
                    {
                        Invariants.checkState(update.cfk() == cfk);
                        nestedNotify.add(update.postProcess());
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
                                                 TxnId redundantBefore,
                                                 TxnId bootstrappedAt,
                                                 @Nullable TxnInfo curInfo,
                                                 @Nullable TxnInfo newInfo)
    {
        PostProcess notifier = null;
        {
            // notify commit uses exclusive bounds, as we use minUndecided
            Timestamp minUndecided = minUndecidedById < 0 ? Timestamp.MAX : byId[minUndecidedById];
            if (!BTree.isEmpty(loadingPruned)) minUndecided = Timestamp.min(minUndecided, BTree.<Pruning.LoadingPruned>findByIndex(loadingPruned, 0));
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
            Timestamp applyTo = null;
            if (newInfo != null && newInfo.status == APPLIED)
            {
                TxnInfo maxContiguousApplied = CommandsForKey.maxContiguousManagedApplied(committedByExecuteAt, maxAppliedWriteByExecuteAt, bootstrappedAt);
                if (maxContiguousApplied != null && maxContiguousApplied.compareExecuteAt(newInfo) >= 0)
                    applyTo = maxContiguousApplied.executeAt;
            }
            else if (newInfo == null)
            {
//                applyTo = maxContiguousManagedAppliedExecuteAt(committedByExecuteAt, maxAppliedWriteByExecuteAt, bootstrappedAt, redundantBefore);
                TxnInfo maxContiguousApplied = CommandsForKey.maxContiguousManagedApplied(committedByExecuteAt, maxAppliedWriteByExecuteAt, bootstrappedAt);
                if (maxContiguousApplied != null)
                    applyTo = maxContiguousApplied.executeAt;
                // if we're updating bootstrappedAt, we can fire anyone waiting on an executeAt before the bootstrappedAt
                applyTo = Timestamp.nonNullOrMax(applyTo, TxnId.nonNullOrMax(redundantBefore, bootstrappedAt));
            }

            if (applyTo != null)
            {
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

        if (newInfo != null && newInfo.status == INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED && curInfo != null && curInfo.status.isCommitted())
        {
            // this is a rare edge case, but we might have unmanaged transactions waiting on this command we must re-schedule or notify
            int start = findFirstApply(unmanageds);
            int end = start;
            while (end < unmanageds.length && unmanageds[end].waitingUntil.equals(curInfo.executeAt))
                ++end;

            if (start != end)
            {
                // find committed predecessor, if any
                int predecessor = -2 - Arrays.binarySearch(committedByExecuteAt, curInfo, TxnInfo::compareExecuteAt);

                if (predecessor >= 0)
                {
                    int maxContiguousApplied = maxContiguousManagedAppliedIndex(committedByExecuteAt, maxAppliedWriteByExecuteAt, bootstrappedAt);
                    if (maxContiguousApplied >= predecessor)
                        predecessor = -1;
                }

                if (predecessor >= 0)
                {
                    Timestamp waitingUntil = committedByExecuteAt[predecessor].plainExecuteAt();
                    unmanageds = unmanageds.clone();
                    for (int i = start ; i < end ; ++i)
                        unmanageds[i] = new Unmanaged(APPLY, unmanageds[i].txnId, waitingUntil);
                }
                else
                {
                    TxnId[] notifyNotWaiting = selectUnmanaged(unmanageds, start, end);
                    unmanageds = removeUnmanaged(unmanageds, start, end);
                    notifier = new PostProcess.NotifyNotWaiting(notifier, notifyNotWaiting);
                }
            }
        }

        if (notifier == null)
            return null;

        return new NotifyUnmanagedResult(unmanageds, notifier);
    }

}
