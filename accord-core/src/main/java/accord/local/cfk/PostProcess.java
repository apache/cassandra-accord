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
import static accord.local.cfk.CommandsForKey.Unmanaged.Pending.APPLY;
import static accord.local.cfk.Updating.updateUnmanaged;
import static accord.local.cfk.Updating.updateUnmanagedAsync;
import static accord.local.cfk.Utils.findApply;
import static accord.local.cfk.Utils.findCommit;
import static accord.local.cfk.Utils.findFirstApply;
import static accord.local.cfk.Utils.removeUnmanaged;
import static accord.local.cfk.Utils.selectUnmanaged;

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
            return new CommandsForKeyUpdate.CommandsForKeyUpdateWithNotifier(result.cfk(), new LoadPruned(result.postProcess(), txnIds));
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
            List<CommandsForKey.Unmanaged> addUnmanageds = new ArrayList<>();
            for (TxnId txnId : notify)
            {
                SafeCommand safeCommand = safeStore.ifLoadedAndInitialised(txnId);
                if (safeCommand != null)
                {
                    Invariants.checkState(cfk == updateUnmanaged(cfk, safeStore, safeCommand, notifySink, false, addUnmanageds));
                }
                else
                {
                    updateUnmanagedAsync(safeStore.commandStore(), txnId, key, notifySink);
                }
            }

            if (!addUnmanageds.isEmpty())
            {
                CommandsForKey cur = safeCfk.current();
                addUnmanageds.sort(CommandsForKey.Unmanaged::compareTo);
                CommandsForKey.Unmanaged[] newUnmanageds = addUnmanageds.toArray(new CommandsForKey.Unmanaged[0]);
                newUnmanageds = SortedArrays.linearUnion(cur.unmanageds, 0, cur.unmanageds.length, newUnmanageds, 0, newUnmanageds.length, CommandsForKey.Unmanaged::compareTo, ArrayBuffers.uncached(CommandsForKey.Unmanaged[]::new));
                safeCfk.set(cur.update(newUnmanageds));
            }
        }
    }

    static CommandsForKeyUpdate notifyUnmanaged(CommandsForKey cfk, @Nullable TxnInfo curInfo, TxnInfo newInfo)
    {
        PostProcess notifier = null;
        CommandsForKey.Unmanaged[] unmanageds = cfk.unmanageds;
        {
            // notify commit uses exclusive bounds, as we use minUndecided
            Timestamp minUndecided = cfk.minUndecidedById < 0 ? Timestamp.MAX : cfk.byId[cfk.minUndecidedById];
            if (!BTree.isEmpty(cfk.loadingPruned)) minUndecided = Timestamp.min(minUndecided, BTree.<Pruning.LoadingPruned>findByIndex(cfk.loadingPruned, 0));
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

        if (newInfo.status.compareTo(APPLIED) >= 0)
        {
            TxnInfo maxContiguousApplied = cfk.maxContiguousManagedApplied();
            if (maxContiguousApplied != null && maxContiguousApplied.compareExecuteAt(newInfo) < 0)
                maxContiguousApplied = null;

            if (maxContiguousApplied != null)
            {
                int start = findFirstApply(unmanageds);
                int end = findApply(unmanageds, start, maxContiguousApplied.executeAt);
                if (start != end)
                {
                    TxnId[] notifyNotWaiting = selectUnmanaged(unmanageds, start, end);
                    unmanageds = removeUnmanaged(unmanageds, start, end);
                    notifier = new PostProcess.NotifyNotWaiting(notifier, notifyNotWaiting);
                }
            }
        }
        if (newInfo.status == INVALID_OR_TRUNCATED_OR_UNMANAGED_COMMITTED && curInfo != null && curInfo.status.isCommitted())
        {
            TxnInfo[] committedByExecuteAt = cfk.committedByExecuteAt;
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
                    int maxContiguousApplied = cfk.maxContiguousManagedAppliedIndex();
                    if (maxContiguousApplied >= predecessor)
                        predecessor = -1;
                }

                if (predecessor >= 0)
                {
                    Timestamp waitingUntil = committedByExecuteAt[predecessor].plainExecuteAt();
                    unmanageds = unmanageds.clone();
                    for (int i = start ; i < end ; ++i)
                        unmanageds[i] = new CommandsForKey.Unmanaged(APPLY, unmanageds[i].txnId, waitingUntil);
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
            return cfk;

        return new CommandsForKeyUpdate.CommandsForKeyUpdateWithNotifier(new CommandsForKey(cfk, cfk.loadingPruned, unmanageds), notifier);
    }

}
