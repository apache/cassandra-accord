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

import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.local.SafeState;
import accord.local.Command;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.NotifySink.DefaultNotifySink;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Status.Durability;
import accord.primitives.TxnId;
import accord.utils.Invariants;

public abstract class SafeCommandsForKey extends SafeState<CommandsForKey>
{
    public static class RecordingNotifySink implements NotifySink
    {
        TxnId[] notified = new TxnId[16];
        long[] notifiedAt = new long[16];
        int notifiedCounter = 0;

        TxnId[] applied = new TxnId[16];
        CommandsForKey[] atApplied = new CommandsForKey[16];
        int appliedCounter = 0;

        public void postApplied(TxnId txnId, CommandsForKey cfk)
        {
            appliedCounter = (appliedCounter + 1) & 15;
            applied[appliedCounter] = txnId;
            atApplied[appliedCounter] = cfk;
        }

        @Override
        public void notWaiting(SafeCommandStore safeStore, TxnId txnId, RoutingKey key, long uniqueHlc)
        {
            DefaultNotifySink.INSTANCE.notWaiting(safeStore, txnId, key, uniqueHlc);
            notifiedCounter = (notifiedCounter + 1) & 15;
            notified[notifiedCounter] = txnId;
            notifiedAt[notifiedCounter] = safeStore.node().now();
        }

        @Override
        public void waitingOn(SafeCommandStore safeStore, CommandsForKey.TxnInfo txn, RoutingKey key, SaveStatus waitingOnStatus, ProgressLog.BlockedUntil blockedUntil, boolean notifyCfk)
        {
            DefaultNotifySink.INSTANCE.waitingOn(safeStore, txn, key, waitingOnStatus, blockedUntil, notifyCfk);
        }
    }

    public final RoutingKey key;
    public SafeCommandsForKey(RoutingKey key)
    {
        this.key = key;
    }

    public final RoutingKey key()
    {
        return key;
    }

    public abstract void overrideSink(NotifySink overrideSink);
    public abstract NotifySink overrideSink();

    private NotifySink defaultSink()
    {
        NotifySink overrideSink = overrideSink();
        return overrideSink == null ? DefaultNotifySink.INSTANCE : overrideSink;
    }

    public void updateUniqueHlc(SafeCommandStore safeStore, long uniqueHlc)
    {
        CommandsForKey prevCfk = current();
        update(safeStore, null, prevCfk, prevCfk.updateUniqueHlc(uniqueHlc), false);
    }

    // equivalent to update, but for async callbacks with additional validation around pruning
    public void callback(SafeCommandStore safeStore, Command nextCommand, boolean forceNotify)
    {
        callback(safeStore, nextCommand, defaultSink(), forceNotify);
    }

    public void callback(SafeCommandStore safeStore, Command nextCommand, NotifySink notifySink, boolean forceNotify)
    {
        CommandsForKey prevCfk = current();
        update(safeStore, nextCommand, prevCfk, prevCfk.callback(safeStore, nextCommand), notifySink, forceNotify);
    }

    private void update(SafeCommandStore safeStore, @Nullable Command command, CommandsForKey prevCfk, CommandsForKeyUpdate updateCfk, boolean forceNotify)
    {
        update(safeStore, command, prevCfk, updateCfk, defaultSink(), forceNotify);
    }

    private void update(SafeCommandStore safeStore, @Nullable Command command, CommandsForKey prevCfk, CommandsForKeyUpdate updateCfk, NotifySink notifySink, boolean forceNotify)
    {
        if (updateCfk == prevCfk && !forceNotify)
            return;

        CommandsForKey nextCfk = updateCfk.cfk();
        if (nextCfk != prevCfk)
        {
            if (command != null && command.hasBeen(Status.Applied))
            {
                Agent agent = safeStore.agent();
                nextCfk = nextCfk.maybePrune(agent.cfkPruneInterval(), agent.cfkHlcPruneDelta());
            }
            set(nextCfk);
        }

        updateCfk.postProcess(safeStore, prevCfk, command, notifySink, forceNotify);
    }

    public void registerUnmanaged(SafeCommandStore safeStore, SafeCommand unmanaged, UpdateUnmanagedMode mode)
    {
        CommandsForKey prevCfk = current();
        update(safeStore, null, prevCfk, prevCfk.registerUnmanaged(safeStore, unmanaged, mode), false);
    }

    public void updateRedundantBefore(SafeCommandStore safeStore, RedundantBefore.Bounds redundantBefore)
    {
        CommandsForKey prevCfk = current();
        update(safeStore, null, prevCfk, prevCfk.withBoundsAtLeast(redundantBefore, true), false);
    }

    public void initialize()
    {
        Invariants.require(isUninitialised());
        current = new CommandsForKey(key);
    }

    public void refresh(SafeCommandStore safeStore)
    {
        updateRedundantBefore(safeStore, safeStore.redundantBefore().get(key));
    }

    public void setDurable(TxnId txnId, Durability durability)
    {
        set(current().setDurable(txnId, durability));
    }

    @Override
    protected final boolean hasChanged(CommandsForKey original, CommandsForKey updated)
    {
        if (original == null)
            return !updated.isEmpty();

        return original != updated && updated.hasChanges(original);
    }
}
