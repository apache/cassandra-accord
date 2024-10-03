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
import accord.api.RoutingKey;
import accord.impl.SafeState;
import accord.local.Command;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.Status;
import accord.primitives.TxnId;

public abstract class SafeCommandsForKey implements SafeState<CommandsForKey>
{
    private final RoutingKey key;

    public SafeCommandsForKey(RoutingKey key)
    {
        this.key = key;
    }

    protected abstract void set(CommandsForKey update);

    public RoutingKey key()
    {
        return key;
    }

    void updatePruned(SafeCommandStore safeStore, Command nextCommand, NotifySink notifySink)
    {
        CommandsForKey prevCfk = current();
        update(safeStore, nextCommand, prevCfk, prevCfk.updatePruned(nextCommand), notifySink);
    }

    public void update(SafeCommandStore safeStore, Command nextCommand, boolean isOutOfRange)
    {
        CommandsForKey prevCfk = current();
        update(safeStore, nextCommand, prevCfk, prevCfk.update(nextCommand, isOutOfRange));
    }

    public void update(SafeCommandStore safeStore, Command nextCommand)
    {
        CommandsForKey prevCfk = current();
        update(safeStore, nextCommand, prevCfk, prevCfk.update(nextCommand));
    }

    private void update(SafeCommandStore safeStore, @Nullable Command command, CommandsForKey prevCfk, CommandsForKeyUpdate updateCfk)
    {
        update(safeStore, command, prevCfk, updateCfk, NotifySink.DefaultNotifySink.INSTANCE);
    }

    private void update(SafeCommandStore safeStore, @Nullable Command command, CommandsForKey prevCfk, CommandsForKeyUpdate updateCfk, NotifySink notifySink)
    {
        if (updateCfk == prevCfk)
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

        updateCfk.postProcess(safeStore, prevCfk, command, notifySink);
    }

    public void registerUnmanaged(SafeCommandStore safeStore, SafeCommand unmanaged)
    {
        CommandsForKey prevCfk = current();
        update(safeStore, null, prevCfk, prevCfk.registerUnmanaged(unmanaged));
    }

    public void registerHistorical(SafeCommandStore safeStore, TxnId txnId)
    {
        CommandsForKey prevCfk = current();
        update(safeStore, null, prevCfk, prevCfk.registerHistorical(txnId));
    }

    public void updateRedundantBefore(SafeCommandStore safeStore, RedundantBefore.Entry redundantBefore)
    {
        CommandsForKey prevCfk = current();
        update(safeStore, null, prevCfk, prevCfk.withRedundantBeforeAtLeast(redundantBefore));
    }

    public void initialize()
    {
        set(new CommandsForKey(key));
    }

    public void refresh(SafeCommandStore safeStore)
    {
        updateRedundantBefore(safeStore, safeStore.redundantBefore().get(key));
    }
}
