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

package accord.local;

import accord.api.Key;
import accord.impl.SafeState;
import accord.primitives.TxnId;

import static accord.local.CommandsForKey.InternalStatus.HISTORICAL;

public abstract class SafeCommandsForKey implements SafeState<CommandsForKey>
{
    private final Key key;

    public SafeCommandsForKey(Key key)
    {
        this.key = key;
    }

    protected abstract void set(CommandsForKey update);

    public Key key()
    {
        return key;
    }

    CommandsForKey update(SafeCommandStore safeStore, Command prevCommand, Command nextCommand)
    {
        CommandsForKey prevCfk = current();
        CommandsForKey nextCfk = prevCfk.update(prevCommand, nextCommand);
        if (nextCfk == prevCfk)
            return prevCfk;
        set(nextCfk.notifyAndUpdatePending(safeStore, nextCommand, prevCfk));
        return nextCfk;
    }

    CommandsForKey update(SafeCommandStore safeStore, Command nextCommand)
    {
        CommandsForKey prevCfk = current();
        CommandsForKey nextCfk = prevCfk.update(null, nextCommand, false);
        if (nextCfk == prevCfk)
            return prevCfk;
        set(nextCfk.notifyAndUpdatePending(safeStore, nextCommand, prevCfk));
        return nextCfk;
    }

    CommandsForKey registerUnmanaged(SafeCommandStore safeStore, SafeCommand unmanaged)
    {
        CommandsForKey prev = current();
        CommandsForKey next = prev.registerUnmanaged(safeStore, unmanaged);
        if (next != prev)
            set(next.notifyAndUpdatePending(safeStore, prev));
        return next;
    }

    public CommandsForKey registerHistorical(SafeCommandStore safeStore, TxnId txnId)
    {
        CommandsForKey prev = current();
        CommandsForKey next = prev.registerHistorical(txnId);
        if (next != prev)
            set(next.notifyAndUpdatePending(safeStore, txnId, HISTORICAL, txnId, prev));
        return next;
    }

    public CommandsForKey updateRedundantBefore(SafeCommandStore safeStore, RedundantBefore.Entry redundantBefore)
    {
        CommandsForKey prev = current();
        CommandsForKey next = prev.withRedundantBefore(redundantBefore);
        if (next != prev)
            set(next.notifyAndUpdatePending(safeStore, prev));
        return next;
    }

    public void initialize()
    {
        set(new CommandsForKey(key));
    }

    public void refresh(SafeCommandStore safeStore)
    {
        updateRedundantBefore(safeStore, safeStore.commandStore().redundantBefore().get(key));
    }
}
