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

package accord.impl;

import java.util.function.Supplier;

import accord.impl.InMemoryCommandStore.GlobalCommand;
import accord.local.Command;
import accord.local.Listeners;
import accord.local.SafeCommand;
import accord.primitives.TxnId;

public class InMemorySafeCommand extends SafeCommand implements SafeState<Command>
{
    private static final Supplier<GlobalCommand> INVALIDATED = () -> null;

    private Supplier<GlobalCommand> lazy;
    private GlobalCommand global;

    public InMemorySafeCommand(TxnId txnId, GlobalCommand global)
    {
        super(txnId);
        this.global = global;
    }

    public InMemorySafeCommand(TxnId txnId, Supplier<GlobalCommand> global)
    {
        super(txnId);
        this.lazy = global;
    }

    @Override
    public Command current()
    {
        touch();
        return global.value();
    }

    @Override
    protected void set(Command update)
    {
        touch();
        global.value(update);
    }

    @Override
    public void addListener(Command.TransientListener listener)
    {
        global.addListener(listener);
    }

    @Override
    public boolean removeListener(Command.TransientListener listener)
    {
        return global.removeListener(listener);
    }

    @Override
    public Listeners<Command.TransientListener> transientListeners()
    {
        return global.transientListeners();
    }

    @Override
    public void invalidate()
    {
        lazy = INVALIDATED;
    }

    @Override
    public boolean invalidated()
    {
        return lazy == INVALIDATED;
    }

    private void touch()
    {
        if (invalidated())
            throw new IllegalStateException("Cannot access invalidated " + this);
        if (lazy != null)
        {
            global = lazy.get();
            lazy = null;
        }
    }

    GlobalCommand global()
    {
        touch();
        return global;
    }
}
