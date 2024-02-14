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

import accord.api.Key;
import accord.local.Command;
import accord.primitives.TxnId;

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

    CommandsForKey update(CommandsForKey update)
    {
        set(update);
        return update;
    }

    CommandsForKey update(Command prev, Command update)
    {
        CommandsForKey current = current();
        CommandsForKey next = current.update(prev, update);
        if (next != current)
            set(next);
        return next;
    }

    CommandsForKey registerHistorical(TxnId txnId)
    {
        CommandsForKey current = current();
        CommandsForKey next = current.registerHistorical(txnId);
        if (next != current)
            set(next);
        return next;
    }

    CommandsForKey updateRedundantBefore(TxnId redundantBefore)
    {
        CommandsForKey current = current();
        CommandsForKey next = current.withoutRedundant(redundantBefore);
        if (next != current)
            set(next);
        return next;
    }

    public void initialize()
    {
        set(new CommandsForKey(key));
    }
}
