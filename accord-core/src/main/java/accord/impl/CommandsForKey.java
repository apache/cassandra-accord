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
import accord.impl.CommandTimeseries.CommandLoader;
import com.google.common.collect.ImmutableSortedMap;

import java.util.Objects;

import accord.local.Command;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

public class CommandsForKey implements DomainCommands
{
    public static class SerializerSupport
    {
        public static Listener listener(Key key)
        {
            return new Listener(key);
        }

        public static  <D> CommandsForKey create(Key key,
                                                 CommandLoader<D> loader,
                                                 ImmutableSortedMap<Timestamp, D> commands)
        {
            return new CommandsForKey(key, loader, commands);
        }
    }

    public static class Listener implements Command.DurableAndIdempotentListener
    {
        protected final Key listenerKey;

        public Listener(Key listenerKey)
        {
            this.listenerKey = listenerKey;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Listener that = (Listener) o;
            return listenerKey.equals(that.listenerKey);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(listenerKey);
        }

        @Override
        public String toString()
        {
            return "ListenerProxy{" + listenerKey + '}';
        }

        public Key key()
        {
            return listenerKey;
        }

        @Override
        public void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            CommandsForKeys.listenerUpdate((AbstractSafeCommandStore<?,?,?,?>) safeStore, listenerKey, safeCommand.current());
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return PreLoadContext.contextFor(caller, Keys.of(listenerKey));
        }
    }

    private final Key key;
    private final CommandTimeseries<?> commands;

    <D> CommandsForKey(Key key,
                       CommandTimeseries<D> commands)
    {
        this.key = key;
        this.commands = commands;
    }

    <D> CommandsForKey(Key key, CommandLoader<D> loader, ImmutableSortedMap<Timestamp, D> commands)
    {
        this(key, new CommandTimeseries<>(key, loader, commands));
    }

    public <D> CommandsForKey(Key key, CommandLoader<D> loader)
    {
        this.key = key;
        this.commands = new CommandTimeseries<>(key, loader);
    }

    @Override
    public String toString()
    {
        return "CommandsForKey@" + System.identityHashCode(this) + '{' + key + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandsForKey that = (CommandsForKey) o;
        return key.equals(that.key) && commands.equals(that.commands);
    }

    @Override
    public final int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    public Key key()
    {
        return key;
    }

    @Override
    public CommandTimeseries<?> commands()
    {
        return commands;
    }

    public <D> CommandsForKey update(CommandTimeseries.Update<TxnId, D> commands)
    {
        if (commands.isEmpty())
            return this;

        return new CommandsForKey(key, commands.apply((CommandTimeseries<D>) this.commands));
    }

    public boolean hasRedundant(TxnId redundantBefore)
    {
        return commands.minTimestamp().compareTo(redundantBefore) < 0;
    }

    public CommandsForKey withoutRedundant(TxnId redundantBefore)
    {
        return new CommandsForKey(key, (CommandTimeseries) commands.unbuild().removeBefore(redundantBefore).build());
    }

    public interface Update {}

}
