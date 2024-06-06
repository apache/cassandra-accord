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

import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Key;
import accord.api.VisibleForImplementation;
import accord.local.*;
import accord.primitives.*;
import accord.utils.Invariants;

import static accord.utils.Invariants.illegalState;
import static java.lang.String.format;

public abstract class AbstractSafeCommandStore<CommandType extends SafeCommand,
                                               TimestampsForKeyType extends SafeTimestampsForKey,
                                               CommandsForKeyType extends SafeCommandsForKey> extends SafeCommandStore
{
    protected final PreLoadContext context;

    public AbstractSafeCommandStore(PreLoadContext context)
    {
        this.context = context;
    }

    private static <K, V> V getIfLoaded(K key, Function<K, V> get, Consumer<V> add, Function<K, V> getIfLoaded)
    {
        V value = get.apply(key);
        if (value != null)
            return value;

        value = getIfLoaded.apply(key);
        if (value == null)
            return null;
        add.accept(value);
        return value;
    }

    protected abstract CommandType getCommandInternal(TxnId txnId);
    protected abstract void addCommandInternal(CommandType command);
    protected abstract CommandType getIfLoaded(TxnId txnId);

    protected abstract TimestampsForKeyType getTimestampsForKeyInternal(Key key);
    protected abstract void addTimestampsForKeyInternal(TimestampsForKeyType cfk);
    protected abstract TimestampsForKeyType getTimestampsForKeyIfLoaded(Key key);

    protected abstract CommandsForKeyType getCommandsForKeyInternal(Key key);
    protected abstract void addCommandsForKeyInternal(CommandsForKeyType cfk);
    protected abstract CommandsForKeyType getCommandsForKeyIfLoaded(Key key);

    @Override
    protected CommandType getInternalIfLoadedAndInitialised(TxnId txnId)
    {
        CommandType command = getIfLoaded(txnId, this::getCommandInternal, this::addCommandInternal, this::getIfLoaded);
        if (command == null || command.isUnset())
            return null;
        return command;
    }

    @Override
    public CommandType getInternal(TxnId txnId)
    {
        CommandType command = getCommandInternal(txnId);
        if (command == null)
            throw illegalState(format("%s was not specified in PreLoadContext", txnId));
        if (command.isUnset())
            command.uninitialised();
        return command;
    }

    private CommandsForKeyType getCommandsIfLoaded(Key key)
    {
        return getIfLoaded(key, this::getCommandsForKeyInternal, this::addCommandsForKeyInternal, this::getCommandsForKeyIfLoaded);
    }

    protected CommandsForKeyType getInternalIfLoadedAndInitialised(Key key)
    {
        CommandsForKeyType cfk = getCommandsIfLoaded(key);
        if (cfk == null)
            return null;
        if (cfk.isUnset())
            cfk.initialize();
        return cfk;
    }

    @VisibleForTesting
    protected CommandsForKeyType getInternal(Key key)
    {
        CommandsForKeyType cfk = getCommandsIfLoaded(key);
        Invariants.checkState(cfk != null, "%s was not specified in PreLoadContext", key);
        if (cfk.isUnset())
            cfk.initialize();
        return cfk;
    }

    @VisibleForImplementation
    public CommandsForKeyType maybeCommandsForKey(Key key)
    {
        CommandsForKeyType cfk = getCommandsIfLoaded(key);
        if (cfk == null || cfk.isUnset())
            return null;
        return cfk;
    }

    public TimestampsForKeyType timestampsIfLoadedAndInitialised(Key key)
    {
        TimestampsForKeyType cfk = getIfLoaded(key, this::getTimestampsForKeyInternal, this::addTimestampsForKeyInternal, this::getTimestampsForKeyIfLoaded);
        if (cfk == null)
            return null;
        if (cfk.isUnset())
        {
            cfk.initialize();
        }
        return cfk;
    }

    public TimestampsForKeyType timestampsForKey(Key key)
    {
        TimestampsForKeyType tfk = getIfLoaded(key, this::getTimestampsForKeyInternal, this::addTimestampsForKeyInternal, this::getTimestampsForKeyIfLoaded);
        Invariants.checkState(tfk != null, "%s was not specified in PreLoadContext", key);
        if (tfk.isUnset())
            tfk.initialize();
        return tfk;
    }


    @VisibleForImplementation
    public TimestampsForKeyType maybeTimestampsForKey(Key key)
    {
        TimestampsForKeyType tfk = getIfLoaded(key, this::getTimestampsForKeyInternal, this::addTimestampsForKeyInternal, this::getTimestampsForKeyIfLoaded);
        if (tfk == null || tfk.isUnset())
            return null;
        return tfk;
    }

    @Override
    public boolean canExecuteWith(PreLoadContext context)
    {
        // TODO (required): check if data is in cache, and if so simply add it to our context
        return context.isSubsetOf(this.context);
    }

    public void postExecute()
    {
    }
}
