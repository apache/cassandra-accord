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

import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.local.*;
import accord.local.cfk.SafeCommandsForKey;
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

    private static <K, V> V getIfLoadedUnsafe(K key, Function<K, V> get, Consumer<V> add, Function<K, V> getIfLoaded)
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

    protected abstract CommandType getCommandUnsafe(TxnId txnId);
    protected abstract void addCommandUnsafe(CommandType command);
    protected abstract CommandType getIfLoadedUnsafe(TxnId txnId);

    protected abstract TimestampsForKeyType getTimestampsForKeyUnsafe(RoutingKey key);
    protected abstract void addTimestampsForKeyUnsafe(TimestampsForKeyType cfk);
    protected abstract TimestampsForKeyType getTimestampsForKeyIfUnsafe(RoutingKey key);

    protected abstract CommandsForKeyType getCommandsForKeyUnsafe(RoutingKey key);
    protected abstract void addCommandsForKeyUnsafe(CommandsForKeyType cfk);
    protected abstract CommandsForKeyType getCommandsForKeyIfUnsafe(RoutingKey key);

    @Override
    protected CommandType ifLoadedAndInitialisedAndNotErasedInternal(TxnId txnId)
    {
        CommandType command = getIfLoadedUnsafe(txnId, this::getCommandUnsafe, this::addCommandUnsafe, this::getIfLoadedUnsafe);
        if (command == null || command.isUnset())
            return null;
        return command;
    }

    @Override
    public CommandType getInternal(TxnId txnId)
    {
        return getCommandUnsafe(txnId);
    }

    private CommandsForKeyType getCommandsIfLoadedUnsafe(RoutingKey key)
    {
        return getIfLoadedUnsafe(key, this::getCommandsForKeyUnsafe, this::addCommandsForKeyUnsafe, this::getCommandsForKeyIfUnsafe);
    }

    protected CommandsForKeyType ifLoadedInternal(RoutingKey key)
    {
        return getCommandsIfLoadedUnsafe(key);
    }

    @VisibleForTesting
    protected CommandsForKeyType getInternal(RoutingKey key)
    {
        return getCommandsIfLoadedUnsafe(key);
    }

    @VisibleForImplementation
    public CommandsForKeyType maybeCommandsForKey(RoutingKey key)
    {
        CommandsForKeyType cfk = getCommandsIfLoadedUnsafe(key);
        if (cfk == null || cfk.isUnset())
            return null;
        return cfk;
    }

    public TimestampsForKeyType timestampsIfLoadedAndInitialised(RoutingKey key)
    {
        TimestampsForKeyType cfk = getIfLoadedUnsafe(key, this::getTimestampsForKeyUnsafe, this::addTimestampsForKeyUnsafe, this::getTimestampsForKeyIfUnsafe);
        if (cfk == null)
            return null;
        if (cfk.isUnset())
        {
            cfk.initialize();
        }
        return cfk;
    }

    public TimestampsForKeyType timestampsForKey(RoutingKey key)
    {
        TimestampsForKeyType tfk = getIfLoadedUnsafe(key, this::getTimestampsForKeyUnsafe, this::addTimestampsForKeyUnsafe, this::getTimestampsForKeyIfUnsafe);
        Invariants.checkState(tfk != null, "%s was not specified in PreLoadContext", key);
        if (tfk.isUnset())
            tfk.initialize();
        return tfk;
    }


    @VisibleForImplementation
    public TimestampsForKeyType maybeTimestampsForKey(RoutingKey key)
    {
        TimestampsForKeyType tfk = getIfLoadedUnsafe(key, this::getTimestampsForKeyUnsafe, this::addTimestampsForKeyUnsafe, this::getTimestampsForKeyIfUnsafe);
        if (tfk == null || tfk.isUnset())
            return null;
        return tfk;
    }

    @Override
    public PreLoadContext context()
    {
        return this.context;
    }

    @Override
    public PreLoadContext canExecute(PreLoadContext context)
    {
        return context.isSubsetOf(this.context) ? context : null;
    }

    public void postExecute()
    {
    }
}
