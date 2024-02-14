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

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

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

    protected abstract TimestampsForKeyType getTimestampsForKeyInternal(RoutableKey key);
    protected abstract void addTimestampsForKeyInternal(TimestampsForKeyType cfk);
    protected abstract TimestampsForKeyType getTimestampsForKeyIfLoaded(RoutableKey key);

    protected abstract CommandsForKeyType getCommandsForKeyInternal(RoutableKey key);
    protected abstract void addCommandsForKeyInternal(CommandsForKeyType cfk);
    protected abstract CommandsForKeyType getCommandsForKeyIfLoaded(RoutableKey key);

    @Override
    protected CommandType getInternalIfLoadedAndInitialised(TxnId txnId)
    {
        CommandType command = getIfLoaded(txnId, this::getCommandInternal, this::addCommandInternal, this::getIfLoaded);
        if (command == null || command.isEmpty())
            return null;
        return command;
    }

    @Override
    public CommandType getInternal(TxnId txnId)
    {
        CommandType command = getCommandInternal(txnId);
        if (command == null)
            throw illegalState(format("%s was not specified in PreLoadContext", txnId));
        if (command.isEmpty())
            command.uninitialised();
        return command;
    }

    private CommandsForKeyType getCommandsIfLoaded(RoutableKey key)
    {
        return getIfLoaded(key, this::getCommandsForKeyInternal, this::addCommandsForKeyInternal, this::getCommandsForKeyIfLoaded);
    }

    CommandsForKeyType commandsIfLoadedAndInitialised(RoutableKey key)
    {
        CommandsForKeyType cfk = getCommandsIfLoaded(key);
        if (cfk == null)
            return null;
        if (cfk.isEmpty())
            cfk.initialize();

        RedundantBefore.Entry entry = commandStore().redundantBefore().get(key.toUnseekable());
        if (entry != null)
            cfk.updateRedundantBefore(entry.shardRedundantBefore());
        return cfk;
    }

    @VisibleForTesting
    public CommandsForKeyType commandsForKey(RoutableKey key)
    {
        CommandsForKeyType cfk = getCommandsIfLoaded(key);
        Invariants.checkState(cfk != null, "%s was not specified in PreLoadContext", key);
        if (cfk.isEmpty())
            cfk.initialize();
        return cfk;
    }

    @VisibleForImplementation
    public CommandsForKeyType maybeCommandsForKey(RoutableKey key)
    {
        CommandsForKeyType cfk = getCommandsIfLoaded(key);
        if (cfk == null || cfk.isEmpty())
            return null;
        return cfk;
    }

    public TimestampsForKeyType timestampsIfLoadedAndInitialised(RoutableKey key)
    {
        TimestampsForKeyType cfk = getIfLoaded(key, this::getTimestampsForKeyInternal, this::addTimestampsForKeyInternal, this::getTimestampsForKeyIfLoaded);
        if (cfk == null)
            return null;
        if (cfk.isEmpty())
        {
            cfk.initialize();
        }
        return cfk;
    }

    public TimestampsForKeyType timestampsForKey(RoutableKey key)
    {
        TimestampsForKeyType tfk = getIfLoaded(key, this::getTimestampsForKeyInternal, this::addTimestampsForKeyInternal, this::getTimestampsForKeyIfLoaded);
        Invariants.checkState(tfk != null, "%s was not specified in PreLoadContext", key);
        if (tfk.isEmpty())
            tfk.initialize();
        return tfk;
    }


    @VisibleForImplementation
    public TimestampsForKeyType maybeTimestampsForKey(RoutableKey key)
    {
        TimestampsForKeyType tfk = getIfLoaded(key, this::getTimestampsForKeyInternal, this::addTimestampsForKeyInternal, this::getTimestampsForKeyIfLoaded);
        if (tfk == null || tfk.isEmpty())
            return null;
        return tfk;
    }

    @Override
    protected void update(Command prev, Command updated, @Nullable Seekables<?, ?> keysOrRanges)
    {
        super.update(prev, updated, keysOrRanges);

        if (!CommandsForKey.needsUpdate(prev, updated))
            return;

        TxnId txnId = updated.txnId();
        if (!txnId.kind().isGloballyVisible() || !txnId.domain().isKey())
            return;

        // TODO (required): consider carefully epoch overlaps for dependencies;
        //      here we're limiting our registration with CFK to the coordination epoch only
        //      if we permit coordination+execution we have to do a very careful dance (or relax validation)
        //      because for some keys we can expect e.g. PreAccept and Accept states to have been processed
        //      and for other keys Committed onwards will appear suddenly (or, if we permit Accept to process
        //      on its executeAt ranges, it could go either way).
        Ranges ranges = ranges().allAt(txnId);
        Keys keys;
        if (keysOrRanges != null) keys = (Keys) keysOrRanges;
        else if (updated.known().isDefinitionKnown()) keys = (Keys)updated.partialTxn().keys();
        else if (prev.known().isDefinitionKnown()) keys = (Keys)prev.partialTxn().keys();
        else if (updated.saveStatus().is(Status.Invalidated)) return; // TODO (required): we may have transaction registered via Accept, and still want to expunge. we shouldn't special case: should ensure we have everything loaded, or permit asynchronous application
        else if (updated.saveStatus().is(Status.AcceptedInvalidate)) return; // TODO (required): we may have transaction registered via Accept, and still want to expunge. we shouldn't special case: should ensure we have everything loaded, or permit asynchronous application
        else throw illegalState("No keys to update CommandsForKey with");

        Routables.foldl(keys, ranges, (self, p, key, u, i) -> {
            SafeCommandsForKey cfk = self.commandsIfLoadedAndInitialised(key);
            // TODO (required): we shouldn't special case invalidations or truncations: should ensure we have everything loaded, or permit asynchronous application
            Invariants.checkState(cfk != null || u.saveStatus().hasBeen(Status.Invalidated) || u.saveStatus().is(Status.AcceptedInvalidate) || u.saveStatus().is(Status.Truncated));
            if (cfk != null)
                cfk.update(p, u);
            return u;
        }, this, prev, updated, i->false);
    }

    @Override
    public boolean canExecuteWith(PreLoadContext context)
    {
        // TODO (required): check if data is in cache, and if so simply add it to our context
        return context.isSubsetOf(this.context);
    }

    protected abstract void invalidateSafeState();

    public void postExecute()
    {
    }

    public void complete()
    {
        postExecute();
        invalidateSafeState();
    }
}
