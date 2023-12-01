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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import accord.api.Key;
import accord.api.VisibleForImplementation;
import accord.impl.CommandTimeseries.CommandLoader;
import accord.local.*;
import accord.primitives.*;
import accord.utils.Invariants;

public abstract class AbstractSafeCommandStore<CommandType extends SafeCommand,
                                               TimestampsForKeyType extends SafeTimestampsForKey,
                                               CommandsForKeyType extends SafeCommandsForKey,
                                               CommandsForKeyUpdateType extends SafeCommandsForKey.Update> extends SafeCommandStore
{
    private static class PendingRegistration<T>
    {
        final T value;
        final Ranges slice;
        final TxnId txnId;

        public PendingRegistration(T value, Ranges slice, TxnId txnId)
        {
            this.value = value;
            this.slice = slice;
            this.txnId = txnId;
        }
    }
    protected final PreLoadContext context;

    private List<PendingRegistration<Seekable>> pendingSeekableRegistrations = null;
    private List<PendingRegistration<Seekables<?, ?>>> pendingSeekablesRegistrations = null;

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

    protected abstract CommandLoader<?> cfkLoader(RoutableKey key);
    protected abstract CommandsForKeyType getDepsCommandsForKeyInternal(RoutableKey key);
    protected abstract void addDepsCommandsForKeyInternal(CommandsForKeyType cfk);
    protected abstract CommandsForKeyType getDepsCommandsForKeyIfLoaded(RoutableKey key);

    protected abstract CommandsForKeyType getAllCommandsForKeyInternal(RoutableKey key);
    protected abstract void addAllCommandsForKeyInternal(CommandsForKeyType cfk);
    protected abstract CommandsForKeyType getAllCommandsForKeyIfLoaded(RoutableKey key);

    protected abstract CommandsForKeyUpdateType getCommandsForKeyUpdateInternal(RoutableKey key);
    protected abstract CommandsForKeyUpdateType createCommandsForKeyUpdateInternal(RoutableKey key);
    protected abstract void addCommandsForKeyUpdateInternal(CommandsForKeyUpdateType update);

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
            throw new IllegalStateException(String.format("%s was not specified in PreLoadContext", txnId));
        if (command.isEmpty())
            command.uninitialised();
        return command;
    }

    private CommandsForKeyType getCommandsIfLoaded(RoutableKey key, KeyHistory keyHistory)
    {
        switch (keyHistory)
        {
            case DEPS:
                return getIfLoaded(key, this::getDepsCommandsForKeyInternal, this::addDepsCommandsForKeyInternal, this::getDepsCommandsForKeyIfLoaded);
            case ALL:
                return getIfLoaded(key, this::getAllCommandsForKeyInternal, this::addAllCommandsForKeyInternal, this::getAllCommandsForKeyIfLoaded);
            default:
                throw new IllegalArgumentException("CommandsForKey not available for " + keyHistory);
        }
    }

    private CommandsForKeyType commandsIfLoadedAndInitialised(RoutableKey key, KeyHistory keyHistory)
    {
        CommandsForKeyType cfk = getCommandsIfLoaded(key, keyHistory);;
        if (cfk == null)
            return null;
        if (cfk.isEmpty())
        {
            cfk.initialize(cfkLoader(key));
        }
        else
        {
            RedundantBefore.Entry entry = commandStore().redundantBefore().get(key.toUnseekable());
            if (entry != null && cfk.current().hasRedundant(entry.shardRedundantBefore()))
                cfk.set(cfk.current().withoutRedundant(entry.shardRedundantBefore()));
        }
        return cfk;
    }

    protected CommandsForKeyType commandsForKey(RoutableKey key, KeyHistory keyHistory)
    {
        CommandsForKeyType cfk = getCommandsIfLoaded(key, keyHistory);
        Invariants.checkState(cfk != null, "%s was not specified in PreLoadContext", key);
        if (cfk.isEmpty())
            cfk.initialize(cfkLoader(key));
        return cfk;
    }

    @VisibleForImplementation
    private CommandsForKeyType maybeCommandsForKey(RoutableKey key, KeyHistory keyHistory)
    {
        CommandsForKeyType cfk = getCommandsIfLoaded(key, keyHistory);
        if (cfk == null || cfk.isEmpty())
            return null;
        return cfk;
    }

    public CommandsForKeyType depsCommandsIfLoadedAndInitialised(RoutableKey key)
    {
        return commandsIfLoadedAndInitialised(key, KeyHistory.DEPS);
    }

    public CommandsForKeyType depsCommandsForKey(RoutableKey key)
    {
        return commandsForKey(key, KeyHistory.DEPS);
    }

    @VisibleForImplementation
    public CommandsForKeyType maybeDepsCommandsForKey(RoutableKey key)
    {
        return maybeCommandsForKey(key, KeyHistory.DEPS);
    }

    public CommandsForKeyType allCommandsIfLoadedAndInitialised(RoutableKey key)
    {
        return commandsIfLoadedAndInitialised(key, KeyHistory.ALL);
    }

    public CommandsForKeyType allCommandsForKey(RoutableKey key)
    {
        return commandsForKey(key, KeyHistory.ALL);
    }

    @VisibleForImplementation
    public CommandsForKeyType maybeAllCommandsForKey(RoutableKey key)
    {
        return maybeCommandsForKey(key, KeyHistory.ALL);
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

    public CommandsForKeyUpdateType getOrCreateCommandsForKeyUpdate(RoutableKey key)
    {
        CommandsForKeyUpdateType update = getIfLoaded(key, this::getCommandsForKeyUpdateInternal, this::addCommandsForKeyUpdateInternal, this::createCommandsForKeyUpdateInternal);
        if (update == null)
        {
            update = createCommandsForKeyUpdateInternal(key);
            addCommandsForKeyUpdateInternal(update);
        }

        if (update.isEmpty())
            update.initialize();

        return update;
    }

    @Override
    public void removeCommandFromSeekableDeps(Seekable seekable, TxnId txnId, Timestamp executeAt, Status status)
    {
        // the cfk listener doesn't know if it can remove the given command from the deps set without loading
        // the deps set, so we don't actually remove it until it becomes applied
        if (!status.hasBeen(Status.Applied))
            return;

        switch (seekable.domain())
        {
            case Key:
                Key key = seekable.asKey();
                CommandsForKeyUpdater.Mutable<?> updater = getOrCreateCommandsForKeyUpdate(key).deps();
                updater.commands().remove(txnId);
                break;
            case Range:
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean canExecuteWith(PreLoadContext context)
    {
        return context.isSubsetOf(this.context);
    }

    @Override
    public void register(Seekables<?, ?> keysOrRanges, Ranges slice, Command command)
    {
        if (pendingSeekablesRegistrations == null)
            pendingSeekablesRegistrations = new ArrayList<>();
        pendingSeekablesRegistrations.add(new PendingRegistration<>(keysOrRanges, slice, command.txnId()));
    }

    @Override
    public void register(Seekable keyOrRange, Ranges slice, Command command)
    {
        if (pendingSeekableRegistrations == null)
            pendingSeekableRegistrations = new ArrayList<>();
        pendingSeekableRegistrations.add(new PendingRegistration<>(keyOrRange, slice, command.txnId()));
    }

    public abstract CommonAttributes completeRegistration(Seekables<?, ?> keysOrRanges, Ranges slice, CommandType command, CommonAttributes attrs);

    public abstract CommonAttributes completeRegistration(Seekable keyOrRange, Ranges slice, CommandType command, CommonAttributes attrs);

    private interface RegistrationCompleter<T, CommandType extends SafeCommand>
    {
        CommonAttributes complete(T value, Ranges ranges, CommandType command, CommonAttributes attrs);
    }

    private <T> void completeRegistrations(Map<TxnId, CommonAttributes> updates, List<PendingRegistration<T>> pendingRegistrations, RegistrationCompleter<T, CommandType> completer)
    {
        if (pendingRegistrations == null)
            return;

        for (PendingRegistration<T> pendingRegistration : pendingRegistrations)
        {
            TxnId txnId = pendingRegistration.txnId;
            CommandType safeCommand = getInternal(pendingRegistration.txnId);
            Command command = safeCommand.current();
            CommonAttributes attrs = updates.getOrDefault(txnId, command);
            attrs = completer.complete(pendingRegistration.value, pendingRegistration.slice, safeCommand, attrs);
            if (attrs != command)
                updates.put(txnId, attrs);
        }
    }

    protected abstract void invalidateSafeState();
    protected abstract void applyCommandForKeyUpdates();

    public void postExecute()
    {
        if (pendingSeekableRegistrations != null || pendingSeekablesRegistrations != null)
        {
            Map<TxnId, CommonAttributes> attributeUpdates = new HashMap<>();
            completeRegistrations(attributeUpdates, pendingSeekablesRegistrations, this::completeRegistration);
            completeRegistrations(attributeUpdates, pendingSeekableRegistrations, this::completeRegistration);
            attributeUpdates.forEach(((txnId, attributes) -> get(txnId).updateAttributes(attributes)));
        }
        applyCommandForKeyUpdates();
    }

    public void complete()
    {
        invalidateSafeState();
    }
}
