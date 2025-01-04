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

package accord.impl.basic;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.collect.ImmutableSortedMap;

import accord.api.Agent;
import accord.api.Journal;
import accord.api.Result;
import accord.impl.CommandChange;
import accord.impl.ErasedSafeCommand;
import accord.impl.InMemoryCommandStore;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Commands;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.PersistentField;
import org.agrona.collections.Int2ObjectHashMap;

import static accord.api.Journal.Load.ALL;
import static accord.impl.CommandChange.*;
import static accord.impl.CommandChange.Field.ACCEPTED;
import static accord.impl.CommandChange.Field.DURABILITY;
import static accord.impl.CommandChange.Field.EXECUTES_AT_LEAST;
import static accord.impl.CommandChange.Field.EXECUTE_AT;
import static accord.impl.CommandChange.Field.PARTIAL_DEPS;
import static accord.impl.CommandChange.Field.PARTIAL_TXN;
import static accord.impl.CommandChange.Field.PARTICIPANTS;
import static accord.impl.CommandChange.Field.PROMISED;
import static accord.impl.CommandChange.Field.RESULT;
import static accord.impl.CommandChange.Field.SAVE_STATUS;
import static accord.impl.CommandChange.Field.WAITING_ON;
import static accord.impl.CommandChange.Field.WRITES;
import static accord.primitives.SaveStatus.ErasedOrVestigial;
import static accord.primitives.SaveStatus.Stable;
import static accord.primitives.Status.Invalidated;
import static accord.primitives.Status.Truncated;
import static accord.utils.Invariants.illegalState;

public class InMemoryJournal implements Journal
{
    private final Int2ObjectHashMap<NavigableMap<TxnId, List<Diff>>> diffsPerCommandStore = new Int2ObjectHashMap<>();
    private final List<TopologyUpdate> topologyUpdates = new ArrayList<>();
    private final Int2ObjectHashMap<FieldUpdates> fieldStates = new Int2ObjectHashMap<>();

    private final Node.Id id;
    private final Agent agent;

    public InMemoryJournal(Node.Id id, Agent agent)
    {
        this.id = id;
        this.agent = agent;
    }

    @Override
    public Command loadCommand(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        NavigableMap<TxnId, List<Diff>> commandStore = this.diffsPerCommandStore.get(commandStoreId);

        if (commandStore == null)
            return null;

        List<Diff> saved = this.diffsPerCommandStore.get(commandStoreId).get(txnId);
        if (saved == null)
            return null;

        Builder builder = reconstruct(saved, ALL);
        Cleanup cleanup = builder.shouldCleanup(agent, redundantBefore, durableBefore);
        switch (cleanup)
        {
            case EXPUNGE_PARTIAL:
            case EXPUNGE:
            case ERASE:
                return ErasedSafeCommand.erased(txnId, ErasedOrVestigial);
        }

        return builder.construct(redundantBefore);
    }

    @Override
    public Command.Minimal loadMinimal(int commandStoreId, TxnId txnId, Load load, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        Builder builder = reconstruct(commandStoreId, txnId, load);
        if (builder == null || builder.isEmpty())
            return null;

        Cleanup cleanup = builder.shouldCleanup(agent, redundantBefore, durableBefore);
        switch (cleanup)
        {
            case EXPUNGE_PARTIAL:
            case EXPUNGE:
            case ERASE:
                return null;
        }

        Invariants.checkState(builder.saveStatus() != null, "No saveSatus loaded, but next was called and cleanup was not: %s", builder);
        return builder.asMinimal();
    }

    private Builder reconstruct(int commandStoreId, TxnId txnId, Load load)
    {
        NavigableMap<TxnId, List<Diff>> commandStore = this.diffsPerCommandStore.get(commandStoreId);

        if (commandStore == null)
            return null;

        return reconstruct(this.diffsPerCommandStore.get(commandStoreId).get(txnId), load);
    }

    private Builder reconstruct(List<Diff> saved, Load load)
    {
        if (saved == null)
            return null;

        Builder builder = null;
        for (Diff diff : saved)
        {
            if (builder == null)
                builder = new Builder(diff.txnId, load);
            builder.apply(diff);
        }
        return builder;
    }

    @Override
    public void saveCommand(int store, CommandUpdate update, Runnable onFlush)
    {
        Diff diff;
        if ((diff = toDiff(update)) == null)
        {
            if (onFlush!= null)
                onFlush.run();
            return;
        }

        diffsPerCommandStore.computeIfAbsent(store, (k) -> new TreeMap<>())
                            .computeIfAbsent(update.txnId, (k_) -> new ArrayList<>())
                            .add(diff);

        if (onFlush!= null)
            onFlush.run();
    }

    @Override
    public Iterator<TopologyUpdate> replayTopologies()
    {
        return new Iterator<>()
        {
            int current = 0;
            public boolean hasNext()
            {
                return current < topologyUpdates.size();
            }

            public TopologyUpdate next()
            {
                return topologyUpdates.get(current++);
            }
        };
    }

    @Override
    public void saveTopology(TopologyUpdate topologyUpdate, Runnable onFlush)
    {
        topologyUpdates.add(topologyUpdate);
        if (onFlush != null)
            onFlush.run();
    }

    public void truncateTopologiesForTesting(long minEpoch)
    {
        List<TopologyUpdate> next = new ArrayList<>();
        for (int i = 0; i < topologyUpdates.size(); i++)
        {
            TopologyUpdate update = topologyUpdates.get(i);
            if (update.global.epoch() >= minEpoch)
                next.add(update);
        }
        topologyUpdates.retainAll(next);
    }

    @Override
    public RedundantBefore loadRedundantBefore(int commandStoreId)
    {
        FieldUpdates fieldStates = this.fieldStates.get(commandStoreId);
        if (fieldStates == null)
            return null;
        return fieldStates.newRedundantBefore;
    }

    @Override
    public NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int commandStoreId)
    {
        FieldUpdates fieldStates = this.fieldStates.get(commandStoreId);
        if (fieldStates == null)
            return null;
        return fieldStates.newBootstrapBeganAt;
    }

    @Override
    public NavigableMap<Timestamp, Ranges> loadSafeToRead(int commandStoreId)
    {
        FieldUpdates fieldStates = this.fieldStates.get(commandStoreId);
        if (fieldStates == null)
            return null;
        return fieldStates.newSafeToRead;
    }

    @Override
    public CommandStores.RangesForEpoch loadRangesForEpoch(int commandStoreId)
    {
        FieldUpdates fieldStates = this.fieldStates.get(commandStoreId);
        if (fieldStates == null)
            return null;
        return fieldStates.newRangesForEpoch;
    }

    @Override
    public PersistentField.Persister<DurableBefore, DurableBefore> durableBeforePersister()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void saveStoreState(int store, FieldUpdates fieldUpdates, Runnable onFlush)
    {
        FieldUpdates fieldStates = this.fieldStates.computeIfAbsent(store, s -> {
            FieldUpdates init = new FieldUpdates();
            init.newRedundantBefore = RedundantBefore.EMPTY;
            init.newBootstrapBeganAt = ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY);
            init.newSafeToRead = ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY);
            return init;
        });

        if (fieldUpdates.newRedundantBefore != null)
            fieldStates.newRedundantBefore = fieldUpdates.newRedundantBefore;
        if (fieldUpdates.newSafeToRead != null)
            fieldStates.newSafeToRead = fieldUpdates.newSafeToRead;
        if (fieldUpdates.newBootstrapBeganAt != null)
            fieldStates.newBootstrapBeganAt = fieldUpdates.newBootstrapBeganAt;
        if (fieldUpdates.newRangesForEpoch != null)
            fieldStates.newRangesForEpoch = fieldUpdates.newRangesForEpoch;

        if (onFlush!= null)
            onFlush.run();
    }

    @Override
    public void purge(CommandStores commandStores)
    {
        for (Map.Entry<Integer, NavigableMap<TxnId, List<Diff>>> e : diffsPerCommandStore.entrySet())
        {
            int commandStoreId = e.getKey();
            Map<TxnId, List<Diff>> localJournal = e.getValue();
            CommandStore store = commandStores.forId(commandStoreId);
            if (store == null)
                continue;

            for (Map.Entry<TxnId, List<Diff>> e2 : localJournal.entrySet())
            {
                List<Diff> diffs = e2.getValue();
                if (diffs.isEmpty()) continue;
                InMemoryJournal.Builder builder = reconstruct(diffs, ALL);
                if (builder.saveStatus().status == Truncated || builder.saveStatus().status == Invalidated)
                    continue; // Already truncated

                Command command = builder.construct(store.unsafeGetRedundantBefore());
                Cleanup cleanup = Cleanup.shouldCleanup(store.agent(), command, store.unsafeGetRedundantBefore(), store.durableBefore());
                switch (cleanup)
                {
                    case NO:
                        break;
                    case INVALIDATE:
                    case TRUNCATE_WITH_OUTCOME:
                    case TRUNCATE:
                    case ERASE:
                        command = Commands.purgeUnsafe(store, command, cleanup);
                        Invariants.checkState(command.saveStatus() != SaveStatus.Uninitialised);
                        Diff diff = toDiff(new CommandUpdate(null, command));
                        e2.setValue(cleanup == Cleanup.ERASE ? new ErasedList(diff) : new TruncatedList(diff));
                        break;

                    case EXPUNGE:
                        e2.setValue(new PurgedList(e2.getValue()));
                        break;
                }
            }
        }
    }

    @Override
    public void replay(CommandStores commandStores)
    {
        OnDone sync = new OnDone() {
            public void success() {}
            public void failure(Throwable t) { throw new RuntimeException("Caught an exception during replay", t); }
        };

        for (Map.Entry<Integer, NavigableMap<TxnId, List<Diff>>> diffEntry : diffsPerCommandStore.entrySet())
        {
            int commandStoreId = diffEntry.getKey();

            // copy to avoid concurrent modification when appending to journal
            Map<TxnId, List<Diff>> diffs = new TreeMap<>(diffEntry.getValue());

            InMemoryCommandStore commandStore = (InMemoryCommandStore) commandStores.forId(commandStoreId);
            Loader loader = commandStore.loader();

            for (Map.Entry<TxnId, List<Diff>> e : diffs.entrySet())
                e.setValue(new ArrayList<>(e.getValue()));

            for (Map.Entry<TxnId, List<Diff>> e : diffs.entrySet())
            {
                if (e.getValue().isEmpty()) continue;
                Command command = reconstruct(e.getValue(), ALL).construct(commandStore.unsafeGetRedundantBefore());
                Invariants.checkState(command.saveStatus() != SaveStatus.Uninitialised,
                                      "Found uninitialized command in the log: %s %s", diffEntry.getKey(), e.getValue());
                loader.load(command, sync);
                if (command.saveStatus().compareTo(Stable) >= 0 && !command.hasBeen(Truncated))
                    loader.apply(command, sync);
            }
        }
    }

    private static class ErasedList extends AbstractList<Diff>
    {
        final Diff erased;

        ErasedList(Diff erased)
        {
            Invariants.checkArgument(erased.changes.get(SAVE_STATUS) == SaveStatus.Erased);
            this.erased = erased;
        }

        @Override
        public Diff get(int index)
        {
            if (index != 0)
                throw new IndexOutOfBoundsException();
            return erased;
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public boolean add(Diff diff)
        {
            if (diff.changes.get(SAVE_STATUS) == SaveStatus.Erased)
                return false;
            throw illegalState();
        }
    }

    static class TruncatedList extends ArrayList<Diff>
    {
        TruncatedList(Diff truncated)
        {
            add(truncated);
        }
    }

    private static class PurgedList extends AbstractList<Diff>
    {
        final List<Diff> purged;
        PurgedList(List<Diff> purged)
        {
            this.purged = purged;
        }

        @Override
        public Diff get(int index)
        {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public boolean add(Diff diff)
        {
            Object saveStatus = diff.changes.get(SAVE_STATUS);
            if (saveStatus == SaveStatus.Erased)
                return false;
            throw illegalState();
        }
    }

    private static Diff toDiff(CommandUpdate update)
    {
        if (update == null
            || update.before == update.after
            || update.after == null
            || update.after.saveStatus() == SaveStatus.Uninitialised)
            return null;

        int flags = validateFlags(getFlags(update.before, update.after));
        if (!anyFieldChanged(flags))
            return null;

        return new Diff(flags, update);
    }

    private static class Diff
    {
        public final TxnId txnId;
        public final Map<Field, Object> changes;
        public final int flags;

        private Diff(int flags, CommandUpdate update)
        {
            this.flags = flags;
            this.txnId = update.txnId;
            this.changes = new EnumMap<>(Field.class);

            Command after = update.after;
            int iterable = toIterableSetFields(flags);
            while (iterable != 0)
            {
                Field field = nextSetField(iterable);
                if (!getFieldChanged(field, flags) || getFieldIsNull(field, flags))
                {
                    iterable = unsetIterableFields(field, iterable);
                    continue;
                }

                switch (field)
                {
                    case EXECUTE_AT:
                        changes.put(EXECUTE_AT, after.executeAt());
                        break;
                    case EXECUTES_AT_LEAST:
                        changes.put(EXECUTES_AT_LEAST, after.executesAtLeast());
                        break;
                    case SAVE_STATUS:
                        changes.put(SAVE_STATUS, after.saveStatus());
                        break;
                    case DURABILITY:
                        changes.put(DURABILITY, after.durability());
                        break;
                    case ACCEPTED:
                        changes.put(ACCEPTED, after.acceptedOrCommitted());
                        break;
                    case PROMISED:
                        changes.put(PROMISED, after.promised());
                        break;
                    case PARTICIPANTS:
                        changes.put(PARTICIPANTS, after.participants());
                        break;
                    case PARTIAL_TXN:
                        changes.put(PARTIAL_TXN, after.partialTxn());
                        break;
                    case PARTIAL_DEPS:
                        changes.put(PARTIAL_DEPS, after.partialDeps());
                        break;
                    case WAITING_ON:
                        Command.WaitingOn waitingOn = getWaitingOn(after);
                        changes.put(WAITING_ON, (WaitingOnProvider) (txnId, deps) -> waitingOn);
                        break;
                    case WRITES:
                        changes.put(WRITES, after.writes());
                        break;
                    case RESULT:
                        changes.put(RESULT, after.result());
                        break;
                    case CLEANUP:
                }

                iterable = unsetIterableFields(field, iterable);
            }
        }
    }

    private static class Builder extends CommandChange.Builder
    {
        private Builder(TxnId txnId, Load load)
        {
            super(txnId, load);
        }

        private void apply(Diff diff)
        {
            Invariants.checkState(diff.txnId != null);
            Invariants.checkState(diff.flags != 0);
            nextCalled = true;
            count++;

            int iterable = toIterableSetFields(diff.flags);
            while (iterable != 0)
            {
                Field field = nextSetField(iterable);
                if (getFieldChanged(field, diff.flags))
                {
                    this.flags = setFieldChanged(field, this.flags);
                    if (getFieldIsNull(field, diff.flags))
                    {
                        this.flags = setFieldIsNull(field, this.flags);
                        setNull(field);
                    }
                    else
                    {
                        this.flags = unsetFieldIsNull(field, this.flags);
                        deserialize(diff, field);
                    }
                }

                iterable = unsetIterableFields(field, iterable);
            }
        }

        private void setNull(Field field)
        {
            switch (field)
            {
                case EXECUTE_AT:
                    executeAt = null;
                    break;
                case EXECUTES_AT_LEAST:
                    executeAtLeast = null;
                    break;
                case SAVE_STATUS:
                    saveStatus = null;
                    break;
                case DURABILITY:
                    durability = null;
                    break;
                case ACCEPTED:
                    acceptedOrCommitted = null;
                    break;
                case PROMISED:
                    promised = null;
                    break;
                case PARTICIPANTS:
                    participants = null;
                    break;
                case PARTIAL_TXN:
                    partialTxn = null;
                    break;
                case PARTIAL_DEPS:
                    partialDeps = null;
                    break;
                case WAITING_ON:
                    waitingOn = null;
                    break;
                case WRITES:
                    writes = null;
                    break;
                case RESULT:
                    result = null;
                    break;
                case CLEANUP:
                    throw new IllegalStateException();
            }
        }

        private void deserialize(Diff diff, Field field)
        {
            switch (field)
            {
                case EXECUTE_AT:
                    executeAt = Invariants.nonNull((Timestamp) diff.changes.get(EXECUTE_AT));
                    break;
                case EXECUTES_AT_LEAST:
                    executeAtLeast = Invariants.nonNull((Timestamp) diff.changes.get(EXECUTES_AT_LEAST));
                    break;
                case SAVE_STATUS:
                    saveStatus = Invariants.nonNull((SaveStatus) diff.changes.get(SAVE_STATUS));
                    break;
                case DURABILITY:
                    durability = Invariants.nonNull((Status.Durability) diff.changes.get(DURABILITY));
                    break;
                case ACCEPTED:
                    acceptedOrCommitted = Invariants.nonNull((Ballot) diff.changes.get(ACCEPTED));
                    break;
                case PROMISED:
                    promised = Invariants.nonNull((Ballot) diff.changes.get(PROMISED));
                    break;
                case PARTICIPANTS:
                    participants = Invariants.nonNull((StoreParticipants) diff.changes.get(PARTICIPANTS));
                    break;
                case PARTIAL_TXN:
                    partialTxn = Invariants.nonNull((PartialTxn) diff.changes.get(PARTIAL_TXN));
                    break;
                case PARTIAL_DEPS:
                    partialDeps = Invariants.nonNull((PartialDeps) diff.changes.get(PARTIAL_DEPS));
                    break;
                case WAITING_ON:
                    waitingOn = Invariants.nonNull((WaitingOnProvider) diff.changes.get(WAITING_ON));
                    break;
                case WRITES:
                    writes = Invariants.nonNull((Writes) diff.changes.get(WRITES));
                    break;
                case RESULT:
                    result = Invariants.nonNull((Result) diff.changes.get(RESULT));
                    break;
                case CLEANUP:
                    throw new IllegalStateException();
            }
        }
    }
}