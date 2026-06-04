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
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Journal;
import accord.api.Result.PersistableResult;
import accord.impl.CommandChange;
import accord.impl.InMemoryCommandStore;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.LogUnavailableException;
import accord.local.MinimalCommand;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.EpochSupplier;
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
import accord.utils.RandomSource;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChainUtils;
import accord.utils.async.AsyncResult;
import org.agrona.collections.Int2ObjectHashMap;

import static accord.api.Journal.Load.ALL;
import static accord.api.Journal.Load.MINIMAL;
import static accord.api.Journal.Load.MINIMAL_WITH_DEPS;
import static accord.impl.AbstractReplayer.Mode.PART_NON_DURABLE;
import static accord.impl.CommandChange.Field;
import static accord.impl.CommandChange.Field.ACCEPTED;
import static accord.impl.CommandChange.Field.CLEANUP;
import static accord.impl.CommandChange.Field.DURABILITY;
import static accord.impl.CommandChange.Field.EXECUTES_AT_LEAST;
import static accord.impl.CommandChange.Field.EXECUTE_AT;
import static accord.impl.CommandChange.Field.MIN_UNIQUE_HLC;
import static accord.impl.CommandChange.Field.PARTIAL_DEPS;
import static accord.impl.CommandChange.Field.PARTIAL_TXN;
import static accord.impl.CommandChange.Field.PARTICIPANTS;
import static accord.impl.CommandChange.Field.PROMISED;
import static accord.impl.CommandChange.Field.RESULT;
import static accord.impl.CommandChange.Field.SAVE_STATUS;
import static accord.impl.CommandChange.Field.WAITING_ON;
import static accord.impl.CommandChange.Field.WRITES;
import static accord.impl.CommandChange.anyFieldChanged;
import static accord.impl.CommandChange.getFlags;
import static accord.impl.CommandChange.isChanged;
import static accord.impl.CommandChange.isNull;
import static accord.impl.CommandChange.nextSetField;
import static accord.impl.CommandChange.setChanged;
import static accord.impl.CommandChange.setIsNullAndChanged;
import static accord.impl.CommandChange.toIterableSetFields;
import static accord.impl.CommandChange.unsetFieldIsNull;
import static accord.impl.CommandChange.unsetIterable;
import static accord.impl.CommandChange.validateFlags;
import static accord.local.Cleanup.ERASE;
import static accord.local.Cleanup.EXPUNGE;
import static accord.local.Cleanup.INVALIDATE;
import static accord.local.Cleanup.Input;
import static accord.local.Cleanup.Input.FULL;
import static accord.local.Cleanup.Input.PARTIAL;
import static accord.local.Cleanup.NO;
import static accord.local.Cleanup.TRUNCATE;
import static accord.local.Cleanup.TRUNCATE_WITH_OUTCOME;
import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.Status.Truncated;
import static accord.utils.Invariants.illegalState;

public class InMemoryJournal implements Journal
{
    private static final Logger log = LoggerFactory.getLogger(InMemoryJournal.class);
    private final Int2ObjectHashMap<NavigableMap<TxnId, Diffs>> diffsPerCommandStore = new Int2ObjectHashMap<>();
    private final List<TopologyUpdate> topologyUpdates = new ArrayList<>();
    private final Int2ObjectHashMap<FieldUpdates> fieldStates = new Int2ObjectHashMap<>();

    private Node node;
    private final RandomSource random;
    private final float partialCompactionChance;

    public InMemoryJournal(Node.Id id, RandomSource random)
    {
        this.random = random;
        this.partialCompactionChance = 1f - (random.nextFloat()/2);
    }

    @Override
    public void open(Node node)
    {
    }

    public void start(Node node)
    {
        this.node = node;
    }

    public void dropAll()
    {
        diffsPerCommandStore.clear();
        // TODO (expected): this seems to be a bit of a mess:
        //   We split responsibility for RangesForEpoch between TopologyUpdate and FieldStates, and clear them differently.
        //   Need to rationalise this better.
        fieldStates.forEach((k, v) -> {
            v.newRedundantBefore = null;
            v.newSafeToRead = null;
            v.newBootstrapBeganAt = null;
        });
    }

    @Override
    public Command loadCommand(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        NavigableMap<TxnId, Diffs> commandStore = this.diffsPerCommandStore.get(commandStoreId);

        if (commandStore == null)
            return null;

        Diffs saved = this.diffsPerCommandStore.get(commandStoreId).get(txnId);
        if (saved == null)
            return null;

        Builder builder = reconstruct(saved, ALL);
        if (builder == null)
            return null;

        builder.maybeCleanup(true, FULL, redundantBefore, durableBefore);
        return builder.construct(redundantBefore);
    }

    private CommandChange.Builder loadMinimalInternal(Load load, int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        Builder builder = reconstruct(commandStoreId, txnId, load);
        if (builder == null || builder.isEmpty())
            return null;

        Cleanup cleanup = builder.shouldCleanup(FULL, redundantBefore, durableBefore);
        switch (cleanup)
        {
            case VESTIGIAL:
            case ERASE:
            case EXPUNGE:
                return null;
        }

        Invariants.require(builder.saveStatus() != null, "No saveSatus loaded, but next was called and cleanup was not: %s", builder);
        return builder;
    }

    @Override
    public MinimalCommand loadMinimal(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        CommandChange.Builder builder = loadMinimalInternal(MINIMAL, commandStoreId, txnId, redundantBefore, durableBefore);
        return builder == null ? null : builder.asMinimal();
    }

    @Override
    public MinimalCommand.MinimalWithDeps loadMinimalWithDeps(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        CommandChange.Builder builder = loadMinimalInternal(MINIMAL_WITH_DEPS, commandStoreId, txnId, redundantBefore, durableBefore);
        return builder == null ? null : builder.asMinimalWithDeps();
    }

    private Builder reconstruct(int commandStoreId, TxnId txnId, Load load)
    {
        NavigableMap<TxnId, Diffs> commandStore = this.diffsPerCommandStore.get(commandStoreId);

        if (commandStore == null)
            return null;

        return reconstruct(this.diffsPerCommandStore.get(commandStoreId).get(txnId), load);
    }

    private Builder reconstruct(Diffs files, Load load)
    {
        if (files == null)
            return null;

        Builder builder = null;
        List<Diff> saved = files.sorted(false);
        for (int i = saved.size() - 1; i >= 0; i--)
        {
            Diff diff = saved.get(i);
            if (diff == null)
                continue;
            if (builder == null)
                builder = new Builder(diff.txnId, load);
            builder.apply(diff);
        }
        return builder;
    }

    @Override
    public void saveCommand(int commandStoreId, CommandUpdate update, Runnable onFlush)
    {
        Diff diff;
        if ((diff = toDiff(update)) == null)
        {
            if (onFlush!= null)
                onFlush.run();
            return;
        }

        diffsPerCommandStore.computeIfAbsent(commandStoreId, (k) -> new TreeMap<>())
                            .computeIfAbsent(update.txnId, (k_) -> new Diffs())
                            .addFlushed(diff);

        if (onFlush!= null)
            onFlush.run();
    }

    @Override
    public List<TopologyUpdate> loadTopologies()
    {
        return new ArrayList<>(topologyUpdates);
    }

    @Override
    public void saveTopology(TopologyUpdate topologyUpdate, Runnable onFlush)
    {
        // Ensure that we only read the latest topologyUpdate as that is what happens in the
        // C* implementation
        int lastIndex = topologyUpdates.size() - 1;
        if (!topologyUpdates.isEmpty() && topologyUpdates.get(lastIndex).global.equals(topologyUpdate.global))
            topologyUpdates.remove(lastIndex);
        topologyUpdates.add(topologyUpdate);
        if (onFlush != null)
            onFlush.run();
    }

    public void truncateTopologiesForTesting(long minEpoch)
    {
        Iterator<TopologyUpdate> iter = topologyUpdates.iterator();
        while (iter.hasNext())
        {
            TopologyUpdate current = iter.next();
            if (current.global.epoch() >= minEpoch)
                break;
            iter.remove();
        }
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
    public Ranges loadPermanentlyUnsafeToRead(int commandStoreId)
    {
        FieldUpdates fieldStates = this.fieldStates.get(commandStoreId);
        if (fieldStates == null)
            return null;
        return fieldStates.newPermanentlyUnsafeToRead;
    }

    @Override
    public PersistentField.Persister<DurableBefore, DurableBefore> durableBeforePersister()
    {
        return DurableBefore.NOOP_PERSISTER;
    }

    @Override
    public void saveStoreState(int store, FieldUpdates fieldUpdates, Runnable onFlush)
    {
        FieldUpdates fieldStates = this.fieldStates.computeIfAbsent(store, s -> {
            FieldUpdates init = new FieldUpdates();
            init.newRedundantBefore = RedundantBefore.EMPTY;
            init.newBootstrapBeganAt = ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY);
            init.newSafeToRead = ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY);
            init.newPermanentlyUnsafeToRead = Ranges.EMPTY;
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
        if (fieldUpdates.newPermanentlyUnsafeToRead != null)
            fieldStates.newPermanentlyUnsafeToRead = fieldUpdates.newPermanentlyUnsafeToRead;

        if (onFlush!= null)
            onFlush.run();
    }

    static class DiffFile extends ArrayList<Diff>
    {
        DiffFile(){}
        DiffFile(List<Diff> diffs)
        {
            for (Diff diff : diffs)
            {
                if (diff != null)
                    add(diff);
            }
        }
    }

    static class Diffs
    {
        final boolean subset;
        final List<DiffFile> files;
        final List<Diff> flushed;
        int nextId;

        int size;
        List<Diff> sorted;

        Diffs()
        {
            this.subset = false;
            this.files = new ArrayList<>();
            this.flushed = new ArrayList<>();
        }

        Diffs(PurgedList purged)
        {
            this.subset = false;
            this.files = Collections.emptyList();
            this.flushed = purged;
        }

        Diffs(TruncatedList truncated)
        {
            this.subset = false;
            this.files = new ArrayList<>();
            this.flushed = truncated;
            this.size = 1;
        }

        Diffs(ErasedList erased)
        {
            this.subset = false;
            this.files = Collections.emptyList();
            this.flushed = erased;
            this.size = 1;
        }

        Diffs(boolean subset, List<DiffFile> files, List<Diff> flushed)
        {
            this.subset = subset;
            this.files = files;
            this.flushed = flushed;
            this.size = flushed.size();
            for (DiffFile file : files)
                size += file.size();
        }

        void addFlushed(Diff diff)
        {
            diff.rowId = ++nextId;
            flushed.add(diff);
            if (sorted != null && sorted != flushed)
                sorted.add(diff);
            ++size;
        }

        List<Diff> sorted(boolean copy)
        {
            if (sorted != null)
            {
                Invariants.require(sorted.size() == size);
                return copy ? new ArrayList<>(sorted) : sorted;
            }

            if (!subset)
            {
                if (files.isEmpty())
                    return copy ? new ArrayList<>(flushed) : flushed;

                if (flushed.isEmpty() && files.size() == 1)
                    return copy ? new ArrayList<>(files.get(0)) : files.get(0);
            }

            List<Diff> sorted = new ArrayList<>(size);
            for (Diff diff : flushed)
            {
                if (diff != null)
                    sorted.add(diff);
            }
            for (DiffFile file : this.files)
            {
                if (file != null)
                    sorted.addAll(file);
            }
            Invariants.require(sorted.size() == size);
            sorted.sort(Comparator.comparingInt(d -> d.rowId));
            if (!copy)
                this.sorted = sorted;
            return sorted;
        }

        void removeAll(Diffs diffs)
        {
            files.removeAll(diffs.files);
            flushed.removeAll(diffs.flushed);
            size -= diffs.size;
            sorted = null;
        }

        boolean isEmpty()
        {
            return size == 0;
        }
    }

    static int counter = 0;
    @Override
    public void purge(CommandStores commandStores, EpochSupplier minEpoch)
    {
        truncateTopologiesForTesting(minEpoch.epoch());
        boolean isPartialCompaction = random.decide(0.9f);
        for (Map.Entry<Integer, NavigableMap<TxnId, Diffs>> e : diffsPerCommandStore.entrySet())
        {
            int commandStoreId = e.getKey();
            Map<TxnId, Diffs> localJournal = e.getValue();
            CommandStore store = commandStores.forId(commandStoreId);
            if (store == null)
                continue;

            for (Map.Entry<TxnId, Diffs> e2 : localJournal.entrySet())
            {
                Diffs diffs = e2.getValue();
                if (diffs.isEmpty()) continue;

                Diffs subset = diffs;
                {
                    int filesAndFlushed = subset.flushed.size() + subset.files.size();
                    if (filesAndFlushed > 1 && isPartialCompaction)
                    {
                        int removeCount = 1 + random.nextInt(filesAndFlushed - 1);
                        int count = filesAndFlushed;
                        subset = new Diffs(true, new ArrayList<>(diffs.files), new ArrayList<>(diffs.flushed));
                        List<DiffFile> files = subset.files;
                        List<Diff> flushed = subset.flushed;
                        while (removeCount-- > 0)
                        {
                            int removeIndex = random.nextInt(filesAndFlushed);
                            if (removeIndex < flushed.size())
                            {
                                if (flushed.get(removeIndex) == null)
                                    continue;
                                --subset.size;
                                flushed.set(removeIndex, null);
                            }
                            else
                            {
                                removeIndex -= flushed.size();
                                if (files.get(removeIndex) == null)
                                    continue;
                                subset.size -= files.get(removeIndex).size();
                                files.set(removeIndex, null);
                            }
                            --count;
                        }

                        if (count == 0)
                            continue;
                    }
                }

                Builder[] builders = new Builder[subset.size];
                List<Diff> sorted = subset.sorted(true);
                for (int i = 0 ; i < sorted.size() ; ++i)
                {
                    if (sorted.get(i) == null) continue;
                    Builder builder = new Builder(e2.getKey(), ALL);
                    builder.apply(sorted.get(i));
                    builders[i] = builder;
                }

                Builder builder = new Builder(e2.getKey(), ALL);
                for (int i = builders.length - 1; i >= 0 ; --i)
                {
                    Builder current = builders[i];
                    if (current == null)
                        continue;
                    current.clearSuperseded(false, builder);
                    builder.fillInMissingOrCleanup(false, current);
                }

                Input input = isPartialCompaction ? PARTIAL : FULL;
                ++counter;

                Cleanup cleanup;
                try {cleanup = builder.shouldCleanup(input, store.unsafeGetRedundantBefore(), store.durableBefore()); }
                catch (LogUnavailableException ignore) {cleanup = ERASE; }

                cleanup = builder.maybeCleanup(true, cleanup);
                if (cleanup != NO)
                {
                    if (cleanup == EXPUNGE)
                    {
                        if (input == FULL) e2.setValue(new Diffs(new PurgedList()));
                        else if (subset == diffs) e2.setValue(new Diffs());
                        else diffs.removeAll(subset);
                        continue;
                    }
                    else
                    {
                        if (isPartialCompaction)
                        {
                            boolean saveCleanup = true;
                            for (int i = builders.length - 1; i >= 0 ; --i)
                            {
                                if (builders[i] != null)
                                {
                                    if (saveCleanup) builders[i].addCleanup(false, cleanup);
                                    else builders[i].cleanup(false, cleanup);
                                    saveCleanup = false;
                                }
                            }
                        }
                        // During full compaction, we erase all previous records, replacing them with new image
                        else
                        {
                            Diff diff = builder.toDiff();
                            e2.setValue(cleanup == ERASE ? new Diffs(new ErasedList(diff)) : new Diffs(new TruncatedList(diff)));
                            continue;
                        }
                    }
                }

                if (diffs.flushed instanceof FinalList)
                    continue;

                int removeCount = 0;
                for (int i = 0 ; i < builders.length ; ++i)
                {
                    if (builders[i] != null)
                    {
                        Diff diff = builders[i].toDiff();
                        if (diff.flags == 0)
                        {
                            ++removeCount;
                            sorted.set(i, null);
                        }
                        else
                        {
                            diff.rowId = sorted.get(i).rowId;
                            sorted.set(i, diff);
                        }
                    }
                }

                Builder before = reconstruct(diffs, ALL);
                boolean unavailableBefore = false, unavailableAfter = false;
                try { before.maybeCleanup(true, FULL, store.unsafeGetRedundantBefore(), store.durableBefore()); }
                catch (LogUnavailableException ignore) { unavailableBefore = true; }
                diffs.size -= removeCount;
                diffs.flushed.removeAll(subset.flushed);
                diffs.files.removeAll(subset.files);
                diffs.files.add(new DiffFile(sorted));
                diffs.sorted = null;
                Builder after = reconstruct(diffs, ALL);
                try { after.maybeCleanup(true, FULL, store.unsafeGetRedundantBefore(), store.durableBefore()); }
                catch (LogUnavailableException ignore) { unavailableAfter = true; }
                Invariants.require(unavailableBefore == unavailableAfter);
                Invariants.require(Objects.equals(before.construct(store.unsafeGetRedundantBefore()), after.construct(store.unsafeGetRedundantBefore())));
            }
        }
    }

    @Override
    public boolean replay(CommandStores commandStores, Object param)
    {
        for (Map.Entry<Integer, NavigableMap<TxnId, Diffs>> diffEntry : diffsPerCommandStore.entrySet())
        {
            int commandStoreId = diffEntry.getKey();

            // copy to avoid concurrent modification when appending to journal
            Map<TxnId, List<Diff>> diffs = new TreeMap<>();

            InMemoryCommandStore commandStore = (InMemoryCommandStore) commandStores.forId(commandStoreId);

            if (commandStore == null)
                continue;

            Replayer replayer = commandStore.replayer(PART_NON_DURABLE);

            for (Map.Entry<TxnId, Diffs> e : diffEntry.getValue().entrySet())
                diffs.put(e.getKey(), e.getValue().sorted(true));

            for (Map.Entry<TxnId, List<Diff>> e : diffs.entrySet())
            {
                if (e.getValue().isEmpty()) continue;

                AsyncResult<?> res = replayer.replay(e.getKey()).beginAsResult();
                AsyncChainUtils.getUnchecked(res);
            }

        }
        return true;
    }

    static class TruncatedList extends ArrayList<Diff>
    {
        TruncatedList(Diff truncated)
        {
            add(truncated);
        }
    }

    private static abstract class FinalList extends AbstractList<Diff>
    {

    }

    private static class ErasedList extends FinalList
    {
        private Diff erased;

        ErasedList(Diff erased)
        {
            Invariants.requireArgument(erased.changes.get(SAVE_STATUS) == Erased || erased.changes.get(CLEANUP) == ERASE);
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
            // TODO (expected): we shouldn't really be saving updates (such as durability updates) to Erased commands
            if (diff.changes.get(SAVE_STATUS) == Erased || diff.changes.get(SAVE_STATUS) == null || diff.changes.get(CLEANUP) == ERASE)
                return false;
            throw illegalState();
        }

        @Override
        public Diff set(int index, Diff diff)
        {
            if (diff.changes.get(SAVE_STATUS) == Erased || diff.changes.get(CLEANUP) == ERASE)
            {
                erased = diff;
                return erased;
            }
            return super.set(index, diff);
        }
    }

    private static class PurgedList extends FinalList
    {
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
            if (saveStatus == Erased)
                return false;
            throw illegalState();
        }
    }

    private static Diff toDiff(@Nonnull CommandUpdate update)
    {
        if (update.before == null)
        {
            if (update.after.saveStatus() == Uninitialised)
                return null;
        }
        else
        {
            Invariants.require(update.after.saveStatus() != Uninitialised);
            if (update.before.saveStatus() == Erased)
                return null;
        }

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
        private int rowId;

        private Diff(TxnId txnId, int flags, Map<Field, Object> changes)
        {
            this.txnId = txnId;
            this.flags = flags;
            this.changes = changes;
        }

        private Diff(int flags, CommandUpdate update)
        {
            this.txnId = update.txnId;
            this.changes = new EnumMap<>(Field.class);

            Command after = update.after;
            int iterable = toIterableSetFields(flags);
            while (iterable != 0)
            {
                Field field = nextSetField(iterable);
                if (isNull(field, flags))
                {
                    Invariants.require(isChanged(field, flags));
                    iterable = unsetIterable(field, iterable);
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
                    case MIN_UNIQUE_HLC:
                        changes.put(MIN_UNIQUE_HLC, after.waitingOn().minUniqueHlc());
                        break;
                    case SAVE_STATUS:
                        changes.put(SAVE_STATUS, after.saveStatus());
                        if (after.saveStatus().is(Truncated))
                        {
                            switch (after.saveStatus())
                            {
                                case TruncatedApplyWithOutcome:
                                    changes.put(CLEANUP, TRUNCATE_WITH_OUTCOME);
                                    flags = setChanged(CLEANUP, unsetFieldIsNull(CLEANUP, flags));
                                    break;
                                case TruncatedApply:
                                case TruncatedUnapplied:
                                case Vestigial:
                                    changes.put(CLEANUP, TRUNCATE);
                                    flags = setChanged(CLEANUP, unsetFieldIsNull(CLEANUP, flags));
                                    break;
                            }
                        }
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
                        Command.WaitingOn waitingOn = after.waitingOn();
                        changes.put(WAITING_ON, new CommandChange.WaitingOnBitSets(waitingOn.waitingOn, waitingOn.appliedOrInvalidated));
                        break;
                    case WRITES:
                        changes.put(WRITES, after.writes());
                        break;
                    case RESULT:
                        changes.put(RESULT, after.result());
                        break;
                    case CLEANUP:
                        switch (after.saveStatus())
                        {
                            default: throw new UnhandledEnum(after.saveStatus());
                            case Erased: changes.put(CLEANUP, ERASE); break;
                            case Invalidated: changes.put(CLEANUP, INVALIDATE); break;
                        }
                        break;
                    default: throw new UnhandledEnum(field);
                }

                iterable = unsetIterable(field, iterable);
            }

            this.flags = flags;
        }

        @Override
        public String toString()
        {
            return "Diff{" +
                   "txnId=" + txnId +
                   ", changes=" + changes +
                   ", flags=" + flags +
                   '}';
        }
    }

    private static class Builder extends CommandChange.Builder
    {
        private Builder(TxnId txnId, Load load)
        {
            super(txnId, load);
        }

        @Override
        public PartialDeps partialDeps()
        {
            return (PartialDeps) partialDeps;
        }

        Diff toDiff()
        {
            int flags = this.flags;
            EnumMap<Field, Object> values = new EnumMap<>(Field.class);

            int iterator = toIterableSetFields(notNulls(flags)); // limit ourselves to 14 bits
            for (Field field = nextSetField(iterator); field != null; iterator = unsetIterable(field, iterator), field = nextSetField(iterator))
            {
                if (field == CLEANUP)
                    continue;
                Object v = Invariants.nonNull(get(field));
                values.put(field, v);
            }

            if (cleanup != null)
            {
                flags |= CommandChange.eraseKnownFieldsMask(cleanup.newStatus);
                values.put(CLEANUP, cleanup);
                flags = setChanged(CLEANUP, flags);
            }

            return new Diff(txnId(), flags, values);
        }

        private void apply(Diff diff)
        {
            Invariants.require(diff.txnId != null);
            Invariants.require(diff.flags != 0);
            hasUpdate = true;
            count++;

            int iterable = toIterableSetFields(diff.flags);
            while (iterable != 0)
            {
                Field field = nextSetField(iterable);

                // Since we are iterating in reverse order, we skip the fields that were
                // set by entries writer later (i.e. already read ones).
                if (isChanged(field, this.flags) || isNull(field, mask))
                {
                    iterable = unsetIterable(field, iterable);
                    if (field != CLEANUP || !isChanged(field, diff.flags))
                        continue;
                }

                if (isNull(field, diff.flags))
                {
                    this.flags = setIsNullAndChanged(field, this.flags);
                }
                else
                {
                    this.flags = setChanged(field, this.flags);
                    deserialize(diff, field);
                }

                iterable = unsetIterable(field, iterable);
            }
        }

        private void deserialize(Diff diff, Field field)
        {
            switch (field)
            {
                default: throw new UnhandledEnum(field);
                case EXECUTE_AT:
                    executeAt = Invariants.nonNull((Timestamp) diff.changes.get(EXECUTE_AT));
                    break;
                case EXECUTES_AT_LEAST:
                    executesAtLeast = Invariants.nonNull((Timestamp) diff.changes.get(EXECUTES_AT_LEAST));
                    break;
                case MIN_UNIQUE_HLC:
                    minUniqueHlc = (Long)diff.changes.get(MIN_UNIQUE_HLC);
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
                    waitingOn = Invariants.nonNull((CommandChange.WaitingOnBitSets) diff.changes.get(WAITING_ON));
                    break;
                case WRITES:
                    writes = Invariants.nonNull((Writes) diff.changes.get(WRITES));
                    break;
                case RESULT:
                    result = Invariants.nonNull((PersistableResult) diff.changes.get(RESULT));
                    break;
                case CLEANUP:
                    Cleanup nextCleanup = Invariants.nonNull((Cleanup) diff.changes.get(CLEANUP));
                    if (cleanup == null)
                        cleanup = nextCleanup;
                    else if (nextCleanup.compareTo(cleanup) > 0)
                        cleanup = nextCleanup;
                    break;
            }
        }
    }
}