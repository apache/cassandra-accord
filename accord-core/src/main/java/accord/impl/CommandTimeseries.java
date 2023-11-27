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

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;

import accord.api.Key;
import accord.local.Command;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;

import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.utils.Utils.ensureSortedImmutable;
import static accord.utils.Utils.ensureSortedMutable;

public class CommandTimeseries<D>
{
    public enum TestTimestamp { BEFORE, AFTER }
    public enum TimestampType { TXN_ID, EXECUTE_AT }

    private final Seekable keyOrRange;
    protected final CommandLoader<D> loader;
    public final ImmutableSortedMap<Timestamp, D> commands;

    public CommandTimeseries(Builder<D> builder)
    {
        this.keyOrRange = builder.keyOrRange;
        this.loader = builder.loader;
        this.commands = ensureSortedImmutable(builder.commands);
    }

    CommandTimeseries(Seekable keyOrRange, CommandLoader<D> loader, ImmutableSortedMap<Timestamp, D> commands)
    {
        this.keyOrRange = keyOrRange;
        this.loader = loader;
        this.commands = commands;
    }

    public CommandTimeseries(Key keyOrRange, CommandLoader<D> loader)
    {
        this(keyOrRange, loader, ImmutableSortedMap.of());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandTimeseries<?> that = (CommandTimeseries<?>) o;
        return keyOrRange.equals(that.keyOrRange) && loader.equals(that.loader) && commands.equals(that.commands);
    }

    @Override
    public int hashCode()
    {
        int hash = 1;
        hash = 31 * hash + Objects.hashCode(keyOrRange);
        hash = 31 * hash + Objects.hashCode(loader);
        hash = 31 * hash + Objects.hashCode(commands);
        return hash;
    }

    public D get(Timestamp key)
    {
        return commands.get(key);
    }

    public boolean isEmpty()
    {
        return commands.isEmpty();
    }

    public Timestamp maxTimestamp()
    {
        if (commands.isEmpty())
            return Timestamp.NONE;
        Timestamp result = null;
        for (D data : commands.values())
        {
            if (result == null)
            {
                result = Timestamp.max(loader.txnId(data), loader.executeAt(data));
            }
            else
            {
                result = Timestamp.max(result, loader.txnId(data));
                result = Timestamp.max(result, loader.executeAt(data));
            }
        }
        return result;
    }

    public Timestamp minTimestamp()
    {
        if (commands.isEmpty())
            return Timestamp.NONE;
        Timestamp result = null;
        for (D data : commands.values())
        {
            if (result == null)
            {
                result = Timestamp.min(loader.txnId(data), loader.executeAt(data));
            }
            else
            {
                result = Timestamp.min(result, loader.txnId(data));
                result = Timestamp.min(result, loader.executeAt(data));
            }
        }
        return result;
    }

    /**
     * All commands before/after (exclusive of) the given timestamp
     * <p>
     * Note that {@code testDep} applies only to commands that know at least proposed deps; if specified any
     * commands that do not know any deps will be ignored.
     * <p>
     * TODO (expected, efficiency): TestDep should be asynchronous; data should not be kept memory-resident as only used for recovery
     */
    public <P1, T> T mapReduce(Kinds testKind, TimestampType timestampType, TestTimestamp testTimestamp, Timestamp timestamp,
                           SafeCommandStore.TestDep testDep, @Nullable TxnId depId,
                           @Nullable Status minStatus, @Nullable Status maxStatus,
                           SafeCommandStore.CommandFunction<P1, T, T> map, P1 p1, T initialValue, Predicate<? super T> terminatePredicate)
    {
        Iterable<D> dataIterable;
        Predicate<Timestamp> executeAtPredicate;
        if (timestampType == TimestampType.TXN_ID)
        {
            dataIterable = testTimestamp == TestTimestamp.BEFORE ? commands.headMap(timestamp, false).values() : commands.tailMap(timestamp, false).values();
            executeAtPredicate = ts -> true;
        }
        else
        {
            dataIterable = commands.values();
            executeAtPredicate = testTimestamp == TestTimestamp.BEFORE ? ts -> ts.compareTo(timestamp) < 0 : ts -> ts.compareTo(timestamp) > 0;
        }


        for (D data : dataIterable)
        {
            Timestamp executeAt = loader.executeAt(data);
            if (!executeAtPredicate.test(executeAt))
                continue;

            TxnId txnId = loader.txnId(data);
            if (!testKind.test(txnId.rw())) continue;
            SaveStatus status = loader.saveStatus(data);
            if (minStatus != null && minStatus.compareTo(status.status) > 0)
                continue;
            if (maxStatus != null && maxStatus.compareTo(status.status) < 0)
                continue;
            List<TxnId> deps = loader.depsIds(data);
            // If we don't have any dependencies, we treat a dependency filter as a mismatch
            if (testDep != ANY_DEPS && (!status.known.deps.hasProposedOrDecidedDeps() || (deps.contains(depId) != (testDep == WITH))))
                continue;
            initialValue = map.apply(p1, keyOrRange, txnId, executeAt, status.status, () -> deps, initialValue);
            if (terminatePredicate.test(initialValue))
                break;
        }
        return initialValue;
    }

    public Timestamp maxExecuteAtBefore(Timestamp before)
    {
        return commands.headMap(before).values().stream().map(loader::executeAt)
                       .filter(Objects::nonNull).reduce(Timestamp::max).orElse(before);
    }

    Stream<TxnId> between(Timestamp min, Timestamp max, Predicate<Status> statusPredicate)
    {
        return commands.subMap(min, true, max, true).values().stream()
                       .filter(d -> statusPredicate.test(loader.status(d))).map(loader::txnId);
    }

    public Stream<D> all()
    {
        return commands.values().stream();
    }

    Builder<D> unbuild()
    {
        return new Builder<>(this);
    }

    public CommandLoader<D> loader()
    {
        return loader;
    }

    public interface CommandLoader<D>
    {
        D saveForCFK(Command command);

        TxnId txnId(D data);
        Timestamp executeAt(D data);
        SaveStatus saveStatus(D data);
        List<TxnId> depsIds(D data);

        default Status status(D data)
        {
            return saveStatus(data).status;
        }

        default Status.Known known(D data)
        {
            return saveStatus(data).known;
        }
    }

    public static class Builder<D>
    {
        private final Seekable keyOrRange;
        protected CommandLoader<D> loader;
        protected NavigableMap<Timestamp, D> commands;

        public Builder(Seekable keyOrRange, CommandLoader<D> loader)
        {
            this.keyOrRange = keyOrRange;
            this.loader = loader;
            this.commands = new TreeMap<>();
        }

        public Builder(CommandTimeseries<D> timeseries)
        {
            this.keyOrRange = timeseries.keyOrRange;
            this.loader = timeseries.loader;
            this.commands = timeseries.commands;
        }

        public Builder<D> add(Timestamp timestamp, Command command)
        {
            commands = ensureSortedMutable(commands);
            commands.put(timestamp, loader.saveForCFK(command));
            return this;
        }

        public Builder<D> add(Timestamp timestamp, D value)
        {
            commands = ensureSortedMutable(commands);
            commands.put(timestamp, value);
            return this;
        }

        public Builder<D> remove(Timestamp timestamp)
        {
            commands = ensureSortedMutable(commands);
            commands.remove(timestamp);
            return this;
        }

        public Builder<D> removeBefore(Timestamp timestamp)
        {
            commands = ensureSortedMutable(commands);
            commands.headMap(timestamp, false).clear();
            return this;
        }

        public CommandTimeseries<D> build()
        {
            return new CommandTimeseries<>(this);
        }
    }

    public interface Update<T extends Timestamp, D>
    {
        interface Mutable<T extends Timestamp, D> extends Update<T, D>
        {
            void add(T ts, Command command);
            void remove(T ts);
        }

        int numWrites();
        int numDeletes();

        boolean contains(T key);

        default int numChanges()
        {
            return numWrites() + numDeletes();
        }

        default boolean isEmpty()
        {
            return numChanges() == 0;
        }

        void forEachWrite(BiConsumer<T, D> consumer);
        void forEachDelete(Consumer<T> consumer);

        default  CommandTimeseries<D> apply(CommandTimeseries<D> current)
        {
            if (isEmpty())
                return current;

            CommandTimeseries.Builder<D> builder = current.unbuild();
            forEachWrite(builder::add);
            forEachDelete(builder::remove);
            return builder.build();
        }

    }

    public static class ImmutableUpdate<T extends Timestamp, D> implements Update<T, D>
    {
        private static final ImmutableUpdate<?, ?> EMPTY = new ImmutableUpdate<>(ImmutableMap.of(), ImmutableSet.of());

        public final ImmutableMap<T, D> writes;
        public final ImmutableSet<T> deletes;

        public ImmutableUpdate(ImmutableMap<T, D> writes, ImmutableSet<T> deletes)
        {
            this.writes = writes;
            this.deletes = deletes;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ImmutableUpdate<?, ?> immutable = (ImmutableUpdate<?, ?>) o;
            return Objects.equals(writes, immutable.writes) && Objects.equals(deletes, immutable.deletes);
        }

        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        public String toString()
        {
            return "Update.Immutable{" +
                   "writes=" + writes +
                   ", deletes=" + deletes +
                   '}';
        }

        public static <T extends Timestamp, D> ImmutableUpdate<T, D> empty()
        {
            return (ImmutableUpdate<T, D>) EMPTY;
        }

        @Override
        public int numWrites()
        {
            return writes.size();
        }

        @Override
        public void forEachWrite(BiConsumer<T, D> consumer)
        {
            writes.forEach(consumer);
        }

        @Override
        public int numDeletes()
        {
            return deletes.size();
        }

        @Override
        public void forEachDelete(Consumer<T> consumer)
        {
            deletes.forEach(consumer);
        }

        public boolean contains(T key)
        {
            return writes.containsKey(key) || deletes.contains(key);
        }

        public ImmutableUpdate<T, D> apply(Update<T, D> next)
        {
            if (next.isEmpty())
                return this;

            Map<T, D> writes = new HashMap<>(this.writes);
            Set<T> deletes = new HashSet<>(this.deletes);

            next.forEachDelete(k -> {
                writes.remove(k);
                deletes.add(k);
            });

            next.forEachWrite((k, v) -> {
                writes.put(k , v);
                deletes.remove(k);
            });

            return new ImmutableUpdate<>(ImmutableMap.copyOf(writes), ImmutableSet.copyOf(deletes));
        }

        public MutableUpdate<T, D> toMutable(CommandLoader<D> loader)
        {
            return new MutableUpdate<>(loader, new HashMap<>(writes), new HashSet<>(deletes));
        }
    }

    public static class MutableUpdate<T extends Timestamp, D> implements Update.Mutable<T, D>
    {
        private final CommandLoader<D> loader;
        private final Map<T, D> writes;
        private final Set<T> deletes;

        public MutableUpdate(CommandLoader<D> loader, Map<T, D> writes, Set<T> deletes)
        {
            this.loader = loader;
            this.writes = writes;
            this.deletes = deletes;
        }

        public MutableUpdate(CommandLoader<D> loader)
        {
            this(loader, new HashMap<>(), new HashSet<>());
        }

        @Override
        public void add(T ts, Command command)
        {
            writes.put(ts, loader.saveForCFK(command));
            deletes.remove(ts);
        }

        @Override
        public void remove(T ts)
        {
            deletes.add(ts);
            writes.remove(ts);
        }

        @Override
        public int numWrites()
        {
            return writes.size();
        }

        @Override
        public void forEachWrite(BiConsumer<T, D> consumer)
        {
            writes.forEach(consumer);
        }

        @Override
        public int numDeletes()
        {
            return deletes.size();
        }

        @Override
        public void forEachDelete(Consumer<T> consumer)
        {
            deletes.forEach(consumer);
        }

        public boolean contains(T key)
        {
            return writes.containsKey(key) || deletes.contains(key);
        }

        /**
         * remove the given key from both the writes and deletes set
         * @param key
         */
        void removeKeyUnsafe(T key)
        {
            writes.remove(key);
            deletes.remove(key);
        }

        /**
         * add the given write data directly, without using the command loader
         * @param key
         * @param value
         */
        void addWriteUnsafe(T key, D value)
        {
            writes.put(key, value);
            deletes.remove(key);
        }

        /**
         * if this update has a write or delete for the given key, remove
         * it from this update and put it in the destination update
         */
        boolean transferKeyTo(T key, MutableUpdate<T, D> dst)
        {
            if (writes.containsKey(key))
            {
                dst.writes.put(key, this.writes.remove(key));
                return true;
            }
            else if (deletes.remove(key))
            {
                dst.deletes.add(key);
                return true;
            }
            return false;
        }

        public ImmutableUpdate<T, D> toImmutable()
        {
            return new ImmutableUpdate<>(ImmutableMap.copyOf(writes), ImmutableSet.copyOf(deletes));
        }
    }
}
