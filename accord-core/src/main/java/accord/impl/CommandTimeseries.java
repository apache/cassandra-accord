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

import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSortedMap;

import accord.api.Key;
import accord.local.Command;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.utils.Utils.ensureSortedImmutable;
import static accord.utils.Utils.ensureSortedMutable;

public class CommandTimeseries<D>
{
    public enum TestTimestamp { BEFORE, AFTER }

    private final Seekable keyOrRange;
    protected final CommandLoader<D> loader;
    public final ImmutableSortedMap<Timestamp, D> commands;

    public CommandTimeseries(Update<D> builder)
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
        return commands.isEmpty() ? Timestamp.NONE : commands.keySet().last();
    }
    public Timestamp minTimestamp()
    {
        return commands.isEmpty() ? Timestamp.NONE : commands.keySet().first();
    }

    /**
     * All commands before/after (exclusive of) the given timestamp
     * <p>
     * Note that {@code testDep} applies only to commands that know at least proposed deps; if specified any
     * commands that do not know any deps will be ignored.
     * <p>
     * TODO (expected, efficiency): TestDep should be asynchronous; data should not be kept memory-resident as only used for recovery
     */
    public <T> T mapReduce(SafeCommandStore.TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                           SafeCommandStore.TestDep testDep, @Nullable TxnId depId,
                           @Nullable Status minStatus, @Nullable Status maxStatus,
                           SafeCommandStore.CommandFunction<T, T> map, T initialValue, T terminalValue)
    {

        for (D data : (testTimestamp == TestTimestamp.BEFORE ? commands.headMap(timestamp, false) : commands.tailMap(timestamp, false)).values())
        {
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
            Timestamp executeAt = loader.executeAt(data);
            initialValue = map.apply(keyOrRange, txnId, executeAt, initialValue);
            if (initialValue.equals(terminalValue))
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

    Update<D> beginUpdate()
    {
        return new Update<>(this);
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

    public static class Update<D>
    {
        private final Seekable keyOrRange;
        protected CommandLoader<D> loader;
        protected NavigableMap<Timestamp, D> commands;

        public Update(Seekable keyOrRange, CommandLoader<D> loader)
        {
            this.keyOrRange = keyOrRange;
            this.loader = loader;
            this.commands = new TreeMap<>();
        }

        public Update(CommandTimeseries<D> timeseries)
        {
            this.keyOrRange = timeseries.keyOrRange;
            this.loader = timeseries.loader;
            this.commands = timeseries.commands;
        }

        public Update<D> add(Timestamp timestamp, Command command)
        {
            commands = ensureSortedMutable(commands);
            commands.put(timestamp, loader.saveForCFK(command));
            return this;
        }

        public Update<D> add(Timestamp timestamp, D value)
        {
            commands = ensureSortedMutable(commands);
            commands.put(timestamp, value);
            return this;
        }

        public Update<D> remove(Timestamp timestamp)
        {
            commands = ensureSortedMutable(commands);
            commands.remove(timestamp);
            return this;
        }

        public Update<D> removeBefore(Timestamp timestamp)
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
}
