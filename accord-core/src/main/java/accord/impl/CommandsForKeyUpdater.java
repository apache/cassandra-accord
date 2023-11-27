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

import accord.impl.CommandTimeseries.CommandLoader;
import accord.primitives.TxnId;

import java.util.Objects;

/**
 * Contains updates for a single commands for key dataset (ie: deps OR all)
 * @param <D>
 */
public abstract class CommandsForKeyUpdater<D>
{
    public abstract CommandTimeseries.Update<TxnId, D> commands();

    public static class Mutable<D> extends CommandsForKeyUpdater<D>
    {
        private final CommandTimeseries.MutableUpdate<TxnId, D> commands;

        Mutable(CommandTimeseries.MutableUpdate<TxnId, D> commands)
        {
            this.commands = commands;
        }

        public Mutable(CommandLoader<D> loader)
        {
            this(new CommandTimeseries.MutableUpdate<>(loader));
        }

        @Override public CommandTimeseries.MutableUpdate<TxnId, D> commands() { return commands; }
        public Immutable<D> toImmutable()
        {
            return new Immutable<>(commands.toImmutable());
        }
    }

    public static class Immutable<D> extends CommandsForKeyUpdater<D>
    {
        private static final Immutable<Object> EMPTY = new Immutable<>(CommandTimeseries.ImmutableUpdate.empty());
        private final CommandTimeseries.ImmutableUpdate<TxnId, D> commands;

        public Immutable(CommandTimeseries.ImmutableUpdate<TxnId, D> commands)
        {
            this.commands = commands;
        }

        @Override
        public String toString()
        {
            return "Immutable{" +
                    "commands=" + commands +
                    '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Immutable<?> immutable = (Immutable<?>) o;
            return Objects.equals(commands, immutable.commands);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(commands);
        }

        public static <D> Immutable<D> empty()
        {
            return (Immutable<D>) EMPTY;
        }

        @Override public CommandTimeseries.ImmutableUpdate<TxnId, D> commands() { return commands; }

        public Mutable<D> toMutable(CommandLoader<D> loader)
        {
            return new Mutable<>(commands.toMutable(loader));
        }
    }

    public int totalChanges()
    {
        return commands().numChanges();
    }

    public boolean isEmpty()
    {
        return commands().isEmpty();
    }

    public CommandsForKey apply(CommandsForKey current)
    {
        if (commands().isEmpty())
            return current;
        return current.update(commands());
    }
}
