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
import accord.primitives.Timestamp;

import java.util.Objects;

/**
 * Contains updates for both commands for key sets. One for deps, one for all,
 * and a common set affecting both
 */
public abstract class CommandsForKeyGroupUpdater<D>
{
    public static class Mutable<D> extends CommandsForKeyGroupUpdater<D>
    {
        private final CommandsForKeyUpdater.Mutable<D> deps;
        private final CommandsForKeyUpdater.Mutable<D> all;
        private final CommandsForKeyUpdater.Mutable<D> common;

        public Mutable(CommandsForKeyUpdater.Mutable<D> deps, CommandsForKeyUpdater.Mutable<D> all, CommandsForKeyUpdater.Mutable<D> common)
        {
            this.deps = deps;
            this.all = all;
            this.common = common;
        }

        public Mutable(CommandLoader<D> loader)
        {
            this(new CommandsForKeyUpdater.Mutable<>(loader),
                 new CommandsForKeyUpdater.Mutable<>(loader),
                 new CommandsForKeyUpdater.Mutable<>(loader));
        }

        @Override
        public CommandsForKeyUpdater.Mutable<D> deps()
        {
            return deps;
        }

        @Override
        public CommandsForKeyUpdater.Mutable<D> all()
        {
            return all;
        }

        @Override
        public CommandsForKeyUpdater.Mutable<D> common()
        {
            return common;
        }

        public Immutable<D> toImmutable()
        {
            return new Immutable<>(deps.toImmutable(), all.toImmutable(), common.toImmutable());
        }
    }

    public static class Immutable<D> extends CommandsForKeyGroupUpdater<D>
    {
        private final CommandsForKeyUpdater.Immutable<D> deps;
        private final CommandsForKeyUpdater.Immutable<D> all;
        private final CommandsForKeyUpdater.Immutable<D> common;

        public interface Factory<D, T extends Immutable<D>>
        {
            T create(CommandsForKeyUpdater.Immutable<D> deps, CommandsForKeyUpdater.Immutable<D> all, CommandsForKeyUpdater.Immutable<D> common);
        }

        public static <T> Factory<T, Immutable<T>> getFactory()
        {
            return Immutable::new;
        }

        public Immutable(CommandsForKeyUpdater.Immutable<D> deps, CommandsForKeyUpdater.Immutable<D> all, CommandsForKeyUpdater.Immutable<D> common)
        {
            this.deps = deps;
            this.all = all;
            this.common = common;
        }

        @Override
        public String toString()
        {
            return "Immutable{" +
                    "deps=" + deps +
                    ", all=" + all +
                    ", common=" + common +
                    '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Immutable<?> immutable = (Immutable<?>) o;
            return Objects.equals(deps, immutable.deps) && Objects.equals(all, immutable.all) && Objects.equals(common, immutable.common);
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CommandsForKeyUpdater.Immutable<D> deps()
        {
            return deps;
        }

        @Override
        public CommandsForKeyUpdater.Immutable<D> all()
        {
            return all;
        }

        @Override
        public CommandsForKeyUpdater.Immutable<D> common()
        {
            return common;
        }

        public Mutable<D> toMutable(CommandLoader<D> loader)
        {
            return new Mutable<>(deps.toMutable(loader), all.toMutable(loader), common.toMutable(loader));
        }

        private static <T extends Timestamp, D> void mergeCommon(CommandTimeseries.ImmutableUpdate<T, D> newCommon,
                                                                 CommandTimeseries.MutableUpdate<T, D> mergedCommon,
                                                                 CommandTimeseries.MutableUpdate<T, D> mergedDeps,
                                                                 CommandTimeseries.MutableUpdate<T, D> mergedAll)
        {
            newCommon.forEachWrite((k, v) -> {
                mergedCommon.addWriteUnsafe(k, v);
                mergedDeps.removeKeyUnsafe(k);
                mergedAll.removeKeyUnsafe(k);
            });
            newCommon.forEachDelete(k -> {
                mergedCommon.remove(k);
                mergedDeps.removeKeyUnsafe(k);
                mergedAll.removeKeyUnsafe(k);
            });
        }

        private static <T extends Timestamp, D> void mergeSpecific(CommandTimeseries.ImmutableUpdate<T, D> newSpecific,
                                                                   CommandTimeseries.MutableUpdate<T, D> mergedSpecific, CommandTimeseries.MutableUpdate<T, D> mergedOther, CommandTimeseries.MutableUpdate<T, D> mergedCommon)
        {
            newSpecific.forEachWrite((k, v) -> {
                mergedSpecific.addWriteUnsafe(k, v);
                mergedCommon.transferKeyTo(k, mergedOther);
            });

            newSpecific.forEachDelete(k -> {
                mergedSpecific.remove(k);
                mergedCommon.transferKeyTo(k, mergedOther);
            });
        }

        public static  <D, T extends Immutable<D>> T merge(T current, T update, Factory<D, T> factory)
        {
            if (current == null || current.isEmpty())
                return update;

            if (update == null || update.isEmpty())
                return current;

            Mutable<D> merged = current.toMutable(null);

            mergeCommon(update.common().commands(), merged.common.commands(), merged.deps.commands(), merged.all.commands());

            mergeSpecific(update.deps().commands(), merged.deps.commands(), merged.all.commands(), merged.common.commands());
            mergeSpecific(update.all().commands(), merged.all.commands(), merged.deps.commands(), merged.common.commands());

            return factory.create(merged.deps.toImmutable(), merged.all.toImmutable(), merged.common.toImmutable());
        }
    }

    public abstract CommandsForKeyUpdater<D> deps();

    public abstract CommandsForKeyUpdater<D> all();

    /**
     * Update the deps and all CFK groups.
     * In case conflicting updates have been applied to the common udpater and a specific one, the specific one should take precedence
     */
    public abstract CommandsForKeyUpdater<D> common();

    public boolean isEmpty()
    {
        return deps().isEmpty() && all().isEmpty() && common().isEmpty();
    }

    public CommandsForKey applyToDeps(CommandsForKey current)
    {
        current = common().apply(current);
        current = deps().apply(current);

        return current;
    }

    public CommandsForKey applyToAll(CommandsForKey current)
    {
        current = common().apply(current);
        current = all().apply(current);

        return current;
    }
}
