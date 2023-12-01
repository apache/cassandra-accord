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

import accord.impl.CommandsForKeyGroupUpdater.Immutable;
import accord.local.Command;
import accord.local.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import com.google.common.collect.ImmutableSortedSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CommandsForKeyGroupUpdaterTest
{
    private static class Sentinel { private static final Sentinel INSTANCE = new Sentinel(); }

    private static final CommandTimeseries.CommandLoader<Sentinel> LOADER = new CommandTimeseries.CommandLoader<Sentinel>()
    {
        @Override public Sentinel saveForCFK(Command command) { return Sentinel.INSTANCE; }
        @Override public TxnId txnId(Sentinel data) { throw new UnsupportedOperationException(); }
        @Override public Timestamp executeAt(Sentinel data) { throw new UnsupportedOperationException(); }
        @Override public SaveStatus saveStatus(Sentinel data) { throw new UnsupportedOperationException(); }
        @Override public List<TxnId> depsIds(Sentinel data) { throw new UnsupportedOperationException(); }
    };

    private static final IntKey.Raw KEY = IntKey.key(1);

    private static TxnId txnId(int i)
    {
        return TxnId.fromValues(0, i, 0, 1);
    }

    private static Timestamp ts(int i)
    {
        return Timestamp.fromValues(0, i, 0, 1);
    }

    private static CommandTimeseries<Sentinel> timeseries()
    {
        CommandTimeseries.Builder<Sentinel> builder = new CommandTimeseries.Builder<>(KEY, LOADER);
        return builder.build();
    }

    private static CommandTimeseries<Sentinel> timeseries(Timestamp... timestamps)
    {
        CommandTimeseries.Builder<Sentinel> builder = new CommandTimeseries.Builder<>(KEY, LOADER);
        for (Timestamp timestamp : timestamps)
            builder.add(timestamp, Sentinel.INSTANCE);
        return builder.build();
    }

    private static CommandTimeseries<Sentinel> timeseries(int... timestamps)
    {
        return timeseries(timestamps(timestamps));
    }

    private static CommandsForKey cfk(CommandTimeseries<Sentinel> commands)
    {
        return new CommandsForKey(KEY, commands);
    }

    private static CommandsForKey cfk(Timestamp[] byId)
    {
        return cfk(timeseries(byId));
    }

    private static CommandsForKey cfk()
    {
        return new CommandsForKey(KEY, timeseries());
    }

    private static Timestamp[] timestamps(int... values)
    {
        Timestamp[] timestamps = new Timestamp[values.length];
        for (int i=0; i<values.length; i++)
            timestamps[i] = ts(values[i]);
        return timestamps;
    }

    private static void assertTimeseries(CommandTimeseries<?> timeseries, Timestamp... expected)
    {
        Assertions.assertEquals(ImmutableSortedSet.copyOf(expected), timeseries.commands.keySet());
    }

    private static void assertTimeseries(CommandTimeseries<?> timeseries, int... expected)
    {
        assertTimeseries(timeseries, timestamps(expected));
    }

    @Test
    void commonTest()
    {
        CommandsForKeyGroupUpdater.Mutable<Sentinel> updater = new CommandsForKeyGroupUpdater.Mutable<>(LOADER);

        updater.common().commands().add(txnId(1), null);

        Assertions.assertEquals(cfk(timeseries(1)), updater.applyToDeps(cfk()));
        Assertions.assertEquals(cfk(timeseries(1)), updater.applyToAll(cfk()));
    }

    @Test
    void specificPrecedence()
    {
        CommandsForKeyGroupUpdater.Mutable<Sentinel> updater = new CommandsForKeyGroupUpdater.Mutable<>(LOADER);

        updater.common().commands().add(txnId(2), null);
        updater.common().commands().add(txnId(1), null);
        updater.all().commands().remove(txnId(1));

        Assertions.assertEquals(cfk(timeseries(1, 2)), updater.applyToDeps(cfk()));
        Assertions.assertEquals(cfk(timeseries(2)), updater.applyToAll(cfk()));
    }

    /**
     * A later specifc update should override a previous common update (making it specific to the opposite cfk)
     */
    @Test
    void mergeOverridingSpecific()
    {
        CommandsForKeyGroupUpdater.Mutable<Sentinel> original = new CommandsForKeyGroupUpdater.Mutable<>(LOADER);

        original.common().commands().add(txnId(1), null);
        original.common().commands().add(txnId(2), null);

        CommandsForKeyGroupUpdater.Mutable<Sentinel> update = new CommandsForKeyGroupUpdater.Mutable<>(LOADER);
        update.deps().commands().remove(txnId(1));
        update.deps().commands().add(txnId(3), null);

        CommandsForKeyGroupUpdater.Mutable<Sentinel> expected = new CommandsForKeyGroupUpdater.Mutable<>(LOADER);

        expected.common().commands().add(txnId(2), null);
        expected.deps().commands().remove(txnId(1));
        expected.deps().commands().add(txnId(3), null);
        expected.all().commands().add(txnId(1), null);

//        Immutable<Sentinel> merged = original.toImmutable().mergeNewer(update.toImmutable());
        Immutable<Sentinel> merged = Immutable.merge(original.toImmutable(), update.toImmutable(), Immutable.getFactory());
        Assertions.assertEquals(expected.toImmutable(), merged);
    }

    @Test
    void mergeOverridingCommon()
    {
        CommandsForKeyGroupUpdater.Mutable<Sentinel> original = new CommandsForKeyGroupUpdater.Mutable<>(LOADER);
        original.deps().commands().add(txnId(1), null);
        original.deps().commands().add(txnId(2), null);
        original.all().commands().add(txnId(2), null);

        CommandsForKeyGroupUpdater.Mutable<Sentinel> update = new CommandsForKeyGroupUpdater.Mutable<>(LOADER);
        update.common().commands().remove(txnId(1));

        CommandsForKeyGroupUpdater.Mutable<Sentinel> expected = new CommandsForKeyGroupUpdater.Mutable<>(LOADER);
        expected.deps().commands().add(txnId(2), null);
        expected.all().commands().add(txnId(2), null);
        expected.common().commands().remove(txnId(1));

        Immutable<Sentinel> merged = Immutable.merge(original.toImmutable(), update.toImmutable(), Immutable.getFactory());
        Assertions.assertEquals(expected.toImmutable(), merged);
    }
}

