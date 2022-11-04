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

import accord.api.Key;
import accord.local.Command;
import accord.local.CommandsForKey;
import accord.local.PartialCommand;
import accord.local.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static accord.local.CommandsForKey.CommandTimeseries.TestDep.*;
import static accord.local.CommandsForKey.CommandTimeseries.TestKind.RorWs;
import static accord.local.Status.PreAccepted;
import static accord.primitives.Txn.Kind.WRITE;

public class InMemoryCommandsForKey extends CommandsForKey
{
    public static class InMemoryCommandTimeseries<T> implements CommandTimeseries<T>
    {
        private final NavigableMap<Timestamp, Command> commands = new TreeMap<>();

        final Function<Command, T> map;

        InMemoryCommandTimeseries(Function<Command, T> map)
        {
            this.map = map;
        }

        @Override
        public void add(Timestamp timestamp, Command command)
        {
            if (commands.containsKey(timestamp) && !commands.get(timestamp).equals(command))
                throw new IllegalStateException(String.format("Attempting to overwrite command at timestamp %s %s with %s.",
                                                              timestamp, commands.get(timestamp), command));

            commands.put(timestamp, command);
        }

        @Override
        public void remove(Timestamp timestamp)
        {
            commands.remove(timestamp);
        }

        @Override
        public boolean isEmpty()
        {
            return commands.isEmpty();
        }

        @Override
        public Stream<T> before(@Nonnull Timestamp timestamp, @Nonnull TestKind testKind, @Nonnull TestDep testDep, @Nullable TxnId depId, @Nonnull TestStatus testStatus, @Nullable Status status)
        {
            return commands.headMap(timestamp, false).values().stream()
                    .filter(cmd -> testKind == RorWs || cmd.kind() == WRITE)
                    .filter(cmd -> testDep == ANY_DEPS || (cmd.partialDeps().contains(depId) ^ (testDep == WITHOUT)))
                    .filter(cmd -> TestStatus.test(cmd.status(), testStatus, status))
                    .map(map);
        }

        @Override
        public Stream<T> after(@Nonnull Timestamp timestamp, @Nonnull TestKind testKind, @Nonnull TestDep testDep, @Nullable TxnId depId, @Nonnull TestStatus testStatus, @Nullable Status status)
        {
            return commands.tailMap(timestamp, false).values().stream()
                    .filter(cmd -> testKind == RorWs || cmd.kind() == WRITE)
                    .filter(cmd -> testDep == ANY_DEPS || (cmd.partialDeps().contains(depId) ^ (testDep == WITHOUT)))
                    .filter(cmd -> TestStatus.test(cmd.status(), testStatus, status))
                    .map(map);
        }

        public Stream<Command> between(Timestamp min, Timestamp max)
        {
            return commands.subMap(min, true, max, true).values().stream();
        }

        public Stream<Command> all()
        {
            return commands.values().stream();
        }
    }

    // TODO (now): add validation that anything inserted into *committedBy* has everything prior in its dependencies
    private final Key key;
    private final InMemoryCommandTimeseries<TxnIdWithExecuteAt> uncommitted = new InMemoryCommandTimeseries<>(cmd -> new TxnIdWithExecuteAt(cmd.txnId(), cmd.executeAt()));
    private final InMemoryCommandTimeseries<TxnId> committedById = new InMemoryCommandTimeseries<>(Command::txnId);
    private final InMemoryCommandTimeseries<TxnId> committedByExecuteAt = new InMemoryCommandTimeseries<>(Command::txnId);

    private Timestamp max = Timestamp.NONE;

    public InMemoryCommandsForKey(Key key)
    {
        this.key = key;
    }

    @Override
    public Key key()
    {
        return key;
    }

    @Override
    public Timestamp max()
    {
        return max;
    }

    @Override
    public void updateMax(Timestamp timestamp)
    {
        max = Timestamp.max(max, timestamp);
    }

    @Override
    public InMemoryCommandTimeseries<TxnIdWithExecuteAt> uncommitted()
    {
        return uncommitted;
    }

    @Override
    public InMemoryCommandTimeseries<TxnId> committedById()
    {
        return committedById;
    }

    @Override
    public InMemoryCommandTimeseries<TxnId> committedByExecuteAt()
    {
        return committedByExecuteAt;
    }

    public void forWitnessed(Timestamp minTs, Timestamp maxTs, Consumer<Command> consumer)
    {
        uncommitted().between(minTs, maxTs)
                .filter(cmd -> cmd.hasBeen(PreAccepted)).forEach(consumer);
        committedById().between(minTs, maxTs).forEach(consumer);
        committedByExecuteAt().between(minTs, maxTs)
                .filter(cmd -> cmd.txnId().compareTo(minTs) < 0 || cmd.txnId().compareTo(maxTs) > 0).forEach(consumer);
    }
}
