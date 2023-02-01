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
import accord.local.SafeCommandStore.SearchFunction;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestKind;
import accord.local.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import javax.annotation.Nullable;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static accord.local.SafeCommandStore.TestDep.*;
import static accord.local.SafeCommandStore.TestKind.Ws;
import static accord.local.Status.KnownDeps.DepsUnknown;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.PreCommitted;

public class InMemoryCommandsForKey extends CommandsForKey
{
    public static class InMemoryCommandTimeseries implements CommandTimeseries
    {
        private final NavigableMap<Timestamp, Command> commands = new TreeMap<>();
        private final Key key;

        public InMemoryCommandTimeseries(Key key)
        {
            this.key = key;
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
        public <T> T mapReduce(TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                               TestDep testDep, @Nullable TxnId depId,
                               @Nullable Status minStatus, @Nullable Status maxStatus,
                               SearchFunction<T, T> map, T initialValue, T terminalValue)
        {

            for (Command cmd : (testTimestamp == TestTimestamp.BEFORE ? commands.headMap(timestamp, false) : commands.tailMap(timestamp, false)).values())
            {
                if (testKind == Ws && cmd.txnId().isRead()) continue;
                    // If we don't have any dependencies, we treat a dependency filter as a mismatch
                if (testDep != ANY_DEPS && (!cmd.known().deps.hasProposedOrDecidedDeps() || (cmd.partialDeps().contains(depId) != (testDep == WITH))))
                    continue;
                if (minStatus != null && minStatus.compareTo(cmd.status()) > 0)
                    continue;
                if (maxStatus != null && maxStatus.compareTo(cmd.status()) < 0)
                    continue;
                initialValue = map.apply(key, cmd.txnId(), cmd.executeAt(), initialValue);
                if (initialValue.equals(terminalValue))
                    break;
            }
            return initialValue;
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
    private final InMemoryCommandTimeseries byId;
    private final InMemoryCommandTimeseries byExecuteAt;

    private Timestamp max = Timestamp.NONE;

    public InMemoryCommandsForKey(Key key)
    {
        this.key = key;
        this.byId = new InMemoryCommandTimeseries(key);
        this.byExecuteAt = new InMemoryCommandTimeseries(key);
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
    public InMemoryCommandTimeseries byId()
    {
        return byId;
    }

    @Override
    public InMemoryCommandTimeseries byExecuteAt()
    {
        return byExecuteAt;
    }

    public void forWitnessed(Timestamp minTs, Timestamp maxTs, Consumer<Command> consumer)
    {
        byId.between(minTs, maxTs).filter(cmd -> cmd.hasBeen(PreAccepted)).forEach(consumer);
        byExecuteAt.between(minTs, maxTs).filter(cmd -> cmd.hasBeen(PreCommitted)).forEach(consumer);
    }
}
