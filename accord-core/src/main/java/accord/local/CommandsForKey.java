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

package accord.local;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import accord.primitives.Timestamp;
import com.google.common.collect.Iterators;

public class CommandsForKey implements Listener, Iterable<Command>
{
    // TODO: efficiency
    public final NavigableMap<Timestamp, Command> uncommitted = new TreeMap<>();
    public final NavigableMap<Timestamp, Command> committedById = new TreeMap<>();
    public final NavigableMap<Timestamp, Command> committedByExecuteAt = new TreeMap<>();

    private Timestamp max = Timestamp.NONE;

    public Timestamp max()
    {
        return max;
    }

    @Override
    public void onChange(Command command)
    {
        max = Timestamp.max(max, command.executeAt());
        switch (command.status())
        {
            case Applied:
            case Executed:
            case Committed:
                committedById.put(command.txnId(), command);
                committedByExecuteAt.put(command.executeAt(), command);
            case Invalidated:
                uncommitted.remove(command.txnId());
                command.removeListener(this);
                break;
        }
    }

    public void register(Command command)
    {
        max = Timestamp.max(max, command.executeAt());
        uncommitted.put(command.txnId(), command);
        command.addListener(this);
    }

    public void forWitnessed(Timestamp minTs, Timestamp maxTs, Consumer<Command> consumer)
    {
        uncommitted.subMap(minTs, true, maxTs, true).values().stream()
                .filter(cmd -> cmd.hasBeen(Status.PreAccepted)).forEach(consumer);
        committedById.subMap(minTs, true, maxTs, true).values().forEach(consumer);
        committedByExecuteAt.subMap(minTs, true, maxTs, true).values().stream()
                .filter(cmd -> cmd.txnId().compareTo(minTs) < 0 || cmd.txnId().compareTo(maxTs) > 0).forEach(consumer);
    }

    @Override
    public Iterator<Command> iterator()
    {
        return Iterators.concat(uncommitted.values().iterator(), committedByExecuteAt.values().iterator());
    }

    public boolean isEmpty()
    {
        return uncommitted.isEmpty() && committedById.isEmpty();
    }
}
