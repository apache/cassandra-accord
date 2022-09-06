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
import java.util.function.Consumer;
import java.util.stream.Stream;

import accord.api.Key;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import com.google.common.collect.Iterators;

import static accord.utils.Utils.*;

public abstract class CommandsForKey implements Listener, Iterable<Command>
{
    public interface CommandTimeseries
    {
        Command get(Timestamp timestamp);
        void add(Timestamp timestamp, Command command);
        void remove(Timestamp timestamp);

        boolean isEmpty();

        /**
         * All commands before (exclusive of) the given timestamp
         */
        Stream<Command> before(Timestamp timestamp);

        /**
         * All commands after (exclusive of) the given timestamp
         */
        Stream<Command> after(Timestamp timestamp);

        /**
         * All commands between (inclusive of) the given timestamps
         */
        Stream<Command> between(Timestamp min, Timestamp max);

        Stream<Command> all();
    }

    public abstract Key key();
    public abstract CommandTimeseries uncommitted();
    public abstract CommandTimeseries committedById();
    public abstract CommandTimeseries committedByExecuteAt();

    public abstract Timestamp max();
    public abstract void updateMax(Timestamp timestamp);

    @Override
    public TxnOperation listenerScope(TxnId caller)
    {
        return TxnOperation.scopeFor(caller, listOf(key()));
    }

    @Override
    public void onChange(Command command)
    {
        updateMax(command.executeAt());
        switch (command.status())
        {
            case Applied:
            case Executed:
            case Committed:
                committedById().add(command.txnId(), command);
                committedByExecuteAt().add(command.executeAt(), command);
            case Invalidated:
                uncommitted().remove(command.txnId());
                command.removeListener(this);
                break;
        }
    }

    public void register(Command command)
    {
        updateMax(command.executeAt());
        uncommitted().add(command.txnId(), command);
        command.addListener(this);
    }

    public void forWitnessed(Timestamp minTs, Timestamp maxTs, Consumer<Command> consumer)
    {
        uncommitted().between(minTs, maxTs)
                .filter(cmd -> cmd.hasBeen(Status.PreAccepted)).forEach(consumer);
        committedById().between(minTs, maxTs).forEach(consumer);
        committedByExecuteAt().between(minTs, maxTs)
                .filter(cmd -> cmd.txnId().compareTo(minTs) < 0 || cmd.txnId().compareTo(maxTs) > 0).forEach(consumer);
    }

    @Override
    public Iterator<Command> iterator()
    {
        return Iterators.concat(uncommitted().all().iterator(), committedByExecuteAt().all().iterator());
    }

    public boolean isEmpty()
    {
        return uncommitted().isEmpty() && committedById().isEmpty();
    }
}
