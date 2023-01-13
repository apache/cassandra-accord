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
import accord.local.*;
import accord.local.SafeCommandStore.SearchFunction;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestKind;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public abstract class CommandsForKey implements CommandListener
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsForKey.class);

    public interface CommandTimeseries
    {
        void add(Timestamp timestamp, Command command);
        void remove(Timestamp timestamp);

        boolean isEmpty();

        enum TestTimestamp { BEFORE, AFTER }

        /**
         * All commands before/after (exclusive of) the given timestamp
         *
         * Note that {@code testDep} applies only to commands that know at least proposed deps; if specified any
         * commands that do not know any deps will be ignored.
         *
         * TODO (expected, efficiency): TestDep should be asynchronous; data should not be kept memory-resident as only used for recovery
         */
        <T> T mapReduce(TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                        TestDep testDep, @Nullable TxnId depId,
                        @Nullable Status minStatus, @Nullable Status maxStatus,
                        SearchFunction<T, T> map, T initialValue, T terminalValue);
    }

    public abstract Key key();
    public abstract CommandTimeseries byId();
    public abstract CommandTimeseries byExecuteAt();

    public abstract Timestamp max();
    protected abstract void updateMax(Timestamp timestamp);

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        return PreLoadContext.contextFor(caller, Keys.of(key()));
    }

    @Override
    public void onChange(SafeCommandStore safeStore, Command command)
    {
        logger.trace("[{}]: updating as listener in response to change on {} with status {} ({})",
                     key(), command.txnId(), command.status(), command);
        updateMax(command.executeAt());
        switch (command.status())
        {
            default: throw new AssertionError();
            case PreAccepted:
            case NotWitnessed:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
                break;
            case Applied:
            case PreApplied:
            case Committed:
            case ReadyToExecute:
                byExecuteAt().remove(command.txnId());
                byExecuteAt().add(command.executeAt(), command);
                break;
            case Invalidated:
                byId().remove(command.txnId());
                byExecuteAt().remove(command.txnId());
                command.removeListener(this);
                break;
        }
    }

    public void register(Command command)
    {
        updateMax(command.executeAt());
        byId().add(command.txnId(), command);
        byExecuteAt().add(command.txnId(), command);
        command.addListener(this);
    }

    public boolean isEmpty()
    {
        return byId().isEmpty();
    }
}
