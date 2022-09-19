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

import java.util.stream.Stream;

import accord.api.Key;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static accord.utils.Utils.*;

public abstract class CommandsForKey implements CommandListener
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsForKey.class);

    public interface CommandTimeseries<T>
    {
        void add(Timestamp timestamp, Command command);
        void remove(Timestamp timestamp);

        boolean isEmpty();

        enum TestDep { WITH, WITHOUT, ANY_DEPS }
        enum TestStatus
        {
            IS, HAS_BEEN, ANY_STATUS;
            public static boolean test(Status test, TestStatus predicate, Status param)
            {
                return predicate == ANY_STATUS || (predicate == IS ? test == param : test.hasBeen(param));
            }
        }
        enum TestKind { Ws, RorWs}

        /**
         * All commands before (exclusive of) the given timestamp
         *
         * TODO (soon): TestDep should be asynchronous; data should not be kept memory-resident as only used for recovery
         */
        Stream<T> before(Timestamp timestamp, TestKind testKind, TestDep testDep, @Nullable TxnId depId, TestStatus testStatus, @Nullable Status status);

        /**
         * All commands after (exclusive of) the given timestamp
         */
        Stream<T> after(Timestamp timestamp, TestKind testKind, TestDep testDep, @Nullable TxnId depId, TestStatus testStatus, @Nullable Status status);
    }

    public static class TxnIdWithExecuteAt
    {
        public final TxnId txnId;
        public final Timestamp executeAt;

        public TxnIdWithExecuteAt(TxnId txnId, Timestamp executeAt)
        {
            this.txnId = txnId;
            this.executeAt = executeAt;
        }
    }

    public abstract Key key();
    public abstract CommandTimeseries<? extends TxnIdWithExecuteAt> uncommitted();
    public abstract CommandTimeseries<TxnId> committedById();
    public abstract CommandTimeseries<TxnId> committedByExecuteAt();

    public abstract Timestamp max();
    protected abstract void updateMax(Timestamp timestamp);

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        return PreLoadContext.contextFor(caller, listOf(key()));
    }

    @Override
    public void onChange(SafeCommandStore safeStore, Command command)
    {
        logger.trace("[{}]: updating as listener in response to change on {} with status {} ({})",
                     key(), command.txnId(), command.status(), command);
        updateMax(command.executeAt());
        switch (command.status())
        {
            case Applied:
            case PreApplied:
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

    public boolean isEmpty()
    {
        return uncommitted().isEmpty() && committedById().isEmpty();
    }
}
