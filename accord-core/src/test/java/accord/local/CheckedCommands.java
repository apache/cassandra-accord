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

import javax.annotation.Nullable;

import accord.api.Result;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;

import java.util.function.BiConsumer;

import static accord.utils.Invariants.illegalState;

public class CheckedCommands
{
    public static void preaccept(SafeCommandStore safeStore, TxnId txnId, PartialTxn partialTxn, FullRoute<?> route)
    {
        preaccept(safeStore, txnId, partialTxn, route, (l, r) -> {});
    }

    public static void preaccept(SafeCommandStore safeStore, TxnId txnId, PartialTxn partialTxn, FullRoute<?> route, BiConsumer<Command, Command> consumer)
    {
        SafeCommand safeCommand = safeStore.get(txnId, txnId, route);
        Command before = safeCommand.current();
        Commands.AcceptOutcome result = Commands.preaccept(safeStore, safeCommand, txnId, txnId.epoch(), partialTxn, route);
        Command after = safeCommand.current();
        if (result != Commands.AcceptOutcome.Success) throw illegalState("Command mutation rejected: " + result);
        consumer.accept(before, after);
    }

    public static void accept(SafeCommandStore safeStore, TxnId txnId, Ballot ballot, Route<?> route, Seekables<?, ?> keys, Timestamp executeAt, PartialDeps partialDeps)
    {
        accept(safeStore, txnId, ballot, route, keys, executeAt, partialDeps, (l, r) -> {});
    }

    public static void accept(SafeCommandStore safeStore, TxnId txnId, Ballot ballot, Route<?> route, Seekables<?, ?> keys, Timestamp executeAt, PartialDeps partialDeps, BiConsumer<Command, Command> consumer)
    {
        SafeCommand safeCommand = safeStore.get(txnId, txnId, route);
        Command before = safeCommand.current();
        Commands.AcceptOutcome result = Commands.accept(safeStore, txnId, ballot, route, keys, executeAt, partialDeps);
        Command after = safeCommand.current();
        if (result != Commands.AcceptOutcome.Success) throw illegalState("Command mutation rejected: " + result);
        consumer.accept(before, after);
    }

    public static void commit(SafeCommandStore safeStore, SaveStatus saveStatus, Ballot ballot, TxnId txnId, Route<?> route, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        commit(safeStore, saveStatus, ballot, txnId, route, partialTxn, executeAt, partialDeps, (l, r) -> {});
    }

    public static void commit(SafeCommandStore safeStore, SaveStatus saveStatus, Ballot ballot, TxnId txnId, Route<?> route, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps, BiConsumer<Command, Command> consumer)
    {
        SafeCommand safeCommand = safeStore.get(txnId, txnId, route);
        Command before = safeCommand.current();
        Commands.CommitOutcome result = Commands.commit(safeStore, safeCommand, saveStatus, ballot, txnId, route, partialTxn, executeAt, partialDeps);
        Command after = safeCommand.current();
        if (result != Commands.CommitOutcome.Success) throw illegalState("Command mutation rejected: " + result);
        consumer.accept(before, after);
    }

    public static void apply(SafeCommandStore safeStore, TxnId txnId, Route<?> route, Timestamp executeAt, @Nullable PartialDeps partialDeps, @Nullable PartialTxn partialTxn, Writes writes, Result result)
    {
        apply(safeStore, txnId, route, executeAt, partialDeps, partialTxn, writes, result, (l, r) -> {});
    }

    public static void apply(SafeCommandStore safeStore, TxnId txnId, Route<?> route, Timestamp executeAt, @Nullable PartialDeps partialDeps, @Nullable PartialTxn partialTxn, Writes writes, Result result, BiConsumer<Command, Command> consumer)
    {
        SafeCommand safeCommand = safeStore.get(txnId, txnId, route);
        Command before = safeCommand.current();
        Commands.ApplyOutcome outcome = Commands.apply(safeStore, safeCommand, txnId, route, executeAt, partialDeps, partialTxn, writes, result);
        Command after = safeCommand.current();
        if (outcome != Commands.ApplyOutcome.Success) throw illegalState("Command mutation rejected: " + outcome);
        consumer.accept(before, after);
    }
}
