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
import accord.api.RoutingKey;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;

public class CheckedCommands
{
    public static void preaccept(SafeCommandStore safeStore, TxnId txnId, PartialTxn partialTxn, FullRoute<?> route, @Nullable RoutingKey progressKey)
    {
        Commands.AcceptOutcome result = Commands.preaccept(safeStore, txnId, txnId.epoch(), partialTxn, route, progressKey);
        if (result != Commands.AcceptOutcome.Success) throw new IllegalStateException("Command mutation rejected: " + result);
    }

    public static void accept(SafeCommandStore safeStore, TxnId txnId, Ballot ballot, PartialRoute<?> route, Seekables<?, ?> keys, @Nullable RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps)
    {
        Commands.AcceptOutcome result = Commands.accept(safeStore, txnId, ballot, route, keys, progressKey, executeAt, partialDeps);
        if (result != Commands.AcceptOutcome.Success) throw new IllegalStateException("Command mutation rejected: " + result);
    }

    public static void commit(SafeCommandStore safeStore, TxnId txnId, Route<?> route, @Nullable RoutingKey progressKey, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        Commands.CommitOutcome result = Commands.commit(safeStore, txnId, route, progressKey, partialTxn, executeAt, partialDeps);
        if (result != Commands.CommitOutcome.Success) throw new IllegalStateException("Command mutation rejected: " + result);
    }

    public static void apply(SafeCommandStore safeStore, TxnId txnId, Route<?> route, @Nullable RoutingKey progressKey, Timestamp executeAt, @Nullable PartialDeps partialDeps, Writes writes, Result result)
    {
        Commands.ApplyOutcome outcome = Commands.apply(safeStore, txnId, route, progressKey, executeAt, partialDeps, writes, result);
        if (outcome != Commands.ApplyOutcome.Success) throw new IllegalStateException("Command mutation rejected: " + outcome);
    }
}
