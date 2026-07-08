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

package accord.local.cfk;

import accord.api.ProgressLog.BlockedUntil;
import accord.api.RoutingKey;
import accord.coordinate.CoordinationAdapter.Adapters;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.ExecutePath;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.ExecutionContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import static accord.coordinate.ExecutePath.BACKLOG;
import static accord.coordinate.ExecutePath.RECOVER;
import static accord.primitives.SaveStatus.ReadyToExecute;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.SingleKey;

public class ExecuteTxnBacklog implements NotifySink
{
    final Node node;

    public ExecuteTxnBacklog(Node node)
    {
        this.node = node;
    }

    @Override
    public void notWaiting(SafeCommandStore safeStore, TxnId txnId, RoutingKey key, long uniqueHlc)
    {
        DefaultNotifySink.INSTANCE.notWaiting(safeStore, txnId, key, uniqueHlc);
        if (txnId.is(SingleKey) && (txnId.is(Read) || txnId.is(Write)) && !txnId.node.equals(node.id()))
            execute(safeStore.commandStore(), txnId);
    }

    private void execute(CommandStore commandStore, TxnId txnId)
    {
        commandStore.execute(ExecutionContext.unsequenced(txnId, "Load for ExecuteBacklog"), safeStore -> {
            SafeCommand safeCommand = safeStore.unsafeGet(txnId);
            Command command = safeCommand.current();
            if (command.saveStatus() != ReadyToExecute || command.participants().stillExecutes().isEmpty())
                return;

            Ballot ballot = command.promised();
            FullRoute<?> route = (FullRoute<?>) command.route();
            Txn txn = command.partialTxn().reconstitute(route);
            Timestamp executeAt = command.executeAt();
            Deps deps = command.partialDeps().reconstitute(route);
            ExecutePath path = ballot.equals(Ballot.ZERO) ? BACKLOG : RECOVER;
            Node.Id notify = (ballot.equals(Ballot.ZERO) ? txnId : ballot).node;
            if (notify.equals(node.id()))
                return;

            node.withEpochAtLeast(executeAt.epoch(), null, node.agent(), () -> {
                node.agent().coordinatorEvents().onRecoveryStarted(txnId, ballot);
                Adapters.standard().execute(node, node.someExclusiveExecutor(), null, route, command.acceptedOrCommitted(), path, CoordinationFlags.none(), txnId, txn, executeAt, deps, deps, (result, fail) -> {
                    if (fail == null) node.reportLocalExecution(txnId, route, ballot, null, null, result);
                    else node.agent().onException(fail);
                });
            });
        });
    }

    @Override
    public void waitingOn(SafeCommandStore safeStore, TxnInfo txn, RoutingKey key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk)
    {
        DefaultNotifySink.INSTANCE.waitingOn(safeStore, txn, key, waitingOnStatus, blockedUntil, notifyCfk);
    }
}
