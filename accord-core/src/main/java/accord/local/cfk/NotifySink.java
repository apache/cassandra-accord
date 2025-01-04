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
import accord.local.Command;
import accord.local.Commands;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.local.KeyHistory.SYNC;
import static accord.primitives.Status.Truncated;

interface NotifySink
{
    void notWaiting(SafeCommandStore safeStore, TxnId txnId, RoutingKey key);

    void waitingOn(SafeCommandStore safeStore, TxnInfo txn, RoutingKey key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk);

    class DefaultNotifySink implements NotifySink
    {
        static final DefaultNotifySink INSTANCE = new DefaultNotifySink();

        @Override
        public void notWaiting(SafeCommandStore safeStore, TxnId txnId, RoutingKey key)
        {
            SafeCommand safeCommand = safeStore.ifLoadedAndInitialised(txnId);
            if (safeCommand != null) notWaiting(safeStore, safeCommand, key);
            else
            {
                PreLoadContext context = PreLoadContext.contextFor(txnId);
                safeStore.commandStore().execute(context, safeStore0 -> notWaiting(safeStore0, safeStore0.unsafeGet(txnId), key))
                         .begin(safeStore.agent());
            }
        }

        private void notWaiting(SafeCommandStore safeStore, SafeCommand safeCommand, RoutingKey key)
        {
            Commands.removeWaitingOnKeyAndMaybeExecute(safeStore, safeCommand, key);
        }

        @Override
        public void waitingOn(SafeCommandStore safeStore, TxnInfo notify, RoutingKey key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk)
        {
            TxnId txnId = notify.plainTxnId();
            PreLoadContext context = PreLoadContext.contextFor(txnId);

            if (safeStore.canExecuteWith(context)) doNotifyWaitingOn(safeStore, txnId, key, waitingOnStatus, blockedUntil, notifyCfk);
            else safeStore.commandStore().execute(context, safeStore0 -> {
                doNotifyWaitingOn(safeStore0, txnId, key, waitingOnStatus, blockedUntil, notifyCfk);
            }).begin(safeStore.agent());
        }

        // TODO (desired): we could complicate our state machine to replicate PreCommitted here, so we can simply wait for waitingOnStatus.execution
        private void doNotifyWaitingOn(SafeCommandStore safeStore, TxnId txnId, RoutingKey key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk)
        {
            SafeCommand safeCommand = safeStore.unsafeGet(txnId);
            safeCommand.initialise();
            Command command = safeCommand.current();
            StoreParticipants participants = command.participants();
            if (!participants.hasTouched(key))
            {
                Invariants.checkState(txnId.is(Routable.Domain.Key));
                // make sure we will notify the CommandsForKey that's waiting
                safeCommand.updateParticipants(safeStore, command.participants().supplementHasTouched(RoutingKeys.of(key)));
            }
            if (command.saveStatus().compareTo(waitingOnStatus) >= 0)
            {
                // TODO (required): we expect this invariant to fail periodically today due to how we handle losing ranges
                //   (specifically at minimum in the case where a CommandsForKey is provided a later transaction as a dependency
                //    for an earlier transaction in order to permit CFK to fill in that dependency history).
                //    This should be revisited at the same time as we resolve epoch changes with Recovery, as we expect
                //    to have earlier epochs continue to maintain recovery state until the new epoch is fully ready
                //    as this simplifies recovery and makes it more deterministic (avoiding epoch chasing).
                Invariants.checkState(command.saveStatus().hasBeen(Truncated) || command.participants().touches(key));
                // if we're committed but not invalidated, that means EITHER we have raced with a commit+
                // OR we adopted as a dependency a <...?>
                if (notifyCfk)
                    doNotifyAlreadyReady(safeStore, txnId, key);
            }
            else
            {
                safeStore.progressLog().waiting(blockedUntil, safeStore, safeCommand, null, RoutingKeys.of(key.toUnseekable()), null);
            }
        }

        private void doNotifyAlreadyReady(SafeCommandStore safeStore, TxnId txnId, RoutingKey key)
        {
            SafeCommandsForKey update = safeStore.ifLoadedAndInitialised(key);
            if (update != null)
            {
                update.callback(safeStore, safeStore.unsafeGet(txnId).current());
            }
            else
            {
                RoutingKeys keys = RoutingKeys.of(key);
                //noinspection ConstantConditions,SillyAssignment
                safeStore = safeStore; // prevent use in lambda
                safeStore.commandStore().execute(PreLoadContext.contextFor(txnId, keys, SYNC), safeStore0 -> {
                    doNotifyAlreadyReady(safeStore0, txnId, key);
                }).begin(safeStore.agent());
            }
        }
    }
}
