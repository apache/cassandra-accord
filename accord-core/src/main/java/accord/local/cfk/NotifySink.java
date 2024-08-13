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

import accord.api.Key;
import accord.api.ProgressLog.BlockedUntil;
import accord.local.Command;
import accord.local.Commands;
import accord.local.CommonAttributes;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.Keys;
import accord.primitives.RoutingKeys;
import accord.primitives.Seekables;
import accord.primitives.TxnId;

import static accord.local.KeyHistory.COMMANDS;

interface NotifySink
{
    void notWaiting(SafeCommandStore safeStore, TxnId txnId, Key key);

    void notWaiting(SafeCommandStore safeStore, SafeCommand safeCommand, Key key);

    void waitingOn(SafeCommandStore safeStore, TxnInfo txn, Key key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk);

    class DefaultNotifySink implements NotifySink
    {
        static final DefaultNotifySink INSTANCE = new DefaultNotifySink();

        @Override
        public void notWaiting(SafeCommandStore safeStore, TxnId txnId, Key key)
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

        @Override
        public void notWaiting(SafeCommandStore safeStore, SafeCommand safeCommand, Key key)
        {
            Commands.removeWaitingOnKeyAndMaybeExecute(safeStore, safeCommand, key);
        }

        @Override
        public void waitingOn(SafeCommandStore safeStore, TxnInfo notify, Key key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk)
        {
            TxnId txnId = notify.plainTxnId();
            PreLoadContext context = PreLoadContext.contextFor(txnId);

            if (safeStore.canExecuteWith(context)) doNotifyWaitingOn(safeStore, txnId, key, waitingOnStatus, blockedUntil, notifyCfk);
            else safeStore.commandStore().execute(context, safeStore0 -> {
                doNotifyWaitingOn(safeStore0, txnId, key, waitingOnStatus, blockedUntil, notifyCfk);
            }).begin(safeStore.agent());
        }

        // TODO (desired): we could complicate our state machine to replicate PreCommitted here, so we can simply wait for waitingOnStatus.execution
        private void doNotifyWaitingOn(SafeCommandStore safeStore, TxnId txnId, Key key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk)
        {
            SafeCommand safeCommand = safeStore.unsafeGet(txnId);
            safeCommand.initialise();
            Command command = safeCommand.current();
            Seekables<?, ?> keysOrRanges = command.keysOrRanges();
            if (keysOrRanges == null || !keysOrRanges.contains(key))
            {
                // make sure we will notify the CommandsForKey that's waiting
                CommonAttributes.Mutable attrs = command.mutable();
                Keys keys = Keys.of(key);
                if (command.additionalKeysOrRanges() == null) attrs.additionalKeysOrRanges(keys);
                else attrs.additionalKeysOrRanges(keys.with((Keys) command.additionalKeysOrRanges()));
                safeCommand.update(safeStore, command.updateAttributes(attrs));
            }
            if (command.saveStatus().compareTo(waitingOnStatus) >= 0)
            {
                // if we're committed but not invalidated, that means EITHER we have raced with a commit+
                // OR we adopted as a dependency a <...?>
                if (notifyCfk)
                    doNotifyAlreadyReady(safeStore, txnId, key);
            }
            else
            {
                safeStore.progressLog().waiting(blockedUntil, safeStore, safeCommand, null, RoutingKeys.of(key.toUnseekable()));
            }
        }

        private void doNotifyAlreadyReady(SafeCommandStore safeStore, TxnId txnId, Key key)
        {
            SafeCommandsForKey update = safeStore.ifLoadedAndInitialised(key);
            if (update != null)
            {
                update.update(safeStore, safeStore.unsafeGet(txnId).current());
            }
            else
            {
                Keys keys = Keys.of(key);
                //noinspection ConstantConditions,SillyAssignment
                safeStore = safeStore; // prevent use in lambda
                safeStore.commandStore().execute(PreLoadContext.contextFor(txnId, keys, COMMANDS), safeStore0 -> {
                    doNotifyAlreadyReady(safeStore0, txnId, key);
                }).begin(safeStore.agent());
            }
        }

    }
}
