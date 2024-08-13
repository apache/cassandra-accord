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

package accord.api;

import accord.local.Command;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status.Durability;
import accord.primitives.TxnId;

/**
 * A collection of remote listeners awaiting some set of local responses from command stores.
 * Registrations here are permanent (don't time out) and one shot; if the reply is lost the remote listener will need to be re-registered.
 * Remote listeners may periodically poll to check the listeners are still registered.
 */
public interface RemoteListeners
{
    interface Factory
    {
        RemoteListeners create(Node node);
    }

    /**
     * An in-progress registration that collects command stores it is waiting on.
     * This may be cancelled until done() is invoked, at which point the registration is permanent.
     */
    interface Registration
    {
        // thread safe
        void add(SafeCommandStore safeStore, SafeCommand safeCommand);

        // thread unsafe
        int done();
    }

    /**
     * CallbackId must be a non-negative integer
     */
    Registration register(TxnId txnId, SaveStatus awaitSaveStatus, Durability awaitDurability, Node.Id listener, int callbackId);
    void notify(SafeCommandStore safeStore, SafeCommand safeCommand, Command prev);

    class NoOpRemoteListeners implements RemoteListeners
    {
        static class NoOpRegistration implements Registration
        {
            @Override public void add(SafeCommandStore safeStore, SafeCommand safeCommand) {}
            @Override public int done() { return 0; }
        }

        @Override public Registration register(TxnId txnId, SaveStatus awaitSaveStatus, Durability awaitDurability, Node.Id listener, int callbackId) { return new NoOpRegistration();}
        @Override public void notify(SafeCommandStore safeStore, SafeCommand safeCommand, Command prev) {}
    }
}
