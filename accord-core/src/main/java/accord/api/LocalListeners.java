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
import accord.local.CommandStore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.primitives.TxnId;

public interface LocalListeners
{
    interface Factory
    {
        LocalListeners create(CommandStore store);
    }

    // to be used sparingly - much less efficient
    interface ComplexListener
    {
        // return true if still listening, false if can be removed
        boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand);
    }

    interface Registered
    {
        // may be invoked from any thread
        void cancel();
    }

    /**
     * Cheap way to notify a transaction when a different transaction reaches a SaveStatus >= a target SaveStatus
     */
    void register(TxnId txnId, SaveStatus await, TxnId waiting);

    /**
     * Less efficient way to listen to a transaction that enables you to supply an arbitrary
     * Java object as a listener instead of a transaction.
     */
    Registered register(TxnId txnId, ComplexListener listener);

    /**
     * Should forward notifications to the node's RemoteListeners object
     */
    void notify(SafeCommandStore safeStore, SafeCommand safeCommand, Command prev);
}
