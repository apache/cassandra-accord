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

import accord.impl.DefaultLocalListeners;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;

/**
 * An abstraction for a collection of listeners to transaction updates within a local CommandStore.
 * The default implementation is {@link DefaultLocalListeners}.
 *
 * Unless otherwise specified, methods must be invoked from the thread that owns the relevant current SafeCommandStore.
 */
public interface LocalListeners
{
    interface Factory
    {
        LocalListeners create(CommandStore store);
    }

    /**
     * A listener that may register arbitrary logic to run on command updates.
     * To be used sparingly.
     */
    interface ComplexListener
    {
        /**
         * Process a notification with the current command state.
         * Return false if the listener is now defunct and can be removed.
         */
        boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand);
    }

    /**
     * A {@code ComplexListener} registration that may be cancelled, clearing associated state.
     */
    interface Registered
    {
        TxnId waitingOn();
        ComplexListener waiting();

        /**
         * Cancel the registered listener. May be invoked from any thread.
         */
        void cancel();
    }

    /**
     * Cheap mechanism for one transaction to be notified when another transaction reaches a SaveStatus >= {@code await}
     */
    void register(TxnId waitingOn, SaveStatus await, TxnId waiting);

    /**
     * Less efficient way to listen to a transaction that enables you to supply an arbitrary
     * Java object as a listener instead of a transaction.
     */
    Registered register(TxnId waitingOn, ComplexListener listener);

    /**
     * Notify any listener waiting on {@code safeCommand}'s updated status.
     * Should also forward notifications to the node's RemoteListeners object
     */
    void notify(SafeCommandStore safeStore, SafeCommand safeCommand, Command prev);

    /**
     * Erase all listeners for transactions with a lower {@code TxnId} than {@code clearBefore}.
     * Notify all waiters on listeners with a lower {@code TxnId} than {@code unreadyBefore}.
     *
     */
    void cleanupBefore(TxnId clearBefore, TxnId unreadyBefore);

    /**
     * Erase all listeners; used only for resetting state
     */
    void clear();

    class TxnListener
    {
        public final TxnId waiter;
        public final TxnId waitingOn;
        public final SaveStatus awaitingStatus;

        public TxnListener(TxnId waiter, TxnId waitingOn, SaveStatus awaitingStatus)
        {
            this.waiter = waiter;
            this.waitingOn = waitingOn;
            this.awaitingStatus = awaitingStatus;
        }

        @Override
        public boolean equals(Object that)
        {
            return that instanceof TxnListener && equals((TxnListener) that);
        }

        public boolean equals(TxnListener that)
        {
            return this.waiter.equals(that.waiter) && this.waitingOn.equals(that.waitingOn) && this.awaitingStatus == that.awaitingStatus;
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }
    }

    Iterable<TxnListener> txnListeners();
    Iterable<TxnId> txnListenersWaitingOn();
    Iterable<Registered> complexListeners();
}
