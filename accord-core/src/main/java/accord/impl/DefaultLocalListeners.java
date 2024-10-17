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

import java.util.Arrays;
import java.util.EnumMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import accord.api.LocalListeners;
import accord.api.RemoteListeners;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.AsymmetricComparator;
import accord.utils.Invariants;
import accord.utils.btree.BTree;
import accord.utils.btree.BTreeRemoval;

// TODO (expected): evict to disk
public class DefaultLocalListeners implements LocalListeners
{
    public static class Factory implements LocalListeners.Factory
    {
        final RemoteListeners remoteListeners;
        final NotifySink notifySink;

        public Factory(Node node)
        {
            this(node, DefaultNotifySink.INSTANCE);
        }

        public Factory(Node node, NotifySink notifySink)
        {
            this.remoteListeners = node.remoteListeners();
            this.notifySink = notifySink;
        }

        @Override
        public LocalListeners create(CommandStore store)
        {
            return new DefaultLocalListeners(remoteListeners, notifySink);
        }
    }

    public interface NotifySink
    {
        void notify(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId listener);
        boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand, ComplexListener listener);

        class NoOpNotifySink implements NotifySink
        {
            @Override public void notify(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId listener) {}
            @Override public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand, LocalListeners.ComplexListener listener) { return false; }
        }
    }

    public static class DefaultNotifySink implements NotifySink
    {
        public static final DefaultNotifySink INSTANCE = new DefaultNotifySink();

        @Override
        public void notify(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId listenerId)
        {
            SafeCommand listener = safeStore.ifLoadedAndInitialisedAndNotErased(listenerId);
            if (listener != null) Commands.listenerUpdate(safeStore, listener, safeCommand);
            else
            {
                //noinspection SillyAssignment,ConstantConditions
                safeStore = safeStore; // prevent use in lambda
                TxnId updatedId = safeCommand.txnId();
                PreLoadContext context = PreLoadContext.contextFor(listenerId, updatedId);
                safeStore.commandStore()
                         .execute(context, safeStore0 -> notify(safeStore0, listenerId, updatedId))
                         .begin(safeStore.agent());
            }
        }

        private static void notify(SafeCommandStore safeStore, TxnId listenerId, TxnId updatedId)
        {
            Commands.listenerUpdate(safeStore, safeStore.unsafeGet(listenerId), safeStore.unsafeGet(updatedId));
        }

        @Override
        public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand, ComplexListener listener)
        {
            return listener.notify(safeStore, safeCommand);
        }
    }

    /*
     * A list that allows duplicates and sorts and removes duplicates on notify and when the list would have to resize
     */
    static class TxnListeners extends TxnId
    {
        final SaveStatus await;
        TxnId[] listeners = NO_TXNIDS;
        int count;

        TxnListeners(TxnId txnId, SaveStatus await)
        {
            super(txnId);
            this.await = await;
        }

        public int compareListeners(TxnListeners that)
        {
            int c = this.compareTo(that);
            if (c == 0) c = this.await.compareTo(that.await);
            return c;
        }

        public static int compareBefore(TxnId txnId, TxnListeners that)
        {
            int c = txnId.compareTo(that);
            if (c == 0) c = -1;
            return c;
        }

        public static int compare(TxnId txnId, SaveStatus await, TxnListeners that, int ifEqual)
        {
            int c = txnId.compareTo(that);
            if (c == 0) c = await.compareTo(that.await);
            if (c == 0) c = ifEqual;
            return c;
        }

        void notify(NotifySink notifySink, SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            trim();
            for (int i = 0 ; i < count ; ++i)
            {
                TxnId listenerId = listeners[i];
                notifySink.notify(safeStore, safeCommand, listenerId);
            }
        }

        /*
         * Removes duplicates
         */
        private int trim()
        {
            Arrays.sort(listeners, 0, count);
            int removedCount = 0;
            for (int i = 1 ; i < count ; ++i)
            {
                if (listeners[i - 1].compareTo(listeners[i]) == 0) ++removedCount;
                else if (removedCount > 0) listeners[i - removedCount] = listeners[i];
            }

            if (removedCount != 0)
            {
                int prevCount = count;
                count -= removedCount;
                Arrays.fill(listeners, count, prevCount, null);
            }
            return removedCount;
        }

        void add(TxnId listener)
        {
            if (count == listeners.length)
            {
                if (count == 0)
                {
                    listeners = new TxnId[4];
                }
                else
                {
                    int removedCount = trim();
                    if (removedCount < listeners.length / 2)
                    {
                        TxnId[] newListeners = new TxnId[count * 2];
                        System.arraycopy(listeners, 0, newListeners, 0, count);
                        listeners = newListeners;
                    }
                }
            }

            listeners[count++] = listener;
        }
    }

    class RegisteredComplexListener implements Registered, BiFunction<TxnId, RegisteredComplexListeners, RegisteredComplexListeners>
    {
        final TxnId txnId;
        final ComplexListener listener;
        int index;

        RegisteredComplexListener(TxnId txnId, ComplexListener listener)
        {
            this.listener = listener;
            this.txnId = txnId;
        }

        @Override
        public void cancel()
        {
            if (index < 0)
                return;

            complexListeners.compute(txnId, this);
        }

        @Override
        public RegisteredComplexListeners apply(TxnId txnId, RegisteredComplexListeners listeners)
        {
            if (listeners == null)
                return null;
            return listeners.remove(this);
        }
    }

    /**
     * a very simple list that we can leave null entries in to make removals and reentry easier.
     *  - removal is easier because we can store an index to the array entry to null it out instantly
     *  - reentry is easier because the notifying thread doesn't need to worry about stuff moving around
     *
     * Methods assume mutual exclusion is guaranteed by the caller, but DOES permit reentry.
     */
    static class RegisteredComplexListeners
    {
        static final RegisteredComplexListener[] NO_LISTENERS = new RegisteredComplexListener[0];
        RegisteredComplexListener[] listeners = NO_LISTENERS;
        int count, length;
        boolean notifying;

        /**
         * Append to the end of the list; if we aren't reentering from notify then if the next position
         * in the list is unavailable and the list is half empty we first compact the list to remove null entries
         */
        RegisteredComplexListeners remove(RegisteredComplexListener remove)
        {
            int index = remove.index;
            if (index < 0)
                return this; // already removed

            Invariants.checkState(listeners[index] == remove);
            listeners[index] = null;
            remove.index = -1;
            // we don't decrement length even if count==length so as to simplify reentry
            --count;
            if (Invariants.isParanoid() && !notifying) checkIntegrity();
            return count > 0 || notifying ? this : null;
        }

        /**
         * Append to the end of the list; if we aren't reentering from notify then if the next position
         * in the list is unavailable and the list is half empty we first compact the list to remove null entries;
         * otherwise we resize the array leaving the entries in their original position
         */
        void add(RegisteredComplexListener add)
        {
            if (listeners.length == length)
            {
                RegisteredComplexListener[] oldListeners = listeners;
                if (length >= count / 2 || notifying)
                    listeners = new RegisteredComplexListener[Math.max(2, length * 2)];

                if (count == length || notifying)
                {
                    // copy to same positions
                    System.arraycopy(oldListeners, 0, listeners, 0, length);
                }
                else
                {
                    // copy and compact
                    int c = 0;
                    for (int i = 0 ; i < length ; ++i)
                    {
                        if (oldListeners[i] == null) continue;
                        Invariants.checkState(oldListeners[i].index == i);
                        listeners[c] = oldListeners[i];
                        listeners[c].index = c;
                        c++;
                    }
                    if (listeners == oldListeners)
                        Arrays.fill(listeners, c, length, null);
                    Invariants.checkState(c == count);
                    length = count;
                }
            }
            listeners[length] = add;
            add.index = length;
            length++;
            count++;
            if (Invariants.isParanoid() && !notifying) checkIntegrity();
        }

        /**
         * Notify any listeners, permitting those listeners to reenter and register/cancel listeners against this TxnId.
         * We do this by ensuring the position of listeners doesn't change while notifying, and visiting only those
         * listeners that were present when we started. We compact the listener collection as we go, though given
         * reentry there is no guarantee the list at exit is compacted.
         */
        RegisteredComplexListeners notify(SafeCommandStore safeStore, SafeCommand safeCommand, NotifySink notifySink)
        {
            int count = 0;
            int length = this.length;

            notifying = true;
            for (int i = 0 ; i < length ; ++i)
            {
                RegisteredComplexListener next = listeners[i];
                if (next == null) continue;
                Invariants.checkState(next.index == i);
                if (!notifySink.notify(safeStore, safeCommand, listeners[i].listener))
                {
                    if (next.index >= 0)
                        --this.count;
                    next.index = -1;
                }
                else if (next.index >= 0) // can be cancelled by notify, without notify return false
                {
                    Invariants.checkArgument(next.index == i);
                    if (i != count)
                    {
                        listeners[count] = next;
                        next.index = count;
                    }
                    ++count;
                }
                else Invariants.checkState(listeners[i] == null);
            }
            notifying = false;

            if (length != this.length)
            {
                // we have had some concurrent insertions (concurrent removals do not alter length)
                // we also have some empty slots, so compact the new entries
                for (int i = length ; i < this.length ; ++i)
                {
                    RegisteredComplexListener next = listeners[i];
                    if (next == null)
                        continue;

                    Invariants.checkState(next.index == i);
                    listeners[count] = next;
                    next.index = count;
                    count++;
                }
                Invariants.checkState(this.count <= count); // we could have already removed some items from the compacted section
                length = this.length;
            }

            Arrays.fill(listeners, count, length, null);
            this.length = count;

            if (Invariants.isParanoid()) checkIntegrity();
            return count == 0 ? null : this;
        }

        private void checkIntegrity()
        {
            int c = 0;
            for (int i = 0 ; i < length ; ++i)
                c += listeners[i] != null ? 1 : 0;
            Invariants.checkState(c == count);
            for (int i = length ; i < listeners.length ; ++i)
                Invariants.checkState(listeners[i] == null);
        }
    }

    private static final EnumMap<SaveStatus, AsymmetricComparator<TxnId, TxnListeners>> compareExact, compareAfter;
    static
    {
        compareAfter = new EnumMap<>(SaveStatus.class);
        compareExact = new EnumMap<>(SaveStatus.class);
        for (SaveStatus saveStatus : SaveStatus.values())
        {
            compareAfter.put(saveStatus, (id, listeners) -> TxnListeners.compare(id, saveStatus, listeners, 1));
            compareExact.put(saveStatus, (id, listeners) -> TxnListeners.compare(id, saveStatus, listeners, 0));
        }
    }

    private final RemoteListeners remoteListeners;
    private final NotifySink notifySink;

    private final ConcurrentHashMap<TxnId, RegisteredComplexListeners> complexListeners = new ConcurrentHashMap<>();
    private Object[] txnListeners = BTree.empty();

    public DefaultLocalListeners(RemoteListeners remoteListeners, NotifySink notifySink)
    {
        this.remoteListeners = remoteListeners;
        this.notifySink = notifySink;
    }

    @Override
    public void register(TxnId txnId, SaveStatus await, TxnId listener)
    {
        TxnListeners entry = BTree.find(txnListeners, compareExact.get(await), txnId);
        if (entry == null)
            txnListeners = BTree.update(txnListeners, BTree.singleton(entry = new TxnListeners(txnId, await)), TxnListeners::compareListeners);
        entry.add(listener);
    }

    @Override
    public Registered register(TxnId txnId, ComplexListener listener)
    {
        RegisteredComplexListener entry = new RegisteredComplexListener(txnId, listener);
        complexListeners.compute(txnId, (id, cur) -> {
            if (cur == null)
                cur = new RegisteredComplexListeners();
            cur.add(entry);
            return cur;
        });
        return entry;
    }

    @Override
    public void notify(SafeCommandStore safeStore, SafeCommand safeCommand, Command prev)
    {
        notifyTxnListeners(safeStore, safeCommand);
        notifyComplexListeners(safeStore, safeCommand);
        remoteListeners.notify(safeStore, safeCommand, prev);
    }

    private void notifyTxnListeners(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Object[] txnListeners = this.txnListeners;
        TxnId txnId = safeCommand.txnId();
        SaveStatus saveStatus = safeCommand.current().saveStatus();
        // TODO (desired): faster iteration, currently this is O(n.lg(n))
        int start = -1 - BTree.findIndex(txnListeners, TxnListeners::compareBefore, txnId);
        int end = -1 - BTree.findIndex(txnListeners, compareAfter.get(saveStatus), txnId);
        while (start < end)
        {
            TxnListeners notify = BTree.findByIndex(txnListeners, start);
            Invariants.checkState(txnId.equals(notify));
            notify.notify(notifySink, safeStore, safeCommand);
            if (this.txnListeners != txnListeners)
            {
                // listener registrations were changed by this listener's notify invocation, so reset our cursor
                txnListeners = this.txnListeners;
                start = BTree.findIndex(txnListeners, TxnListeners::compareListeners, notify);
                end = -1 - BTree.findIndex(txnListeners, compareAfter.get(saveStatus), txnId);
                // we only permit callers to insert into this collection, so we do not have to consider
                // the case where the listener we are processing has been deleted under us
            }

            this.txnListeners = txnListeners = BTreeRemoval.remove(txnListeners, TxnListeners::compareListeners, notify);
            --end;
        }
        this.txnListeners = txnListeners;
    }

    private void notifyComplexListeners(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        // TODO (expected): potential for lock inversion on notify calls; consider buffering notifies as we do elsewhere
        complexListeners.compute(safeCommand.txnId(), (id, cur) -> {
            if (cur == null)
                return null;
            return cur.notify(safeStore, safeCommand, notifySink);
        });
    }

}
