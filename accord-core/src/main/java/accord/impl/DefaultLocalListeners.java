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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import javax.annotation.Nullable;

import accord.api.LocalListeners;
import accord.api.RemoteListeners;
import accord.api.VisibleForImplementation;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.ExecutionContext.ExecutionSequence;
import accord.local.Node;
import accord.local.ExecutionContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.AsymmetricComparator;
import accord.utils.Invariants;
import accord.utils.btree.BTree;
import accord.utils.btree.BTreeRemoval;
import accord.utils.btree.BulkIterator;
import accord.utils.btree.UpdateFunction;

import static accord.utils.ArrayBuffers.cachedAny;
import static accord.utils.ArrayBuffers.cachedTxnIds;

// TODO (desired): evict to disk
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
        public LocalListeners create(CommandStore commandStore)
        {
            return new DefaultLocalListeners(commandStore, remoteListeners, notifySink);
        }
    }

    public interface NotifySink
    {
        void notify(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId listener);
        boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand, ComplexListener listener);

        @VisibleForImplementation
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
            SafeCommand listener = safeStore.ifLoadedAndInitialised(listenerId);
            if (listener != null && safeStore.tryRecurse())
            {
                try { Commands.listenerUpdate(safeStore, listener, safeCommand); }
                catch (Throwable t) { safeStore.agent().onException(t); }
                finally { safeStore.unrecurse(); }
            }
            else
            {
                //noinspection SillyAssignment,ConstantConditions
                safeStore = safeStore; // prevent use in lambda
                TxnId updatedId = safeCommand.txnId();
                ExecutionContext context = new NotifyContext(listenerId, updatedId);
                safeStore.commandStore().execute(context, safeStore0 -> { notify(safeStore0, listenerId, updatedId); }, safeStore.agent());
            }
        }

        private static void notify(SafeCommandStore safeStore, TxnId listenerId, TxnId updatedId)
        {
            Commands.listenerUpdate(safeStore, safeStore.unsafeGet(listenerId), safeStore.unsafeGet(updatedId));
        }

        @Override
        public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand, ComplexListener listener)
        {
            try { return listener.notify(safeStore, safeCommand); }
            catch (Throwable t) { safeStore.agent().onException(t); return false; }
        }
    }

        static class NotifyContext implements ExecutionContext
        {
            final TxnId primaryTxnId;
            final TxnId additionalTxnId;

            NotifyContext(TxnId primaryTxnId, TxnId additionalTxnId)
            {
                this.primaryTxnId = primaryTxnId;
                this.additionalTxnId = additionalTxnId;
            }

            @Override public @Nullable TxnId primaryTxnId() { return primaryTxnId; }
            @Override public @Nullable TxnId additionalTxnId() { return additionalTxnId; }
            @Override public ExecutionSequence executionSequence() { return ExecutionSequence.UNSEQUENCED; }
            @Override public String reason() { return "Notify"; }
            @Override public String toString() { return describe(); }
        };

    /*
     * A list that allows duplicates and sorts and removes duplicates on notify and when the list would have to resize
     * TODO (expected): save time and space by:
     *      - encoding SaveStatus as byte
     *      - encoding listeners as any of: single TxnId, array of TxnId (for small size), btree for a large collection
     */
    static class TxnListeners extends TxnId implements ExecutionContext
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

        void notify(DefaultLocalListeners owner, SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            trim();
            for (int i = 0 ; i < count ; ++i)
            {
                TxnId listenerId = listeners[i];
                owner.notifySink.notify(safeStore, safeCommand, listenerId);
            }
        }

        /*
         * Removes duplicates
         */
        private int trim()
        {
            if (count == 0)
                return 0;

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

        @Nullable
        @Override
        public TxnId primaryTxnId()
        {
            return this;
        }

        @Override
        public String reason()
        {
            return "Notify Listeners";
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
        public TxnId waitingOn()
        {
            return txnId;
        }

        @Override
        public ComplexListener waiting()
        {
            return listener;
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

        /**
         * Append to the end of the list; if we aren't reentering from notify then if the next position
         * in the list is unavailable and the list is half empty we first compact the list to remove null entries
         */
        RegisteredComplexListeners remove(RegisteredComplexListener remove)
        {
            int index = remove.index;
            if (index < 0)
                return this; // already removed

            Invariants.require(listeners[index] == remove);
            listeners[index] = null;
            remove.index = -1;
            if (--count == 0)
                return null;

            if (Invariants.isParanoid()) checkIntegrity();
            return this;
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
                if (length >= count / 2)
                    listeners = new RegisteredComplexListener[Math.max(2, length * 2)];

                if (count == length)
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
                        Invariants.require(oldListeners[i].index == i);
                        listeners[c] = oldListeners[i];
                        listeners[c].index = c;
                        c++;
                    }
                    if (listeners == oldListeners)
                        Arrays.fill(listeners, c, length, null);
                    Invariants.require(c == count);
                    length = count;
                }
            }
            listeners[length] = add;
            add.index = length;
            length++;
            count++;
            if (Invariants.isParanoid()) checkIntegrity();
        }

        /**
         * Notify any listeners, permitting those listeners to reenter and register/cancel listeners against this TxnId.
         * We do this by ensuring the position of listeners doesn't change while notifying, and visiting only those
         * listeners that were present when we started. We compact the listener collection as we go, though given
         * reentry there is no guarantee the list at exit is compacted.
         */
        void collect(List<RegisteredComplexListener> notify)
        {
            for (int i = 0 ; i < length ; ++i)
            {
                RegisteredComplexListener next = listeners[i];
                if (next == null) continue;
                Invariants.require(next.index == i);
                notify.add(listeners[i]);
            }
        }

        private void checkIntegrity()
        {
            int c = 0;
            for (int i = 0 ; i < length ; ++i)
                c += listeners[i] != null ? 1 : 0;
            Invariants.require(c == count);
            for (int i = length ; i < listeners.length ; ++i)
                Invariants.require(listeners[i] == null);
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

    private final CommandStore commandStore;
    private final RemoteListeners remoteListeners;
    private final NotifySink notifySink;

    private final ConcurrentHashMap<TxnId, RegisteredComplexListeners> complexListeners = new ConcurrentHashMap<>();
    private Object[] txnListeners = BTree.empty();

    public DefaultLocalListeners(CommandStore commandStore, RemoteListeners remoteListeners, NotifySink notifySink)
    {
        this.commandStore = commandStore;
        this.remoteListeners = remoteListeners;
        this.notifySink = notifySink;
    }

    @Override
    public void register(TxnId waitingOn, SaveStatus await, TxnId listener)
    {
        TxnListeners entry = BTree.find(txnListeners, compareExact.get(await), waitingOn);
        if (entry == null)
            txnListeners = BTree.update(txnListeners, BTree.singleton(entry = new TxnListeners(waitingOn, await)), TxnListeners::compareListeners);
        entry.add(listener);
    }

    @Override
    public Registered register(TxnId waitingOn, ComplexListener listener)
    {
        RegisteredComplexListener entry = new RegisteredComplexListener(waitingOn, listener);
        complexListeners.compute(waitingOn, (id, cur) -> {
            if (cur == null)
                cur = new RegisteredComplexListeners();
            cur.add(entry);
            return cur;
        });
        return entry;
    }

    @Override
    public void notify(SafeCommandStore safeStore, SafeCommand safeCommand, @Nullable Command prev)
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
            Invariants.require(txnId.equals(notify));
            notify.notify(this, safeStore, safeCommand);
            if (this.txnListeners != txnListeners)
            {
                // listener registrations were changed by this listener's notify invocation, so reset our cursor
                txnListeners = this.txnListeners;
                end = -1 - BTree.findIndex(txnListeners, compareAfter.get(saveStatus), txnId);
                start = BTree.findIndex(txnListeners, TxnListeners::compareListeners, notify);
                if (start < 0)
                {
                    start = -1 - start;
                    continue;
                }
                // we only permit callers to insert into this collection, so we do not have to consider
                // the case where the listener we are processing has been deleted under us
            }

            this.txnListeners = txnListeners = BTreeRemoval.remove(txnListeners, TxnListeners::compareListeners, notify);
            --end;
        }
        this.txnListeners = txnListeners;
    }

    static class NotifyComplex extends BufferList<RegisteredComplexListener> implements BiFunction<TxnId, RegisteredComplexListeners, RegisteredComplexListeners>
    {
        boolean remove;
        @Override
        public RegisteredComplexListeners apply(TxnId txnId, RegisteredComplexListeners cur)
        {
            if (cur == null)
                return null;

            if (remove)
            {
                for (RegisteredComplexListener listener : this)
                {
                    if (listener != null && null == cur.remove(listener))
                        return null; // can only return early if remaining listeners already removed
                }
            }
            else
            {
                cur.collect(this);
            }

            return cur;
        }
    }

    private void notifyComplexListeners(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        try (NotifyComplex notify = new NotifyComplex())
        {
            complexListeners.compute(safeCommand.txnId(), notify);

            int size = notify.size(), count = size;
            for (int i = 0 ; i < size ; ++i)
            {
                RegisteredComplexListener registered = notify.get(i);
                if (registered.index < 0) continue;
                boolean noChange = notifySink.notify(safeStore, safeCommand, registered.listener)
                                   || registered.index < 0; // check we haven't removed ourselves during notify, to avoid wasted work

                if (noChange)
                {
                    notify.set(i, null);
                    --count;
                }
            }

            if (count > 0)
            {
                notify.remove = true;
                complexListeners.compute(safeCommand.txnId(), notify);
            }
        }
    }

    @Override
    public void clearBefore(TxnId clearBefore)
    {
        while (!BTree.isEmpty(txnListeners))
        {
            TxnListeners entry = BTree.findByIndex(txnListeners, 0);
            if (entry.compareTo(clearBefore) >= 0)
                return;

            commandStore.execute(entry, safeStore -> {
                SafeCommand safeCommand = safeStore.unsafeGet(entry);
                Command command = safeCommand.current();
                SaveStatus saveStatus = command.saveStatus();
                Invariants.require(saveStatus.compareTo(entry.await) >= 0 || command.participants().stillOwns().isEmpty());
                entry.notify(this, safeStore, safeCommand);
            }, commandStore.agent());
            txnListeners = BTreeRemoval.remove(txnListeners, TxnListeners::compareListeners, entry);
        }
    }

    public void clear()
    {
        txnListeners = BTree.empty();
        complexListeners.forEach((key, value) -> {
            // the listener registration needs to be invalidated so that a caller does not try to cancel it
            RegisteredComplexListeners listeners = complexListeners.remove(key);
            if (listeners != null)
            {
                if (Invariants.isParanoid()) listeners.checkIntegrity();
                // On removal listeners contains nulls, so skip
                for (int i = 0 ; i < listeners.length ; i++)
                {
                    RegisteredComplexListener l = listeners.listeners[i];
                    if (l != null)
                        l.index = -1;
                }
            }
        });
    }

    @Override
    public Iterable<TxnId> txnListenersWaitingOn()
    {
        return () -> {
            return new Iterator<>()
            {
                Object[] snapshot = txnListeners;
                Iterator<TxnListeners> iter = BTree.slice(snapshot, TxnListeners::compareListeners, BTree.Dir.ASC);
                TxnListeners prev, next;

                @Override
                public boolean hasNext()
                {
                    Invariants.require(commandStore.inStore());

                    if (snapshot != txnListeners)
                    {
                        snapshot = txnListeners;
                        if (prev == null) iter = BTree.slice(snapshot, TxnListeners::compareListeners, BTree.Dir.ASC);
                        else iter = BTree.slice(snapshot, TxnListeners::compareListeners, prev, false, null, false, BTree.Dir.ASC);
                    }

                    if (!iter.hasNext())
                        return false;

                    next = iter.next();
                    return true;
                }

                @Override
                public TxnId next()
                {
                    prev = next;
                    next = null;
                    return new TxnId(prev);
                }
            };
        };
    }

    @Override
    public Iterable<TxnListener> txnListeners()
    {
        return () -> {
            return new Iterator<>()
            {
                Object[] snapshot;
                Iterator<TxnListeners> iter;
                TxnListeners cur;
                TxnId[] buffer = TxnId.NO_TXNIDS;
                int bufferIndex, bufferCount, maxBufferCount;

                @Override
                public boolean hasNext()
                {
                    Invariants.require(commandStore.inStore());

                    if (bufferIndex < bufferCount)
                        return true;

                    if (snapshot != txnListeners)
                    {
                        snapshot = txnListeners;
                        if (cur == null) iter = BTree.slice(snapshot, TxnListeners::compareListeners, BTree.Dir.ASC);
                        else iter = BTree.slice(snapshot, TxnListeners::compareListeners, cur, false, null, false, BTree.Dir.ASC);
                    }

                    while (true)
                    {
                        if (!iter.hasNext())
                        {
                            cachedTxnIds().forceDiscard(buffer, maxBufferCount);
                            buffer = null;
                            return false;
                        }

                        cur = iter.next();
                        bufferIndex = 0;
                        cur.trim();
                        bufferCount = cur.count;
                        if (bufferCount == 0)
                            continue;

                        if (bufferCount > maxBufferCount)
                        {
                            if (bufferCount > buffer.length)
                            {
                                cachedTxnIds().forceDiscard(buffer, maxBufferCount);
                                buffer = cachedTxnIds().get(Math.max(buffer.length * 2, bufferCount));
                            }
                            maxBufferCount = bufferCount;
                        }

                        System.arraycopy(cur.listeners, 0, buffer, 0, bufferCount);
                        return true;
                    }
                }

                @Override
                public TxnListener next()
                {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    return new TxnListener(buffer[bufferIndex++], new TxnId(cur), cur.await);
                }
            };
        };
    }

    private static final Object[] NO_OBJECTS = new Object[0];
    @Override
    public Iterable<Registered> complexListeners()
    {
        return () -> {
            return new Iterator<>()
            {
                final Enumeration<TxnId> iter = complexListeners.keys();
                Object[] buffer = NO_OBJECTS;
                int bufferIndex, bufferCount, maxBufferCount;

                @Override
                public boolean hasNext()
                {
                    if (bufferIndex < bufferCount)
                        return true;

                    while (true)
                    {
                        if (!iter.hasMoreElements())
                        {
                            cachedAny().forceDiscard(buffer, Math.max(bufferCount, maxBufferCount));
                            buffer = null;
                            return false;
                        }

                        TxnId txnId = iter.nextElement();
                        complexListeners.compute(txnId, (ignore, cur) -> {
                            if (cur == null)
                                return cur;

                            bufferIndex = 0;
                            bufferCount = cur.count;
                            if (bufferCount == 0)
                                return cur;

                            if (bufferCount > maxBufferCount)
                            {
                                if (bufferCount > buffer.length)
                                    buffer = cachedAny().resize(buffer, 0, Math.max(buffer.length * 2, bufferCount));
                                maxBufferCount = bufferCount;
                            }

                            System.arraycopy(cur.listeners, 0, buffer, 0, bufferCount);
                            return cur;
                        });

                        if (bufferCount > 0)
                            return true;
                    }
                }

                @Override
                public Registered next()
                {
                    if (!hasNext())
                        throw new NoSuchElementException();
                    return (Registered) buffer[bufferIndex++];
                }
            };
        };
    }

    public void restore(List<TxnListener> listeners)
    {
        if (listeners.isEmpty())
            return;

        if (!BTree.isEmpty(txnListeners))
            throw new IllegalStateException("Restore only supported if uninitialised");

        listeners.sort((a, b) -> {
            int c = a.waitingOn.compareTo(b.waitingOn);
            if (c == 0) c = a.awaitingStatus.compareTo(b.awaitingStatus);
            if (c == 0) c = a.waiter.compareTo(b.waiter);
            return c;
        });

        List<TxnListeners> build = new ArrayList<>();
        int li = 0;
        while (li < listeners.size())
        {
            TxnListener l = listeners.get(li);
            TxnListeners ls = new TxnListeners(l.waitingOn, l.awaitingStatus);
            build.add(ls);
            ls.add(l.waiter);
            while (++li < listeners.size() && (l = listeners.get(li)).waitingOn.equals(ls) && l.awaitingStatus == ls.await)
                ls.add(l.waiter);
        }
        txnListeners = BTree.build(BulkIterator.of(build.iterator()), build.size(), UpdateFunction.noOp());
    }

    public List<TxnListener> snapshot()
    {
        List<TxnListener> snapshot = new ArrayList<>(BTree.size(txnListeners));
        for (TxnListener listener : txnListeners())
            snapshot.add(listener);
        return snapshot;
    }
}
