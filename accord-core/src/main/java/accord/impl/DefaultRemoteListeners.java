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
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;

import accord.api.RemoteListeners;
import accord.local.Command;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.messages.Await.AsyncAwaitComplete;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.utils.Invariants;

// TODO (expected): evict to disk
public class DefaultRemoteListeners implements RemoteListeners
{
    public interface NotifySink
    {
        void notify(TxnId txnId, SaveStatus saveStatus, Route<?> participants, long[] listeners, int listenerCount);
    }

    public static class DefaultNotifySink implements NotifySink
    {
        final Node node;
        public DefaultNotifySink(Node node)
        {
            this.node = node;
        }

        @Override
        public void notify(TxnId txnId, SaveStatus saveStatus, Route<?> route, long[] listeners, int listenerCount)
        {
            for (int i = 0 ; i < listenerCount ; ++i)
            {
                long listener = listeners[i];
                // we could in theory reply to the same node multiple times here, but should not be common enough to optimise
                Node.Id replyTo = new Node.Id(listenerNodeId(listener));
                int callbackId = listenerCallbackId(listener);
                node.send(replyTo, new AsyncAwaitComplete(txnId, route, saveStatus, callbackId));
            }
        }
    }

    static class StatusListeners
    {
        final int await;

        /**
         * waitingOn is a sorted list of commandStore ids that have not reached our wait condition.
         * To save time on deletions, we only set the top bit to mark deleted until we have halved the number of entries,
         * at which point we remove the tombstones.
         * waitingOnSize tracks the top entry, waitingOnCount the number of live entries
         */
        int[] waitingOn;
        int waitingOnSize, waitingOnCount;

        /**
         * This is a packed list of listeners, encoding the nodeId to respond to and the callbackId
         */
        long[] listeners;
        int listenerCount;

        StatusListeners(SaveStatus awaitSaveStatus, Durability awaitDurability, int[] waitingOn, int waitingOnCount, int listenerNodeId, int listenerCallbackId)
        {
            this.await = encodeAwait(awaitSaveStatus, awaitDurability);
            this.waitingOn = waitingOn;
            this.waitingOnCount = this.waitingOnSize = waitingOnCount;
            this.listeners = new long[] { encodeListener(listenerNodeId, listenerCallbackId) };
            this.listenerCount = 1;
        }

        StatusListeners merge(StatusListeners that)
        {
            mergeInts(this.waitingOn, this.waitingOnSize, that.waitingOn, that.waitingOnSize, StatusListeners::updateWaitingOn);
            mergeLongs(this.listeners, this.listenerCount, that.listeners, that.listenerCount, StatusListeners::updateListeners);
            return this;
        }

        interface IntMergeConsumer
        {
            void accept(StatusListeners listeners, int[] out, int count);
        }

        interface LongMergeConsumer
        {
            void accept(StatusListeners listeners, long[] out, int count);
        }

        private void updateWaitingOn(int[] newWaitingOn, int newWaitingOnCount)
        {
            this.waitingOn = newWaitingOn;
            this.waitingOnCount = this.waitingOnSize = newWaitingOnCount;
            Invariants.checkArgumentSorted(newWaitingOn, 0, newWaitingOnCount);
        }
        
        private void updateListeners(long[] newListeners, int newListenerCount)
        {
            this.listeners = newListeners;
            this.listenerCount = newListenerCount;
            Invariants.checkArgumentSorted(newListeners, 0, newListenerCount);
        }

        void mergeInts(int[] vis, int viSize, int[] vjs, int vjSize, IntMergeConsumer consumer)
        {
            int count = 0;
            int i = 0, j = 0;
            while (i < viSize && j < vjSize)
            {
                int vi = vis[i], vj = vjs[j];
                if (Math.min(vi, vj) < 0)
                {
                    i += vi >>> 31;
                    j += vj >>> 31;
                    continue;
                }

                int c = vi - vj;
                if (c <= 0)
                {
                    vis[count++] = vis[i++];
                    j += c == 0 ? 1 : 0;
                }
                else if (count < i)
                {
                    vis[count++] = vjs[j++];
                }
                else
                {
                    int[] newvis = new int[count + (viSize - i) + (vjSize - j)];
                    System.arraycopy(vis, 0, newvis, 0, count);
                    int remaining = (viSize - i);
                    System.arraycopy(vis, i, newvis, newvis.length - remaining, remaining);
                    vis = newvis;
                    i = vis.length - remaining;
                    viSize = newvis.length;
                }
            }

            while (i < viSize)
            {
                if (vis[i] >= 0)
                    vis[count++] = vis[i];
                i++;
            }

            if (vis.length < count + vjSize - j)
                vis = Arrays.copyOf(vis, Math.max(viSize + viSize/2, count + vjSize - j));

            while (j < vjSize)
            {
                if (vjs[j] >= 0)
                    vis[count++] = vjs[j];
                ++j;
            }

            consumer.accept(this, vis, count);
        }

        void mergeLongs(long[] vis, int viSize, long[] vjs, int vjSize, LongMergeConsumer consumer)
        {
            int count = 0;
            int i = 0, j = 0;
            while (i < viSize && j < vjSize)
            {
                long vi = vis[i], vj = vjs[j];

                Invariants.checkState(vi >= 0);
                Invariants.checkState(vj >= 0);

                long c = vi - vj;
                if (c <= 0)
                {
                    vis[count++] = vis[i++];
                    j += c == 0 ? 1 : 0;
                }
                else if (count < i)
                {
                    vis[count++] = vjs[j++];
                }
                else
                {
                    long[] newvis = new long[count + (viSize - i) + (vjSize - j)];
                    System.arraycopy(vis, 0, newvis, 0, count);
                    int remaining = (viSize - i);
                    System.arraycopy(vis, i, newvis, newvis.length - remaining, remaining);
                    vis = newvis;
                    i = vis.length - remaining;
                    viSize = newvis.length;
                }
            }

            while (i < viSize)
            {
                if (vis[i] >= 0)
                    vis[count++] = vis[i];
                i++;
            }

            if (vis.length < count + vjSize - j)
                vis = Arrays.copyOf(vis, Math.max(viSize + viSize/2, count + vjSize - j));

            while (j < vjSize)
            {
                if (vjs[j] >= 0)
                    vis[count++] = vjs[j];
                ++j;
            }

            consumer.accept(this, vis, count);
        }

        void removeWaitingOn(int storeId)
        {
            int index = find(waitingOn, 0, waitingOnSize, storeId);
            if (index >= 0 && waitingOn[index] >= 0)
            {
                // set tombstone flag and decrement count
                waitingOn[index] |= Integer.MIN_VALUE;
                --waitingOnCount;
                if (waitingOnCount <= waitingOnSize / 2)
                {
                    int count = 0;
                    for (int i = 0 ; i < waitingOnSize ; ++i)
                    {
                        if (waitingOn[i] >= 0)
                            waitingOn[count++] = waitingOn[i];
                    }
                    Invariants.checkState(count == waitingOnCount);
                    waitingOnSize = waitingOnCount;
                }
            }
        }

        // copied and simplified from SortedArrays
        private static int find(int[] waitingOn, int from, int to, int find)
        {
            while (from < to)
            {
                int i = (from + to) >>> 1;
                int c = Integer.compare(find, waitingOn[i] & Integer.MAX_VALUE);
                if (c < 0) to = i;
                else if (c > 0) from = i + 1;
                else return i;
            }
            return -1 - from;
        }

        @Override
        public String toString()
        {
            return awaitSaveStatus(await) + "+" + awaitDurability(await);
        }
    }

    static class Listeners
    {
        /**
         * listeners with their associated status they are waiting for;
         * this is a sorted list populated [start..end)
         */
        StatusListeners[] statusListeners;
        int start, end;

        Listeners(StatusListeners statusListeners)
        {
            this.statusListeners = new StatusListeners[] { statusListeners };
            this.end = 1;
        }

        Listeners merge(Listeners that)
        {
            int count = 0;
            int i = this.start, j = that.start;
            while (i < this.end && j < that.end)
            {
                int c = Integer.compare(this.statusListeners[i].await, that.statusListeners[j].await);
                if (c == 0)
                {
                    this.statusListeners[count++] = this.statusListeners[i++].merge(that.statusListeners[j++]);
                }
                else if (c < 0)
                {
                    this.statusListeners[count++] = this.statusListeners[i++];
                }
                else if (count < i)
                {
                    this.statusListeners[count++] = that.statusListeners[j++];
                }
                else
                {
                    StatusListeners[] newStatusListeners = new StatusListeners[count + (this.end - i) + (that.end - j)];
                    System.arraycopy(statusListeners, 0, newStatusListeners, 0, count);
                    int remaining = (this.end - i);
                    System.arraycopy(statusListeners, i, newStatusListeners, newStatusListeners.length - remaining, remaining);
                    statusListeners = newStatusListeners;
                    i = statusListeners.length - remaining;
                    end = statusListeners.length;
                }
            }

            while (i < this.end)
                statusListeners[count++] = this.statusListeners[i++];

            if (statusListeners.length < count + that.end - j)
                statusListeners = Arrays.copyOf(statusListeners, Math.max(statusListeners.length + statusListeners.length/2, count + that.end - j));

            while (j < that.end)
                statusListeners[count++] = that.statusListeners[j++];

            if (count < end)
                Arrays.fill(statusListeners, count, end, null);

            start = 0;
            end = count;
            return this;
        }
    }

    class Register implements Registration
    {
        final TxnId txnId;
        final SaveStatus awaitSaveStatus;
        final Durability awaitDurability;
        final Node.Id listeningNodeId;
        final int callbackId;

        int[] waitingOn = new int[4];
        int count = 0;

        Register(TxnId txnId, SaveStatus awaitSaveStatus, Durability awaitDurability, Node.Id listeningNodeId, int callbackId)
        {
            this.txnId = txnId;
            this.awaitSaveStatus = awaitSaveStatus;
            this.awaitDurability = awaitDurability;
            this.listeningNodeId = listeningNodeId;
            this.callbackId = callbackId;
        }

        @Override
        public synchronized void add(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            Invariants.checkState(safeCommand.current().saveStatus().compareTo(awaitSaveStatus) < 0);
            if (count == waitingOn.length)
            {
                trim();
                if (count >= waitingOn.length/2)
                {
                    int[] newWaitingOn = new int[count * 2];
                    System.arraycopy(waitingOn, 0, newWaitingOn, 0, count);
                    waitingOn = newWaitingOn;
                }
            }
            waitingOn[count++] = safeStore.commandStore().id();
        }

        private void trim()
        {
            Arrays.sort(waitingOn, 0, count);
            int removedCount = 0;
            for (int i = 1 ; i < count ; ++i)
            {
                if (waitingOn[i - 1] == waitingOn[i]) ++removedCount;
                else if (removedCount > 0) waitingOn[i - removedCount] = waitingOn[i];
            }
            count -= removedCount;
        }

        @Override
        public int done()
        {
            if (count == 0)
                return 0;

            trim();
            if (waitingOn.length > 4 && count < waitingOn.length / 2)
                waitingOn = Arrays.copyOf(waitingOn, count);

            StatusListeners listener = new StatusListeners(awaitSaveStatus, awaitDurability, waitingOn, count, listeningNodeId.id, callbackId);
            listeners.merge(txnId, new Listeners(listener), Listeners::merge);
            return count;
        }
    }

    final NotifySink notifySink;
    final ConcurrentHashMap<TxnId, Listeners> listeners = new ConcurrentHashMap<>();

    public DefaultRemoteListeners(NotifySink notifySink)
    {
        this.notifySink = notifySink;
    }

    public DefaultRemoteListeners(Node node)
    {
        this.notifySink = new DefaultNotifySink(node);
    }

    @Override
    public Registration register(TxnId txnId, SaveStatus awaitSaveStatus, Durability awaitDurability, Node.Id listener, int callbackId)
    {
        Invariants.checkArgument(callbackId >= 0);
        return new Register(txnId, awaitSaveStatus, awaitDurability, listener, callbackId);
    }

    @Override
    public void notify(SafeCommandStore safeStore, SafeCommand safeCommand, Command prev)
    {
        TxnId txnId = safeCommand.txnId();
        Listeners entry = this.listeners.get(txnId);
        if (entry == null)
            return;

        int storeId = safeStore.commandStore().id();
        SaveStatus newStatus = safeCommand.current().saveStatus();
        Durability newDurability = safeCommand.current().durability();

        int start = entry.start, end = entry.end;
        StatusListeners[] listeners = entry.statusListeners;
        for (int i = start ; i < end ; ++i)
        {
            StatusListeners listener = listeners[i];
            if (awaitSaveStatus(listener.await).compareTo(newStatus) > 0)
                return;

            if (awaitDurability(listener.await).compareTo(newDurability) > 0)
                continue;

            listener.removeWaitingOn(storeId);
            if (listener.waitingOnCount == 0)
            {
                // if we get invalidated we don't save the route, so we take the combined route of before and after the new status
                Route<?> route = Route.merge(safeCommand.current().route(), prev == null ? null : (Route)prev.route());
                notifySink.notify(txnId, newStatus, route, listener.listeners, listener.listenerCount);
                if (i != start)
                    System.arraycopy(listeners, start, listeners, start + 1, i - start);
                listeners[start] = null;
                start = ++entry.start;
            }
        }
    }

    private static int listenerNodeId(long encodedListener)
    {
        return (int) (encodedListener >>> 31);
    }

    private static int listenerCallbackId(long encodedListener)
    {
        return (int) (encodedListener & 0x3fffffff);
    }

    @VisibleForTesting
    public static long encodeListener(int nodeId, int callbackId)
    {
        Invariants.checkArgument(callbackId >= 0);
        return callbackId | ((long)nodeId << 31);
    }

    static
    {
        Invariants.checkState(Durability.maxOrdinal() < 16);
    }

    private static SaveStatus awaitSaveStatus(int encodedAwait)
    {
        return SaveStatus.forOrdinal(encodedAwait >>> 4);
    }

    private static Durability awaitDurability(int encodedAwait)
    {
        return Durability.forOrdinal(encodedAwait & 0xf);
    }

    private static int encodeAwait(SaveStatus awaitSaveStatus, Durability awaitDurability)
    {
        return (awaitSaveStatus.ordinal() << 4) | awaitDurability.ordinal();
    }
}
