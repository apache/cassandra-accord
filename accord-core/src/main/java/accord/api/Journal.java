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

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.local.Command;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.primitives.EpochSupplier;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.PersistentField.Persister;
import accord.utils.async.AsyncChain;
import org.agrona.collections.Int2ObjectHashMap;

/**
 * Persisted journal for transactional recovery.
 */
public interface Journal
{
    Journal start(Node node);

    Command loadCommand(int store, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore);
    Command.Minimal loadMinimal(int store, TxnId txnId, Load load, RedundantBefore redundantBefore, DurableBefore durableBefore);

    void saveCommand(int store, CommandUpdate value, OnDone onDone);

    Iterator<? extends TopologyUpdate> replayTopologies();
    void saveTopology(TopologyUpdate topologyUpdate, OnDone onDone);

    void purge(CommandStores commandStores, EpochSupplier minEpoch);
    void replay(CommandStores commandStores);

    RedundantBefore loadRedundantBefore(int store);
    NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int store);
    NavigableMap<Timestamp, Ranges> loadSafeToRead(int store);
    CommandStores.RangesForEpoch loadRangesForEpoch(int store);

    Persister<DurableBefore, DurableBefore> durableBeforePersister();

    void saveStoreState(int store, FieldUpdates fieldUpdates, OnDone onDone);

    class TopologyUpdate
    {
        public final Int2ObjectHashMap<CommandStores.RangesForEpoch> commandStores;
        public final Topology local;
        public final Topology global;

        public TopologyUpdate(@Nonnull Int2ObjectHashMap<CommandStores.RangesForEpoch> commandStores, @Nonnull Topology local, @Nonnull Topology global)
        {
            this.commandStores = commandStores;
            this.local = local;
            this.global = global;
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            TopologyUpdate update = (TopologyUpdate) object;
            return Objects.equals(commandStores, update.commandStores) && Objects.equals(local, update.local) && Objects.equals(global, update.global);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(commandStores, local, global);
        }

        @Override
        public String toString()
        {
            return "TopologyUpdate{" +
                   "local=" + local +
                   ", commandStores=" + commandStores +
                   ", global=" + global +
                   '}';
        }
    }

    class CommandUpdate
    {
        public final TxnId txnId;
        public final Command before;
        public final Command after;

        public CommandUpdate(@Nullable Command before, @Nonnull Command after)
        {
            this.txnId = after.txnId();
            this.before = before;
            this.after = after;
        }
    }

    class FieldUpdates
    {
        public RedundantBefore newRedundantBefore;
        public NavigableMap<TxnId, Ranges> newBootstrapBeganAt;
        public NavigableMap<Timestamp, Ranges> newSafeToRead;
        public CommandStores.RangesForEpoch newRangesForEpoch;

        public String toString()
        {
            StringBuilder builder = new StringBuilder("FieldUpdates{");
            if (newRedundantBefore != null)
                builder.append("newRedundantBefore=").append(newRedundantBefore).append(", ");
            if (newBootstrapBeganAt != null)
                builder.append("newBootstrapBeganAt=").append(newBootstrapBeganAt).append(", ");
            if (newSafeToRead != null)
                builder.append("newSafeToRead=").append(newSafeToRead).append(", ");
            if (newRangesForEpoch != null)
                builder.append("newRangesForEpoch=").append(newRangesForEpoch).append(", ");
            builder.setLength(builder.length() - 2);
            builder.append('}');
            return builder.toString();
        }
    }

    enum Load
    {
        ALL,
        PURGEABLE,
        MINIMAL
    }

    /**
     * Helper for CommandStore to restore Command states.
     */
    interface Loader
    {
        AsyncChain<Command> load(TxnId txnId);
    }


    interface OnDone
    {
        public static OnDone fromRunnable(Runnable r)
        {
            return new OnDone()
            {
                @Override
                public void success()
                {
                    r.run();
                }

                public void failure(Throwable t)
                {
                    throw new RuntimeException(t);
                }
            };
        }
        void success();
        void failure(Throwable t);
    }
}