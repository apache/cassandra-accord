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

package accord.primitives;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import accord.api.Data;
import accord.api.DataResolver;
import accord.api.Key;
import accord.api.Query;
import accord.api.Read;
import accord.api.RepairWrites;
import accord.api.Result;
import accord.api.UnresolvedData;
import accord.api.Update;
import accord.local.Command;
import accord.local.SafeCommandStore;
import accord.primitives.Routable.Domain;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.nonNull;

public interface Txn
{
    enum Kind
    {
        Read,
        Write,

        /**
         * A pseudo-transaction whose deps represent the complete set of transactions that may execute before it,
         * without interfering with their execution.
         *
         * A SyncPoint is unique in that it does not agree an executeAt, but instead agrees a precise collection of
         * dependencies that represent a superset of the transactions that have reached consensus to execute before
         * their txnId. This set of dependencies will be made durable in the Accept round, and re-proposed by recovery
         * if the transaction is not fully committed (but was durably accepted).
         *
         * This is only safe because the transaction does not really "execute" and does not order itself with respect to
         * others, it only orders others with respect to itself, so its executeAt can be declared to be its txnId.
         * In effect it represents an inequality relation, rather than a precise point in the transaction log - its
         * dependencies permit saying that we are "after" its point in the log, not that we are *at* that point.
         * This permits us to use the dependencies from the PreAccept round.
         *
         * Note, it would be possible to do a three-round operation that achieved this with a precise "at" position
         * in the log, with a second round between PreAccept and Accept to collect deps < executeAt, if executeAt &gt; txnId,
         * but we do not need this property here.
         *
         * This all ensures the effect of this transaction on invalidation of earlier transactions is durable.
         * This is most useful for ExclusiveSyncPoint.
         */
        SyncPoint,

        /**
         * A {@link #SyncPoint} that invalidates transactions with lower TxnId that it does not witness, i.e. it ensures
         * that earlier TxnId that had not reached consensus before it did must be retried with a higher TxnId,
         * so that replicas that are bootstrapping may ignore lower TxnId and still be sure they have a complete
         * representation of the reified transaction log.
         */
        ExclusiveSyncPoint;

        // in future: BlindWrite, Interactive?

        private static final Kind[] VALUES = Kind.values();

        public boolean isWrite()
        {
            return this == Write;
        }

        public boolean isRead()
        {
            return this == Read;
        }

        /**
         * Does the transaction propose dependencies as part of its Accept round, i.e. make durable a set of dependencies
         *
         * Note that this it is only possible to do this for transactions whose execution time is not dependent on
         * others, i.e. where we may safely propose executeAt = txnId regardless of when it is witnessed by
         * replicas
         */
        public boolean proposesDeps()
        {
            return this == ExclusiveSyncPoint || this == SyncPoint;
        }

        public static Kind ofOrdinal(int ordinal)
        {
            return VALUES[ordinal];
        }
    }

    class InMemory implements Txn
    {
        private final Kind kind;
        private final Seekables<?, ?> keys;
        private final Read read;
        private final DataResolver readResolver;
        private final Query query;
        private final Update update;

        public InMemory(@Nonnull Seekables<?, ?> keys, @Nonnull Read read, @Nonnull DataResolver readResolver, @Nonnull Query query)
        {
            nonNull(readResolver, "readResolver is null");
            this.kind = Kind.Read;
            this.keys = keys;
            this.read = read;
            this.query = query;
            this.readResolver = readResolver;
            this.update = null;
        }

        public InMemory(@Nonnull Seekables<?, ?> keys, @Nonnull Read read, @Nonnull DataResolver readResolver, @Nonnull Query query, @Nullable Update update)
        {
            nonNull(readResolver, "readResolver is null");
            this.kind = Kind.Write;
            this.keys = keys;
            this.read = read;
            this.readResolver = readResolver;
            this.update = update;
            this.query = query;
        }

        public InMemory(@Nonnull Kind kind, @Nonnull Seekables<?, ?> keys, @Nonnull Read read, @Nonnull DataResolver readResolver, @Nullable Query query, @Nullable Update update)
        {
            nonNull(readResolver, "readResolver is null");
            this.kind = kind;
            this.keys = keys;
            this.read = read;
            this.readResolver = readResolver;
            this.update = update;
            this.query = query;
        }

        @Override
        public PartialTxn slice(Ranges ranges, boolean includeQuery)
        {
            return new PartialTxn.InMemory(
                    ranges, kind(), keys().slice(ranges),
                    read().slice(ranges), readResolver(), includeQuery ? query() : null,
                    update() == null ? null : update().slice(ranges)
            );
        }

        @Override
        public Kind kind()
        {
            return kind;
        }

        @Override
        public Seekables<?, ?> keys()
        {
            return keys;
        }

        @Override
        public Read read()
        {
            return read;
        }

        @Override
        public Query query()
        {
            return query;
        }

        @Override
        public Update update()
        {
            return update;
        }
        
        @Override
        public DataResolver readResolver()
        {
            return readResolver;
        }

        @Override
        public DataConsistencyLevel writeDataCL()
        {
            return update != null ? update.writeDataCl() : DataConsistencyLevel.UNSPECIFIED;
        }

        @Override
        public DataConsistencyLevel readDataCL()
        {
            return read.readDataCL();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Txn txn = (Txn) o;
            return kind() == txn.kind()
                    && keys().equals(txn.keys())
                    && read().equals(txn.read())
                    && Objects.equals(query(), txn.query())
                    && Objects.equals(update(), txn.update());
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        public String toString()
        {
            return "{read:" + read() + (update() != null ? ", update:" + update() : "") + '}';
        }
    }

    @Nonnull Kind kind();
    @Nonnull Seekables<?, ?> keys();
    @Nonnull Read read();
    @Nonnull
    DataResolver readResolver();
    @Nullable Query query(); // may be null only in PartialTxn
    @Nullable Update update();

    @Nonnull PartialTxn slice(Ranges ranges, boolean includeQuery);

    default boolean isWrite()
    {
        return kind().isWrite();
    }

    default DataConsistencyLevel writeDataCL()
    {
        return DataConsistencyLevel.UNSPECIFIED;
    }

    default DataConsistencyLevel readDataCL()
    {
        return DataConsistencyLevel.UNSPECIFIED;
    }

    default Result result(TxnId txnId, Timestamp executeAt, @Nullable Data data)
    {
        return query().compute(txnId, executeAt, keys(), data, read(), update());
    }

    default Writes execute(Timestamp executeAt, @Nullable Data data, @Nullable RepairWrites repairWrites)
    {
        checkArgument(repairWrites == null || !repairWrites.isEmpty());
        Update update = update();
        if (update == null)
        {
            if (repairWrites != null)
                return new Writes(executeAt, repairWrites.keys(), repairWrites.toWrite());
            else
                return new Writes(executeAt, Keys.EMPTY, null);
        }

        // Update keys might not include keys needing repair
        Seekables keys = update.keys();
        if (repairWrites != null)
            keys = keys.with(repairWrites.keys());

        return new Writes(executeAt, keys, update.apply(data, repairWrites));
    }

    default AsyncChain<UnresolvedData> read(SafeCommandStore safeStore, @Nullable RoutingKeys dataReadKeys, @Nullable Read followupRead, Command.Committed command)
    {
        Ranges ranges = safeStore.ranges().at(command.executeAt().epoch());
        List<AsyncChain<UnresolvedData>> futures = Routables.foldlMinimal(read().keys(), ranges, (key, accumulate, index) -> {
            Read read = followupRead != null ? followupRead : read();
            checkArgument(dataReadKeys == null || key.domain() == Domain.Key || !readDataCL().requiresDigestReads, "Digest reads are unsupported for ranges");
            boolean digestRead = readDataCL().requiresDigestReads
                                 && dataReadKeys != null
                                 && !dataReadKeys.contains(((Key)key).toUnseekable());
            AsyncChain<UnresolvedData> result = read.read(key, digestRead, kind(), safeStore, command.executeAt(), safeStore.dataStore());
            accumulate.add(result);
            return accumulate;
        }, new ArrayList<>());
        return AsyncChains.reduce(futures, UnresolvedData::mergeForReduce);
    }
}
