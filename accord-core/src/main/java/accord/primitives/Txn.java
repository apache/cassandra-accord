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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Data;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Update;
import accord.local.SafeCommandStore;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import java.util.function.Predicate;

import static accord.primitives.Txn.Kind.Kinds.AnyGloballyVisible;
import static accord.primitives.Txn.Kind.Kinds.Nothing;
import static accord.primitives.Txn.Kind.Kinds.RsOrWs;
import static accord.primitives.Txn.Kind.Kinds.ExclusiveSyncPoints;
import static accord.primitives.Txn.Kind.Kinds.Ws;
import static accord.primitives.Txn.Kind.Kinds.WsOrSyncPoints;

public interface Txn
{
    /**
     * NOTE: we keep Read/Write adjacent to make it easier to check for non-standard flags in serialization
     */
    enum Kind
    {
        Read,
        Write,

        /**
         * A non-durable read that cannot be recovered and provides only per-key linearizability guarantees.
         * This may be used to implement single-partition-key reads with strict serializable isolation OR
         * weaker isolation multi-key/range reads for interoperability with weaker isolation systems.
         */
        EphemeralRead,

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
         *
         * Invisible to other transactions.
         */
        SyncPoint,

        /**
         * A {@link #SyncPoint} that invalidates transactions with lower TxnId that it does not witness, i.e. it ensures
         * that earlier TxnId that had not reached consensus before it did must be retried with a higher TxnId,
         * so that replicas that are bootstrapping may ignore lower TxnId and still be sure they have a complete
         * representation of the reified transaction log.
         *
         * Other transactions do not typically take a dependency upon an ExclusiveSyncPoint as part of coordination,
         * however during execution on a bootstrapping replica the sync point may be inserted as a dependency until
         * the bootstrap has progressed far enough to know which transactions will be executed before the bootstrap
         * (and therefore should be pruned from dependencies, as their outcome will be included in the bootstrap)
         * and those which will be executed after, on the replica (and therefore should be retained as dependencies).
         *
         * Invisible to other transactions.
         */
        // TODO (expected): introduce a special kind of visible ExclusiveSyncPoint that creates a precise moment,
        //    and is therefore visible to all transactions.
        ExclusiveSyncPoint('X'),

        /**
         * Used for local book-keeping only, not visible to any other replica or directly to other transactions.
         * This is used to create pseudo transactions that take the place of dependencies that will be fulfilled by a bootstrap.
         */
        LocalOnly;

        Kind()
        {
            this.shortName = name().charAt(0);
        }

        Kind(char shortName)
        {
            this.shortName = shortName;
        }

        public enum Kinds implements Predicate<Kind>
        {
            Nothing,
            Ws,

            /**
             * Any DURABLE read or write. This does not witness EphemeralReads.
             */
            RsOrWs,

            WsOrSyncPoints,
            ExclusiveSyncPoints,
            AnyGloballyVisible;

            @Override
            public boolean test(Kind kind)
            {
                switch (this)
                {
                    default: throw new AssertionError();
                    case AnyGloballyVisible: return kind.isGloballyVisible();
                    case WsOrSyncPoints: return kind == Write || kind == Kind.SyncPoint || kind == ExclusiveSyncPoint;
                    case ExclusiveSyncPoints: return kind == ExclusiveSyncPoint;
                    case RsOrWs: return kind == Read || kind == Write;
                    case Ws: return kind == Write;
                    case Nothing: return false; // TODO (expected, consider): throw exception? we shouldn't ever be presented the option to test even.
                }
            }
        }

        // in future: BlindWrite, Interactive?

        private static final Kind[] VALUES = Kind.values();
        static
        {
            Map<Character, Kind> shortNames = new HashMap<>();
            for (Kind kind : VALUES)
                Invariants.checkState(null == shortNames.putIfAbsent(kind.shortName, kind), "Short name conflict between: " + kind + " and " + shortNames.get(kind.shortName));
        }

        private final char shortName;

        public boolean isWrite()
        {
            return this == Write;
        }

        public boolean isRead()
        {
            return this == Read;
        }

        public boolean isLocal()
        {
            return this == LocalOnly;
        }

        public boolean isDurable()
        {
            return this != EphemeralRead;
        }

        public boolean isGloballyVisible()
        {
            switch (this)
            {
                default: throw new AssertionError("Unhandled Kind: " + this);
                case EphemeralRead:
                case LocalOnly:
                    return false;
                case Write:
                case Read:
                case ExclusiveSyncPoint:
                case SyncPoint:
                    return true;
            }
        }

        public boolean isSyncPoint()
        {
            return this == ExclusiveSyncPoint || this == SyncPoint;
        }

        /**
         * An ExclusiveSyncPoint and EphemeralRead execute only after all of their dependencies, and have no logical executeAt.
         */
        public boolean awaitsOnlyDeps()
        {
            return this == ExclusiveSyncPoint || this == EphemeralRead;
        }

        public static Kind ofOrdinal(int ordinal)
        {
            return VALUES[ordinal];
        }

        public Kinds witnesses()
        {
            switch (this)
            {
                default: throw new AssertionError();
                case EphemeralRead:
                case Read:
                    return Ws;
                case Write:
                case SyncPoint:
                    return RsOrWs;
                case ExclusiveSyncPoint:
                    return AnyGloballyVisible;
            }
        }

        public boolean witnesses(TxnId txnId)
        {
            return witnesses(txnId.kind());
        }

        public boolean witnesses(Kind kind)
        {
            return witnesses().test(kind);
        }

        public Kinds witnessedBy()
        {
            switch (this)
            {
                default: throw new AssertionError();
                case EphemeralRead:
                    return Nothing;
                case Read:
                    return WsOrSyncPoints;
                case Write:
                    return AnyGloballyVisible;
                case SyncPoint:
                case ExclusiveSyncPoint:
                    return ExclusiveSyncPoints;
            }
        }

        public char shortName()
        {
            return shortName;
        }
    }

    class InMemory implements Txn
    {
        private final Kind kind;
        private final Seekables<?, ?> keys;
        private final Read read;
        private final Query query;
        private final Update update;

        public InMemory(@Nonnull Seekables<?, ?> keys, @Nonnull Read read, @Nonnull Query query)
        {
            this.kind = Kind.Read;
            this.keys = keys;
            this.read = read;
            this.query = query;
            this.update = null;
        }

        public InMemory(@Nonnull Seekables<?, ?> keys, @Nonnull Read read, @Nonnull Query query, @Nullable Update update)
        {
            this.kind = Kind.Write;
            this.keys = keys;
            this.read = read;
            this.update = update;
            this.query = query;
        }

        public InMemory(@Nonnull Kind kind, @Nonnull Seekables<?, ?> keys, @Nonnull Read read, @Nullable Query query, @Nullable Update update)
        {
            this.kind = kind;
            this.keys = keys;
            this.read = read;
            this.update = update;
            this.query = query;
        }

        @Override
        public PartialTxn slice(Ranges ranges, boolean includeQuery)
        {
            return new PartialTxn.InMemory(
                kind(), keys().slice(ranges),
                read().slice(ranges), includeQuery ? query() : null,
                update() == null ? null : update().slice(ranges)
            );
        }

        @Nonnull
        @Override
        public PartialTxn intersecting(Participants<?> participants, boolean includeQuery)
        {
            return new PartialTxn.InMemory(
                kind(), keys().intersecting(participants),
                read().intersecting(participants), includeQuery ? query() : null,
                update() == null ? null : update().intersecting(participants)
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
    @Nullable Query query(); // may be null only in PartialTxn
    @Nullable Update update();

    @Nonnull PartialTxn slice(Ranges ranges, boolean includeQuery);
    @Nonnull PartialTxn intersecting(Participants<?> participants, boolean includeQuery);

    default boolean isWrite()
    {
        return kind().isWrite();
    }

    default Result result(TxnId txnId, Timestamp executeAt, @Nullable Data data)
    {
        return query().compute(txnId, executeAt, keys(), data, read(), update());
    }

    default Writes execute(TxnId txnId, Timestamp executeAt, @Nullable Data data)
    {
        Update update = update();
        if (update == null)
            return new Writes(txnId, executeAt, Keys.EMPTY, null);

        return new Writes(txnId, executeAt, update.keys(), update.apply(executeAt, data));
    }

    default AsyncChain<Data> read(SafeCommandStore safeStore, Timestamp executeAt, Ranges unavailable)
    {
        Ranges ranges = safeStore.ranges().allAt(executeAt).without(unavailable);
        List<AsyncChain<Data>> chains = Routables.foldlMinimal(keys(), ranges, (key, accumulate, index) -> {
            AsyncChain<Data> result = read().read(key, safeStore, executeAt, safeStore.dataStore());
            accumulate.add(result);
            return accumulate;
        }, new ArrayList<>());

        if (chains.isEmpty())
            return AsyncChains.success(null);

        return AsyncChains.reduce(chains, Data::merge);
    }
}
