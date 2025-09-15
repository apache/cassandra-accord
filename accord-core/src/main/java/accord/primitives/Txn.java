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
import accord.api.Sliceable;
import accord.api.Update;
import accord.local.CommandStore;
import accord.local.SafeCommandStore;
import accord.utils.Invariants;
import accord.utils.TinyEnumSet;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import static accord.primitives.Routables.Slice.Minimal;

public interface Txn
{
    enum Witnesses { Writes, ReadsOrWrites, AnyVisible }
    enum WitnessedBy { All, WritesOrSyncPoints, SyncPoints, Nothing }
    /**
     * NOTE: we keep Read/Write adjacent to make it easier to check for non-standard flags in serialization
     */
    enum Kind
    {
        Read('R', true, false, false, false, Witnesses.Writes, WitnessedBy.WritesOrSyncPoints),
        Write('W', true, false, false, false, Witnesses.ReadsOrWrites, WitnessedBy.All),

        /**
         * A non-durable read that cannot be recovered and provides only per-key linearizability guarantees.
         * This may be used to implement single-partition-key reads with strict serializable isolation OR
         * weaker isolation multi-key/range reads for interoperability with weaker isolation systems.
         */
        EphemeralRead('E', false, false, false, true, Witnesses.Writes, WitnessedBy.Nothing),

        /**
         * A pseudo-transaction whose deps represent the complete set of transactions that may execute before it,
         * without interfering with their execution.
         *
         * Any transaction with a lower TxnId that is not witnessed by this transaction will not be executed,
         * i.e. earlier TxnId that had not reached consensus before it did must be retried with a higher TxnId,
         * so that replicas that are bootstrapping may ignore lower TxnId and still be sure they have a complete
         * representation of the reified transaction log.
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
         * This all ensures the effect of this transaction on invalidation of earlier transactions is durable.
         *
         * Other transactions do not typically take a dependency upon an ExclusiveSyncPoint as part of coordination,
         * however during execution on a bootstrapping replica the sync point may be inserted as a dependency until
         * the bootstrap has progressed far enough to know which transactions will be executed before the bootstrap
         * (and therefore should be pruned from dependencies, as their outcome will be included in the bootstrap)
         * and those which will be executed after, on the replica (and therefore should be retained as dependencies).
         *
         * Invisible to other transactions.
         */
        ExclusiveSyncPoint('X', true, true, true, true, Witnesses.AnyVisible, WitnessedBy.SyncPoints),
        /**
         * An ExclusiveSyncPoint that does not filter its dependencies so that they may be used for
         * ensuring a new epoch's quorum can perform coordination without earlier epochs
         */
        VisibilitySyncPoint('V', true, true, true, true, Witnesses.AnyVisible, WitnessedBy.SyncPoints)
        ;

        public static class Kinds extends TinyEnumSet<Kind>
        {
            public Kinds(Kind ... kinds)
            {
                super(kinds);
            }

            public Kinds(int bitset)
            {
                super(bitset);
            }

            public Kinds or(Kinds or)
            {
                return or(this, or, Kinds::new);
            }

            public boolean test(TxnId txnId)
            {
                return txnId.is(this);
            }
        }

        // in future: BlindWrite, Interactive?

        private static final Kind[] VALUES = Kind.values();
        private static final int COUNT = VALUES.length;
        private static final long ENCODED_ORDINAL_INFO;
        private static final long ENCODED_WITNESSES_INFO;
        private static final int IS_VISIBLE_ORDINAL_INFO_OFFSET = 0;
        private static final int IS_SYNCPOINT_ORDINAL_INFO_OFFSET = COUNT;
        private static final int IS_SYSTEM_ORDINAL_INFO_OFFSET = 2 * COUNT;
        private static final int AWAITS_ONLY_DEPS_ORDINAL_INFO_OFFSET = 3 * COUNT;

        public static final Kinds Nothing = new Kinds();
        public static final Kinds Ws = new Kinds(Write);
        public static final Kinds RsOrWs = new Kinds(Write, Read);
        public static final Kinds WsOrSyncPoints = new Kinds(Write, ExclusiveSyncPoint, VisibilitySyncPoint);
        public static final Kinds SyncPoints = new Kinds(ExclusiveSyncPoint, VisibilitySyncPoint);
        public static final Kinds AnyVisible = new Kinds(Write, Read, ExclusiveSyncPoint, VisibilitySyncPoint);
        public static final Kinds All = new Kinds(Write, Read, EphemeralRead, ExclusiveSyncPoint, VisibilitySyncPoint);
        private static final Kinds[] WITNESSES = new Kinds[] { Ws, RsOrWs, AnyVisible };
        private static final Kinds[] WITNESSED_BY = new Kinds[] { All, WsOrSyncPoints, SyncPoints, Nothing };

        static
        {
            Invariants.require(AWAITS_ONLY_DEPS_ORDINAL_INFO_OFFSET + VALUES.length <= 64);
            long encodedOrdinalInfo = 0;
            Map<Character, Kind> shortNames = new HashMap<>();
            for (Kind kind : VALUES)
            {
                Invariants.require(null == shortNames.putIfAbsent(kind.shortName, kind), "Short name conflict between: " + kind + " and " + shortNames.get(kind.shortName));
                if (kind.isVisible()) encodedOrdinalInfo   |= 1L << (IS_VISIBLE_ORDINAL_INFO_OFFSET + kind.ordinal());
                if (kind.isSyncPoint()) encodedOrdinalInfo |= 1L << (IS_SYNCPOINT_ORDINAL_INFO_OFFSET + kind.ordinal());
                if (kind.isSystemTxn()) encodedOrdinalInfo |= 1L << (IS_SYSTEM_ORDINAL_INFO_OFFSET + kind.ordinal());
                if (kind.awaitsOnlyDeps()) encodedOrdinalInfo |= 1L << (AWAITS_ONLY_DEPS_ORDINAL_INFO_OFFSET + kind.ordinal());
            }
            ENCODED_ORDINAL_INFO = encodedOrdinalInfo;
            Invariants.require(COUNT * COUNT <= 64);
            int offset = 0;
            long encodedWitnessesInfo = 0L;
            for (Kind witness : VALUES)
            {
                for (Kind witnessed : VALUES)
                {
                    if (witness.witnessesKinds().test(witnessed))
                        encodedWitnessesInfo |= 1L << offset;
                    ++offset;
                }
            }
            ENCODED_WITNESSES_INFO = encodedWitnessesInfo;
        }

        private final char shortName;
        public final boolean isVisible;
        public final boolean isSyncPoint;
        public final boolean isSystem;
        public final boolean awaitsOnlyDeps;
        public final Witnesses witnesses;
        public final WitnessedBy witnessedBy;

        Kind(char shortName, boolean isVisible, boolean isSyncPoint, boolean isSystem, boolean awaitsOnlyDeps, Witnesses witnesses, WitnessedBy witnessedBy)
        {
            this.shortName = shortName;
            this.isVisible = isVisible;
            this.isSyncPoint = isSyncPoint;
            this.isSystem = isSystem;
            this.awaitsOnlyDeps = awaitsOnlyDeps;
            this.witnesses = witnesses;
            this.witnessedBy = witnessedBy;
        }

        public boolean isWrite()
        {
            return this == Write;
        }

        public boolean isDurable()
        {
            return this != EphemeralRead;
        }

        public boolean isVisible()
        {
            return isVisible;
        }

        public boolean isSyncPoint()
        {
            return isSyncPoint;
        }

        public boolean isSystemTxn()
        {
            return isSystem;
        }

        /**
         * An ExclusiveSyncPoint and EphemeralRead execute only after all of their dependencies, and have no logical executeAt.
         */
        public boolean awaitsOnlyDeps()
        {
            return awaitsOnlyDeps;
        }

        public static Kind forOrdinal(int ordinal)
        {
            return VALUES[ordinal];
        }

        public static boolean isVisible(int ordinal)
        {
            return 0 != (ENCODED_ORDINAL_INFO & (1L << (IS_VISIBLE_ORDINAL_INFO_OFFSET + ordinal)));
        }

        public static boolean isSyncPoint(int ordinal)
        {
            return 0 != (ENCODED_ORDINAL_INFO & (1L << (IS_SYNCPOINT_ORDINAL_INFO_OFFSET + ordinal)));
        }

        public static boolean isSystemTxn(int ordinal)
        {
            return 0 != (ENCODED_ORDINAL_INFO & (1L << (IS_SYSTEM_ORDINAL_INFO_OFFSET + ordinal)));
        }

        public static boolean awaitsOnlyDeps(int ordinal)
        {
            return 0 != (ENCODED_ORDINAL_INFO & (1L << (AWAITS_ONLY_DEPS_ORDINAL_INFO_OFFSET + ordinal)));
        }

        public static boolean awaitsPreviouslyOwned(int ordinal)
        {
            return ordinal == ExclusiveSyncPoint.ordinal() || ordinal == VisibilitySyncPoint.ordinal();
        }

        public Kinds witnessesKinds()
        {
            return WITNESSES[witnesses.ordinal()];
        }

        public boolean witnesses(TxnId txnId)
        {
            return witnesses(ordinal(), TxnId.kindOrdinal(txnId.flagsUnmasked()));
        }

        public boolean witnesses(Kind kind)
        {
            return witnesses(this.ordinal(), kind.ordinal());
        }

        static boolean witnesses(int witnessOrdinal, int witnessedOrdinal)
        {
            return 0 != ((1L << (witnessOrdinal * COUNT + witnessedOrdinal)) & ENCODED_WITNESSES_INFO);
        }

        public boolean witnessedBy(Kind kind)
        {
            return kind.witnesses(this);
        }

        public Kinds witnessedByKinds()
        {
            return WITNESSED_BY[witnessedBy.ordinal()];
        }

        public char shortName()
        {
            return shortName;
        }

        public static Txn.Kind forShortName(char ch)
        {
            for (Txn.Kind kind : VALUES)
            {
                if (kind.shortName == ch)
                    return kind;
            }
            return null;
        }
    }

    class InMemory implements Txn
    {
        private final Kind kind;
        private final Seekables<?, ?> keys;
        private final Read read;
        private final Query query;
        private final Update update;
        // TODO (desired): maybe introduce a C* Txn object instead of stashing this here
        public final Sliceable implementationDefined;

        public InMemory(@Nonnull Seekables<?, ?> keys, @Nonnull Read read, @Nonnull Query query)
        {
            this(Kind.Read, keys, read, query, null, null);
        }

        public InMemory(@Nonnull Seekables<?, ?> keys, @Nonnull Read read, @Nonnull Query query, Sliceable implementationDefined)
        {
            this(Kind.Read, keys, read, query, null, implementationDefined);
        }

        public InMemory(@Nonnull Seekables<?, ?> keys, @Nonnull Read read, @Nonnull Query query, @Nullable Update update)
        {
            this(Kind.Write, keys, read, query, update, null);
        }

        public InMemory(@Nonnull Seekables<?, ?> keys, @Nonnull Read read, @Nonnull Query query, @Nullable Update update, Sliceable implementationDefined)
        {
            this(Kind.Write, keys, read, query, update, implementationDefined);
        }

        public InMemory(@Nonnull Kind kind, @Nonnull Seekables<?, ?> keys, @Nonnull Read read, @Nullable Query query, @Nullable Update update)
        {
            this(kind, keys, read, query, update, null);
        }

        public InMemory(@Nonnull Kind kind, @Nonnull Seekables<?, ?> keys, @Nonnull Read read, @Nullable Query query, @Nullable Update update, Sliceable implementationDefined)
        {
            this.kind = kind;
            this.keys = keys;
            this.read = read;
            this.update = update;
            this.query = query;
            this.implementationDefined = implementationDefined;
            Invariants.require(!kind.isSyncPoint() || keys.domain() == Routable.Domain.Range);
        }

        @Override
        public PartialTxn slice(Ranges ranges, boolean includeQuery)
        {
            return new PartialTxn.InMemory(
                kind(), keys().slice(ranges, Minimal),
                read().slice(ranges), includeQuery ? query() : null,
                update() == null ? null : update().slice(ranges),
                implementationDefined == null ? null : implementationDefined.slice(ranges)
            );
        }

        @Nonnull
        @Override
        public PartialTxn intersecting(Participants<?> participants, boolean includeQuery)
        {
            return new PartialTxn.InMemory(
                kind(), keys().intersecting(participants, Minimal),
                read.intersecting(participants),
                includeQuery ? query() : null,
                update == null ? null : update.intersecting(participants),
                implementationDefined == null ? null : implementationDefined.intersecting(participants)
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

    default boolean covers(Route<?> route) { return true; }
    default boolean covers(Unseekables<?> participants) { return true; }

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

    default AsyncChain<Data> read(SafeCommandStore safeStore, Timestamp executeAt, Participants<?> execute)
    {
        Seekables<?, ?> keys = read().keys().intersecting(execute, Minimal);
        int count = keys.size();
        switch (count)
        {
            case 0: return read().read(safeStore, null, executeAt);
            case 1: return read().read(safeStore, keys.get(0), executeAt);
            default:
            {
                List<AsyncChain<Data>> chains = new ArrayList<>(keys.size());
                Read read = read();
                for (int i = 0 ; i < count ; ++i)
                    chains.add(read.read(safeStore, keys.get(i), executeAt));

                return AsyncChains.reduce(chains, Data::merge);
            }
        }
    }

    default AsyncChain<Data> readDirect(CommandStore commandStore, Timestamp executeAt, Participants<?> execute)
    {
        Seekables<?, ?> keys = read().keys().intersecting(execute, Minimal);
        int count = keys.size();
        switch (count)
        {
            case 0:
            {
                return read().readDirect(commandStore, null, executeAt).then(head -> new AsyncChains.MapLink<>(head){
                    @Override
                    public Data apply(Data data)
                    {
                        Invariants.nonNull(data, "Read.readDirect is not allowed to return null");
                        return data;
                    }
                });
            }
            case 1: return read().readDirect(commandStore, keys.get(0), executeAt);
            default:
            {
                List<AsyncChain<Data>> chains = new ArrayList<>(keys.size());
                Read read = read();
                for (int i = 0 ; i < count ; ++i)
                    chains.add(read.readDirect(commandStore, keys.get(i), executeAt));

                return AsyncChains.reduce(chains, Data::merge);
            }
        }
    }
}
