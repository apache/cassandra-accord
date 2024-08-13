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

import accord.api.Key;
import accord.local.cfk.CommandsForKey;
import accord.utils.Invariants;
import accord.utils.MergeFewDisjointSortedListsCursor;
import accord.utils.SortedCursor;
import accord.utils.SortedList;

import java.util.AbstractList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * A collection of transaction dependencies, keyed by the key or range on which they were adopted.
 *
 * Dependencies are used in the following phases:
 *
 *  - PreAccept calculates dependencies that contain TxnId < the proposed TxnId
 *      - if the fast path is taken, these dependencies are used immediately for execution
 *      - if the slow path is taken, these dependencies are proposed in the Accept phase
 *  - Accept phase calculates dependencies that contain TxnId < the proposed executeAt; these are merged with those found during the PreAccept phase
 *  - Commit phase simply makes these dependencies durable
 *
 *  Dependencies are mostly calculated by CommandsForKey, which has some efficiency tricks, including pruning both itself
 *  and transitive dependencies in any calculation. This means that
 *   - Dependencies will omit any transaction that is known to be a durable dependency of another transaction that has been included.
 *     This mechanism is performed in isolation by each replica, so a final set of Deps may contain redundant transactions.
 *   - CommandsForKey may prune already-applied transactions. This means we can in some circumstances try to calculate
 *     dependencies for a timestamp that occurs before the prune point. If this happens, we insert a future dependency,
 *     so it is possible for Deps to contain TxnId > their intended bound.
 *      - This can occur for regular transactions with old TxnId during PreAccept; in this case we always propose an executeAt that is greater than this inserted Dep
 *      - This can occur for SyncPoint and ExclusiveSyncPoint because they are invisible to other transactions and only
 *        create happens-before edges. So they may have their dependencies calculated at any point after pruning may happen.
 *        In this case, CommandsForKey uses this future dependency to ensure its earlier dependencies are up-to-date before
 *        deciding the execution sequence for the SyncPoint/ExclusiveSyncPoint.
 */
public class Deps
{
    public static final Deps NONE = new Deps(KeyDeps.NONE, RangeDeps.NONE, KeyDeps.NONE);

    public static Builder builder()
    {
        return new Builder();
    }

    // TODO (expected, efficiency): cache this object per thread
    public static abstract class AbstractBuilder<T extends Deps> implements AutoCloseable
    {
        final KeyDeps.Builder keyBuilder;
        RangeDeps.Builder rangeBuilder;
        KeyDeps.Builder directKeyBuilder;

        AbstractBuilder()
        {
            this.keyBuilder = KeyDeps.builder();
        }

        public AbstractBuilder<T> add(Seekable keyOrRange, TxnId txnId)
        {
            Invariants.checkArgument(keyOrRange.domain() == txnId.domain(), keyOrRange + " is not same domain as " + txnId);
            switch (txnId.domain())
            {
                default: throw new AssertionError();
                case Key:
                    if (CommandsForKey.managesExecution(txnId))
                    {
                        keyBuilder.add(keyOrRange.asKey(), txnId);
                    }
                    else
                    {
                        if (directKeyBuilder == null)
                            directKeyBuilder = KeyDeps.builder();
                        directKeyBuilder.add(keyOrRange.asKey(), txnId);
                    }
                    break;

                case Range:
                    if (rangeBuilder == null)
                        rangeBuilder = RangeDeps.builder();
                    rangeBuilder.add(keyOrRange.asRange(), txnId);
                    break;
            }
            return this;
        }

        public abstract T build();

        @Override
        public void close()
        {
            keyBuilder.close();
            if (rangeBuilder != null)
                rangeBuilder.close();
            if (directKeyBuilder != null)
                directKeyBuilder.close();
        }
    }

    public static class Builder extends AbstractBuilder<Deps>
    {
        public Builder()
        {
            super();
        }

        @Override
        public Deps build()
        {
            return new Deps(keyBuilder.build(),
                            rangeBuilder == null ? RangeDeps.NONE : rangeBuilder.build(),
                            directKeyBuilder == null ? KeyDeps.NONE : directKeyBuilder.build());
        }
    }

    /**
     * key dependencies where the execution will be managed by {@link CommandsForKey}, so {@link accord.local.Command.WaitingOn} will wait only on the Key.
     * This is essentially plain reads and writes.
     *
     * i.e. where {@code CommandsForKey.managesExecution}
     */
    public final KeyDeps keyDeps;

    /**
     * Range transaction dependencies. The execution of these is equivalent to {@code directKeyDeps}, in that they are
     * managed via direct relationships in {@link accord.local.Command.WaitingOn}.
     */
    public final RangeDeps rangeDeps;

    /**
     * key dependencies where the execution will be managed by direct dependency relationships, so {@link accord.local.Command.WaitingOn} will wait on the {@code TxnId} directly
     * i.e. where {@code !CommandsForKey.managesExecution}
     */
    public final KeyDeps directKeyDeps;

    public Deps(KeyDeps keyDeps, RangeDeps rangeDeps, KeyDeps directKeyDeps)
    {
        this.keyDeps = keyDeps;
        this.directKeyDeps = directKeyDeps;
        this.rangeDeps = rangeDeps;
    }

    public boolean contains(TxnId txnId)
    {
        Routable.Domain domain = txnId.domain();
        if (domain.isRange())
            return rangeDeps.contains(txnId);

        if (CommandsForKey.managesExecution(txnId))
            return keyDeps.contains(txnId);

        return directKeyDeps.contains(txnId);
    }

    public boolean intersects(TxnId txnId, Ranges ranges)
    {
        Routable.Domain domain = txnId.domain();
        if (domain.isRange())
            return rangeDeps.intersects(txnId, ranges);

        if (CommandsForKey.managesExecution(txnId))
            return keyDeps.intersects(txnId, ranges);

        return directKeyDeps.intersects(txnId, ranges);
    }

    public SortedCursor<TxnId> txnIds(Key key)
    {
        SortedList<TxnId> keyDeps = this.keyDeps.txnIds(key);
        SortedList<TxnId> rangeDeps = this.rangeDeps.computeTxnIds(key);
        SortedList<TxnId> directKeyDeps = this.directKeyDeps.txnIds(key);
        int count = Math.min(1, keyDeps.size()) + Math.min(1, directKeyDeps.size()) + Math.min(1, rangeDeps.size());
        MergeFewDisjointSortedListsCursor<TxnId> cursor = new MergeFewDisjointSortedListsCursor<>(count);
        if (keyDeps.size() > 0)
            cursor.add(keyDeps);
        if (rangeDeps.size() > 0)
            cursor.add(rangeDeps);
        if (directKeyDeps.size() > 0)
            cursor.add(directKeyDeps);
        cursor.init();
        return cursor;
    }

    public Deps with(Deps that)
    {
        return new Deps(this.keyDeps.with(that.keyDeps), this.rangeDeps.with(that.rangeDeps), this.directKeyDeps.with(that.directKeyDeps));
    }

    public Deps without(Predicate<TxnId> remove)
    {
        return new Deps(keyDeps.without(remove), rangeDeps.without(remove), directKeyDeps.without(remove));
    }

    public PartialDeps intersecting(Participants<?> participants)
    {
        return new PartialDeps(participants, keyDeps.intersecting(participants), rangeDeps.intersecting(participants), directKeyDeps.intersecting(participants));
    }

    public boolean isEmpty()
    {
        return keyDeps.isEmpty() && rangeDeps.isEmpty() && directKeyDeps.isEmpty();
    }

    public int txnIdCount()
    {
        return keyDeps.txnIdCount() + rangeDeps.txnIdCount() + directKeyDeps.txnIdCount();
    }

    public TxnId txnId(int i)
    {
        {
            int keyDepsLimit = keyDeps.txnIdCount();
            if (i < keyDepsLimit)
                return keyDeps.txnId(i);
            i -= keyDepsLimit;
        }

        {
            int directKeyDepsLimit = directKeyDeps.txnIdCount();
            if (i < directKeyDepsLimit)
                return directKeyDeps.txnId(i);
            i -= directKeyDepsLimit;
        }

        return rangeDeps.txnId(i);
    }

    public List<TxnId> txnIds()
    {
        return new AbstractList<>()
        {
            @Override
            public TxnId get(int index) { return txnId(index); }
            @Override
            public int size()
            {
                return txnIdCount();
            }
        };
    }

    public Participants<?> participants(TxnId txnId)
    {
        switch (txnId.domain())
        {
            default:    throw new AssertionError();
            case Key:   return CommandsForKey.managesExecution(txnId) ? keyDeps.participants(txnId) : directKeyDeps.participants(txnId);
            case Range: return rangeDeps.participants(txnId);
        }
    }

    // NOTE: filter only applied to keyDeps
    public void forEachUniqueTxnId(Ranges ranges, Consumer<TxnId> forEach)
    {
        keyDeps.forEachUniqueTxnId(ranges, forEach);
        directKeyDeps.forEachUniqueTxnId(ranges, forEach);
        rangeDeps.forEachUniqueTxnId(ranges, forEach);
    }

    public static <T> Deps merge(List<T> list, Function<T, Deps> getter)
    {
        return new Deps(KeyDeps.merge(list, getter, deps -> deps.keyDeps),
                        RangeDeps.merge(list, getter, deps -> deps.rangeDeps),
                        KeyDeps.merge(list, getter, deps -> deps.directKeyDeps));
    }

    @Override
    public String toString()
    {
        return keyDeps + ", " + rangeDeps + ", " + directKeyDeps;
    }

    @Override
    public boolean equals(Object that)
    {
        return this == that || (that instanceof Deps && equals((Deps)that));
    }

    public boolean equals(Deps that)
    {
        return that != null && this.keyDeps.equals(that.keyDeps) && this.rangeDeps.equals(that.rangeDeps) && this.directKeyDeps.equals(that.directKeyDeps);
    }

    public @Nullable TxnId maxTxnId()
    {
        TxnId maxKeyDep = keyDeps.isEmpty() ? null : keyDeps.txnId(keyDeps.txnIdCount() - 1);
        TxnId maxRangeDep = rangeDeps.isEmpty() ? null : rangeDeps.txnId(rangeDeps.txnIdCount() - 1);
        TxnId maxDirectKeyDep = directKeyDeps.isEmpty() ? null : directKeyDeps.txnId(directKeyDeps.txnIdCount() - 1);
        return TxnId.nonNullOrMax(TxnId.nonNullOrMax(maxKeyDep, maxRangeDep), maxDirectKeyDep);
    }

    public TxnId maxTxnId(TxnId orElse)
    {
        return TxnId.nonNullOrMax(maxTxnId(), orElse);
    }

    public @Nullable TxnId minTxnId()
    {
        TxnId minKeyDep = keyDeps.isEmpty() ? null : keyDeps.txnId(0);
        TxnId minRangeDep = rangeDeps.isEmpty() ? null : rangeDeps.txnId(0);
        TxnId minDirectKeyDep = directKeyDeps.isEmpty() ? null : directKeyDeps.txnId(0);
        return TxnId.nonNullOrMin(TxnId.nonNullOrMin(minKeyDep, minRangeDep), minDirectKeyDep);
    }

    public TxnId minTxnId(TxnId orElse)
    {
        return TxnId.nonNullOrMin(minTxnId(), orElse);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }
}
