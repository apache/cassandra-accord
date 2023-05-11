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
import accord.utils.Invariants;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * A collection of transaction dependencies, keyed by the key or range on which they were adopted
 */
public class Deps
{
    public static final Deps NONE = new Deps(KeyDeps.NONE, RangeDeps.NONE);

    public static Builder builder()
    {
        return new Builder();
    }

    // TODO (expected, efficiency): cache this object per thread
    public static abstract class AbstractBuilder<T extends Deps> implements AutoCloseable
    {
        final KeyDeps.Builder keyBuilder;
        RangeDeps.Builder rangeBuilder;

        AbstractBuilder()
        {
            this.keyBuilder = KeyDeps.builder();
        }

        public AbstractBuilder<T> add(Seekable keyOrRange, TxnId txnId)
        {
            switch (keyOrRange.domain())
            {
                default: throw new AssertionError();
                case Key:
                    keyBuilder.add(keyOrRange.asKey(), txnId);
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
            return new Deps(keyBuilder.build(), rangeBuilder == null ? RangeDeps.NONE : rangeBuilder.build());
        }
    }

    public final KeyDeps keyDeps;
    public final RangeDeps rangeDeps;

    public Deps(KeyDeps keyDeps, RangeDeps rangeDeps)
    {
        this.keyDeps = keyDeps;
        this.rangeDeps = rangeDeps;
    }

    public boolean contains(TxnId txnId)
    {
        return keyDeps.contains(txnId) || rangeDeps.contains(txnId);
    }

    public Deps with(Deps that)
    {
        return new Deps(this.keyDeps.with(that.keyDeps), this.rangeDeps.with(that.rangeDeps));
    }

    public Deps without(Predicate<TxnId> remove)
    {
        return new Deps(keyDeps.without(remove), rangeDeps.without(remove));
    }

    public PartialDeps slice(Ranges covering)
    {
        return slice(covering, covering);
    }

    public PartialDeps slice(Ranges covering, Ranges slice)
    {
        return new PartialDeps(covering, keyDeps.slice(slice), rangeDeps.slice(slice));
    }

    public boolean isEmpty()
    {
        return keyDeps.isEmpty() && rangeDeps.isEmpty();
    }

    public int txnIdCount()
    {
        return keyDeps.txnIdCount() + rangeDeps.txnIdCount();
    }

    public int indexOf(TxnId txnId)
    {
        switch (txnId.domain())
        {
            default: throw new AssertionError();
            case Key:
                return Arrays.binarySearch(keyDeps.txnIds, txnId);
            case Range:
                int i = Arrays.binarySearch(rangeDeps.txnIds, txnId);
                if (i < 0) i -= keyDeps.txnIdCount();
                else i += keyDeps.txnIdCount();
                return i;
        }
    }

    public TxnId txnId(int i)
    {
        return i < keyDeps.txnIdCount()
                ? keyDeps.txnId(i)
                : rangeDeps.txnId(i - keyDeps.txnIdCount());
    }

    public TxnId minTxnId()
    {
        if (keyDeps.isEmpty() && rangeDeps.isEmpty())
            throw new IndexOutOfBoundsException();
        if (keyDeps.isEmpty()) return rangeDeps.txnId(0);
        if (rangeDeps.isEmpty()) return keyDeps.txnId(0);
        return TxnId.min(keyDeps.txnId(0), rangeDeps.txnId(0));
    }

    public List<TxnId> txnIds()
    {
        final int txnIdCount = txnIdCount();
        final int keyDepsCount = keyDeps.txnIdCount();
        return new AbstractList<TxnId>() {
            @Override
            public TxnId get(int index)
            {
                return index < keyDepsCount
                        ? keyDeps.txnId(index)
                        : rangeDeps.txnId(index - keyDepsCount);
            }

            @Override
            public int size()
            {
                return txnIdCount;
            }
        };
    }

    public List<TxnId> txnIds(Seekable keyOrRange)
    {
        List<TxnId> keyIds, rangeIds;
        switch (keyOrRange.domain())
        {
            default: throw new AssertionError();
            case Key:
            {
                Key key = keyOrRange.asKey();
                keyIds = keyDeps.txnIds(key);
                rangeIds = rangeDeps.txnIds(key);
                break;
            }
            case Range:
            {
                Range range = keyOrRange.asRange();
                keyIds = keyDeps.txnIds(range);
                rangeIds = rangeDeps.txnIds(range);
            }
        }

        if (rangeIds.isEmpty()) return keyIds;
        if (keyIds.isEmpty()) return rangeIds;

        List<TxnId> output = new ArrayList<>(keyIds.size() + rangeIds.size());
        int ki = 0, ri = 0;
        while (ki < keyIds.size() && ri < rangeIds.size())
        {
            int c = keyIds.get(ki).compareTo(rangeIds.get(ri));
            Invariants.checkState(c != 0);
            if (c < 0) output.add(keyIds.get(ki++));
            else output.add(rangeIds.get(ri++));
        }
        while (ki < keyIds.size())
            output.add(keyIds.get(ki++));
        while (ri < rangeIds.size())
            output.add(rangeIds.get(ri++));
        return output;
    }

    public Unseekables<?, ?> someUnseekables(TxnId txnId)
    {
        switch (txnId.domain())
        {
            default:    throw new AssertionError();
            case Key:   return keyDeps.someUnseekables(txnId);
            case Range: return rangeDeps.someUnseekables(txnId);
        }
    }

    // NOTE: filter only applied to keyDeps
    public void forEachUniqueTxnId(Ranges ranges, Consumer<TxnId> forEach)
    {
        keyDeps.forEachUniqueTxnId(ranges, forEach);
        rangeDeps.forEachUniqueTxnId(ranges, forEach);
    }

    public static <T> Deps merge(List<T> list, Function<T, Deps> getter)
    {
        return new Deps(KeyDeps.merge(list, getter, deps -> deps.keyDeps),
                        RangeDeps.merge(list, getter, deps -> deps.rangeDeps));
    }

    @Override
    public String toString()
    {
        return keyDeps.toString() + ", " + rangeDeps.toString();
    }

    @Override
    public boolean equals(Object that)
    {
        return this == that || (that instanceof Deps && equals((Deps)that));
    }

    public boolean equals(Deps that)
    {
        return this.keyDeps.equals(that.keyDeps) && this.rangeDeps.equals(that.rangeDeps);
    }

    public @Nullable TxnId maxTxnId()
    {
        TxnId maxKeyDep = keyDeps.isEmpty() ? null : keyDeps.txnId(keyDeps.txnIdCount() - 1);
        TxnId maxRangeDep = rangeDeps.isEmpty() ? null : rangeDeps.txnId(rangeDeps.txnIdCount() - 1);
        return TxnId.nonNullOrMax(maxKeyDep, maxRangeDep);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }
}
