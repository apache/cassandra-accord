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
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

import accord.api.RoutingKey;
import accord.local.ShardDistributor;
import accord.primitives.RangeFactory;
import accord.primitives.RoutableKey;
import accord.primitives.Ranges;
import accord.primitives.Keys;
import accord.utils.Invariants;

import static accord.utils.Utils.toArray;
import javax.annotation.Nonnull;

public abstract class IntHashKey implements RoutableKey
{
    public static class Splitter implements ShardDistributor.EvenSplit.Splitter<Long>
    {
        @Override
        public Long sizeOf(accord.primitives.Range range)
        {
            return ((IntHashKey)range.end()).hash - (long)((IntHashKey)range.start()).hash;
        }

        @Override
        public accord.primitives.Range subRange(accord.primitives.Range range, Long start, Long end)
        {
            Invariants.checkArgument(((IntHashKey)range.start()).hash + end.intValue() <= ((IntHashKey)range.end()).hash);
            return range.newRange(
                    new Hash(((IntHashKey)range.start()).hash + start.intValue()),
                    new Hash(((IntHashKey)range.start()).hash + end.intValue())
            );
        }

        @Override
        public Long valueOf(int v)
        {
            return (long)v;
        }

        @Override
        public Long zero()
        {
            return 0L;
        }

        @Override
        public Long add(Long a, Long b)
        {
            return a + b;
        }

        @Override
        public Long subtract(Long a, Long b)
        {
            return a - b;
        }

        @Override
        public Long divide(Long a, int i)
        {
            return a / i;
        }

        @Override
        public Long divide(Long a, Long b)
        {
            return a / b;
        }

        @Override
        public Long multiply(Long a, int i)
        {
            return a * i;
        }

        @Override
        public int min(Long v, int i)
        {
            return (int)Math.min(v, i);
        }

        @Override
        public int compare(Long a, Long b)
        {
            return a.compareTo(b);
        }
    }

    public static final class Key extends IntHashKey implements accord.api.Key
    {
        public Key(int key)
        {
            super(key);
        }
    }

    public static final class Hash extends IntHashKey implements RoutingKey
    {
        public Hash(int hash)
        {
            super(hash, true);
        }

        @Override
        public accord.primitives.Range asRange()
        {
            return new Range(new Hash(hash - 1), new Hash(hash));
        }

        @Override
        public RoutingKey asRoutingKey()
        {
            return this;
        }

        @Override
        public RangeFactory rangeFactory()
        {
            return (s, e) -> new Range((Hash) s, (Hash) e);
        }
    }

    public static class Range extends accord.primitives.Range.EndInclusive
    {
        public Range(Hash start, Hash end)
        {
            super(start, end);
        }

        @Override
        public accord.primitives.Range newRange(RoutingKey start, RoutingKey end)
        {
            return new Range((Hash) start, (Hash) end);
        }

        public Ranges split(int count)
        {
            int startHash = ((IntHashKey)start()).hash;
            int endHash = ((IntHashKey)end()).hash;
            int currentSize = endHash - startHash;
            if (currentSize < count)
                return Ranges.of(this);
            int interval =  currentSize / count;

            int last = 0;
            accord.primitives.Range[] ranges = new accord.primitives.Range[count];
            for (int i=0; i<count; i++)
            {
                int subStart = i > 0 ? last : startHash;
                int subEnd = i < count - 1 ? subStart + interval : endHash;
                ranges[i] = new Range(new Hash(subStart), new Hash(subEnd));
                last = subEnd;
            }
            return Ranges.ofSortedAndDeoverlapped(ranges);
        }
    }

    public final int key;
    public final int hash;

    private IntHashKey(int key)
    {
        if (key == Integer.MIN_VALUE)
            throw new IllegalArgumentException();
        this.key = key;
        this.hash = hash(key);
    }

    private IntHashKey(int hash, boolean isHash)
    {
        assert isHash;
        this.key = Integer.MIN_VALUE;
        this.hash = hash;
    }

    public static Key key(int k)
    {
        return new Key(k);
    }

    public static Hash forHash(int hash)
    {
        return new Hash(hash);
    }

    public static Keys keys(int k0, int... kn)
    {
        accord.api.Key[] keys = new accord.api.Key[kn.length + 1];
        keys[0] = key(k0);
        for (int i=0; i<kn.length; i++)
            keys[i + 1] = key(kn[i]);

        return Keys.of(keys);
    }

    public static accord.primitives.Range[] ranges(int count)
    {
        List<accord.primitives.Range> result = new ArrayList<>();
        long delta = 0xffff / count;
        long start = 0;
        Hash prev = new Hash((int)start);
        for (int i = 1 ; i < count ; ++i)
        {
            Hash next = new Hash((int)Math.min(0xffff, start + i * delta));
            result.add(new Range(prev, next));
            prev = next;
        }
        result.add(new Range(prev, new Hash(0xffff)));
        return toArray(result, accord.primitives.Range[]::new);
    }

    public static accord.primitives.Range range(Hash start, Hash end)
    {
        return new Range(start, end);
    }

    @Override
    public String toString()
    {
        if (key == Integer.MIN_VALUE && hash(key) != hash) return "#" + hash;
        return Integer.toString(key);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntHashKey that = (IntHashKey) o;
        return key == that.key && hash == that.hash;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(key, hash);
    }

    private static int hash(int key)
    {
        CRC32 crc32c = new CRC32();
        crc32c.update(key);
        crc32c.update(key >> 8);
        crc32c.update(key >> 16);
        crc32c.update(key >> 24);
        return (int)crc32c.getValue() & 0xffff;
    }

    @Override
    public RoutingKey toUnseekable()
    {
        if (key == Integer.MIN_VALUE)
            return this instanceof Hash ? (Hash)this : new Hash(hash);

        return forHash(hash);
    }

    @Override
    public int compareTo(@Nonnull RoutableKey that)
    {
        return Integer.compare(this.hash, ((IntHashKey)that).hash);
    }
}
