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
import javax.annotation.Nonnull;

import accord.api.RoutingKey;
import accord.local.ShardDistributor;
import accord.primitives.RangeFactory;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.utils.CRCUtils;
import accord.utils.Invariants;

import static accord.primitives.Range.AntiRangeMarker.ANTI_RANGE;
import static accord.utils.Utils.toArray;

public abstract class PrefixedIntHashKey implements RoutableKey
{
    public static final int MIN_KEY = Integer.MIN_VALUE + 1;

    public static class Splitter implements ShardDistributor.EvenSplit.Splitter<Long>
    {
        @Override
        public Long sizeOf(accord.primitives.Range range)
        {
            return ((PrefixedIntHashKey) range.end()).hash - (long) ((PrefixedIntHashKey) range.start()).hash;
        }

        @Override
        public accord.primitives.Range subRange(accord.primitives.Range range, Long start, Long end)
        {
            PrefixedIntHashKey currentStart = (PrefixedIntHashKey) range.start();
            Invariants.requireArgument(currentStart.hash + end.intValue() <= ((PrefixedIntHashKey) range.end()).hash);
            return range.newRange(new Hash(currentStart.prefix, currentStart.hash + start.intValue()),
                                  new Hash(currentStart.prefix, currentStart.hash + end.intValue()));
        }

        @Override
        public Long zero()
        {
            return 0L;
        }

        @Override
        public Long valueOf(int v)
        {
            return (long) v;
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
            return (int) Math.min(v, i);
        }

        @Override
        public int compare(Long a, Long b)
        {
            return a.compareTo(b);
        }
    }

    public static final class Key extends PrefixedIntHashKey implements accord.api.Key
    {
        private Key(int prefix, int key, int hash)
        {
            super(prefix, key, hash);
        }

        @Override
        public Object suffix()
        {
            return hash + ":" + key;
        }
    }

    public static abstract class PrefixedIntRoutingKey extends PrefixedIntHashKey implements RoutingKey
    {
        private PrefixedIntRoutingKey(int prefix, int hash)
        {
            super(prefix, Integer.MIN_VALUE, hash);
        }

        @Override
        public RangeFactory rangeFactory()
        {
            return new RangeFactory()
            {
                @Override
                public accord.primitives.Range newRange(RoutingKey s, RoutingKey e)
                {
                    PrefixedIntRoutingKey start = (PrefixedIntRoutingKey) s;
                    PrefixedIntRoutingKey end = (PrefixedIntRoutingKey) e;
                    Invariants.require(start.prefix == end.prefix, "Unable to create range from different prefixes; %s has a different prefix than %s", start, end);
                    return new Range(start, end);
                }

                @Override
                public accord.primitives.Range newAntiRange(RoutingKey s, RoutingKey e)
                {
                    return new Range((PrefixedIntRoutingKey) s, (PrefixedIntRoutingKey) e, ANTI_RANGE);
                }
            };
        }

        @Override
        public Object prefix()
        {
            return prefix;
        }
    }

    public static final class Sentinel extends PrefixedIntRoutingKey
    {
        private final boolean isMin;

        private Sentinel(int prefix, boolean isMin)
        {
            super(prefix, isMin ? Integer.MIN_VALUE : Integer.MAX_VALUE);
            this.isMin = isMin;
        }

        @Override
        public accord.primitives.Range asRange()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RoutingKey asRoutingKey()
        {
            return this;
        }

        @Override
        public Object suffix()
        {
            return isMin ? "-Inf" : "+Inf";
        }
    }

    public static final class Hash extends PrefixedIntRoutingKey
    {
        public Hash(int prefix, int hash)
        {
            super(prefix, hash);
        }

        /**
         * Parse a Hash from string format "prefix#hash"
         * @param str String in format "prefix#hash", e.g. "0#32960"
         * @return Hash instance
         * @throws IllegalArgumentException if string format is invalid
         */
        public static Hash fromString(String str)
        {
            if (str == null || str.trim().isEmpty())
                throw new IllegalArgumentException("String cannot be null or empty");
            
            String trimmed = str.trim();
            int hashIndex = trimmed.indexOf('#');
            if (hashIndex == -1)
                throw new IllegalArgumentException("Invalid format: expected 'prefix#hash', got: " + str);
            
            try
            {
                int prefix = Integer.parseInt(trimmed.substring(0, hashIndex));
                int hash = Integer.parseInt(trimmed.substring(hashIndex + 1));
                return new Hash(prefix, hash);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Invalid number format in: " + str, e);
            }
        }

        @Override
        public accord.primitives.Range asRange()
        {
            PrefixedIntRoutingKey start = hash == Integer.MIN_VALUE ?
                                          new Sentinel(prefix, true) :
                                          new Hash(prefix, hash - 1);
            return new Range(start, new Hash(prefix, hash));
        }

        @Override
        public RoutingKey asRoutingKey()
        {
            return this;
        }

        @Override
        public Object suffix()
        {
            return hash;
        }

        @Override
        public String toString()
        {
            return prefix + "#" + hash;
        }
    }

    public static class Range extends accord.primitives.Range.EndInclusive
    {
        private Range(PrefixedIntRoutingKey start, PrefixedIntRoutingKey end)
        {
            super(start, end);
        }

        private Range(PrefixedIntRoutingKey start, PrefixedIntRoutingKey end, AntiRangeMarker antiRange)
        {
            super(start, end, antiRange);
        }

        @Override
        public accord.primitives.Range newRange(RoutingKey s, RoutingKey e)
        {
            PrefixedIntRoutingKey start = (PrefixedIntRoutingKey) s;
            PrefixedIntRoutingKey end = (PrefixedIntRoutingKey) e;
            Invariants.require(start.prefix == end.prefix, "Unable to create range from different prefixes; %s has a different prefix than %s", start, end);
            return new Range(start, end);
        }

        @Override
        public accord.primitives.Range newAntiRange(RoutingKey start, RoutingKey end)
        {
            return new Range((PrefixedIntRoutingKey) start, (PrefixedIntRoutingKey) end);
        }

        public Ranges split(int count)
        {
            int prefix = ((PrefixedIntRoutingKey) start()).prefix;
            int startHash = ((PrefixedIntRoutingKey) start()).hash;
            int endHash = ((PrefixedIntRoutingKey) end()).hash;
            int currentSize = endHash - startHash;
            if (currentSize < count)
                return Ranges.of(this);
            int interval = currentSize / count;

            int last = 0;
            accord.primitives.Range[] ranges = new accord.primitives.Range[count];
            for (int i = 0; i < count; i++)
            {
                int subStart = i > 0 ? last : startHash;
                int subEnd = i < count - 1 ? subStart + interval : endHash;
                ranges[i] = new Range(new Hash(prefix, subStart), new Hash(prefix, subEnd));
                last = subEnd;
            }
            return Ranges.ofSortedAndDeoverlapped(ranges);
        }
    }

    public final int prefix;
    public final int key;
    public final int hash;
    private final boolean isHash;

    private PrefixedIntHashKey(int prefix, int key, int hash)
    {
        this.prefix = prefix;
        this.isHash = key == Integer.MIN_VALUE; // this constructor is only used by Key
        this.key = key;
        this.hash = hash;
    }

    public static Key key(int prefix, int k, int hash)
    {
        return new Key(prefix, k, hash);
    }

    public static Hash forHash(int prefix, int hash)
    {
        return new Hash(prefix, hash);
    }

    public static accord.primitives.Range[] ranges(int prefix, int count)
    {
        return ranges(prefix, Integer.MIN_VALUE, Integer.MAX_VALUE, count);
    }

    public static accord.primitives.Range[] ranges(int prefix, int startHash, int endHash, int count)
    {
        assert startHash < endHash : String.format("%d >= %d", startHash, endHash);
        List<accord.primitives.Range> result = new ArrayList<>(count);
        long domainSize = (long) endHash - (long) startHash + 1L;
        long delta = domainSize / count;
        Hash prev = new Hash(prefix, startHash);
        for (int i = 1; i < count; ++i)
        {
            Hash next = new Hash(prefix, (int) (startHash + i * delta));
            result.add(range(prev, next));
            prev = next;
        }
        result.add(range(prev, new Hash(prefix, endHash)));
        return toArray(result, accord.primitives.Range[]::new);
    }

    public static Range range(PrefixedIntRoutingKey start, PrefixedIntRoutingKey end)
    {
        Invariants.require(start.prefix == end.prefix, "Unable to create range from different prefixes; %s has a different prefix than %s", start, end);
        return new Range(start, end);
    }

    public static Range range(int prefix, int start, int end)
    {
        return new Range(new Hash(prefix, start), new Hash(prefix, end));
    }

    @Override
    public String toString()
    {
        if (isHash) return prefix + "#" + hash;
        return prefix + ":" + key + '#' + hash;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrefixedIntHashKey that = (PrefixedIntHashKey) o;
        return prefix == that.prefix && key == that.key && hash == that.hash;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(prefix, key, hash);
    }

    public static int hash(int key)
    {
        return CRCUtils.crc32LittleEnding(key);
    }

    @Override
    public RoutingKey toUnseekable()
    {
        if (key == Integer.MIN_VALUE)
            return this instanceof Hash ? (Hash) this : new Hash(prefix, hash);

        return forHash(prefix, hash);
    }

    @Override
    public int compareTo(@Nonnull RoutableKey that)
    {
        PrefixedIntHashKey other = (PrefixedIntHashKey) that;
        int rc = Integer.compare(prefix, other.prefix);
        if (rc == 0)
        {
            rc = Integer.compare(this.hash, other.hash);
            if (rc == 0)
            {
                if (this instanceof Sentinel || that instanceof Sentinel)
                {
                    int leftInt = this instanceof Sentinel ? (((Sentinel) this).isMin ? -1 : 1) : 0;
                    int rightInt = that instanceof Sentinel ? (((Sentinel) that).isMin ? -1 : 1) : 0;
                    rc = Integer.compare(leftInt, rightInt);
                }
                else if (this instanceof Key && that instanceof Key)
                {
                    rc = Integer.compare(this.key, other.key);
                }
            }
        }
        return rc;
    }
}
