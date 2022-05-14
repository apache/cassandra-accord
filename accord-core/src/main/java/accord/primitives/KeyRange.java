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

import accord.api.RoutingKey;
import accord.utils.SortedArrays;
import accord.utils.SortedArrays.Search;

import com.google.common.base.Preconditions;

import java.util.Objects;

import static accord.utils.SortedArrays.Search.CEIL;
import static accord.utils.SortedArrays.Search.FAST;

/**
 * A range of keys
 */
public abstract class KeyRange implements Comparable<RoutingKey>
{
    public static class EndInclusive extends KeyRange
    {
        public EndInclusive(RoutingKey start, RoutingKey end)
        {
            super(start, end);
        }

        @Override
        public int compareTo(RoutingKey key)
        {
            if (key.compareTo(start()) <= 0)
                return 1;
            if (key.compareTo(end()) > 0)
                return -1;
            return 0;
        }

        @Override
        public boolean startInclusive()
        {
            return false;
        }

        @Override
        public boolean endInclusive()
        {
            return true;
        }

        @Override
        public KeyRange subRange(RoutingKey start, RoutingKey end)
        {
            return new EndInclusive(start, end);
        }
    }

    public static class StartInclusive extends KeyRange
    {
        public StartInclusive(RoutingKey start, RoutingKey end)
        {
            super(start, end);
        }

        @Override
        public int compareTo(RoutingKey key)
        {
            if (key.compareTo(start()) < 0)
                return 1;
            if (key.compareTo(end()) >= 0)
                return -1;
            return 0;
        }

        @Override
        public boolean startInclusive()
        {
            return true;
        }

        @Override
        public boolean endInclusive()
        {
            return false;
        }

        @Override
        public KeyRange subRange(RoutingKey start, RoutingKey end)
        {
            return new StartInclusive(start, end);
        }
    }

    public static KeyRange range(RoutingKey start, RoutingKey end, boolean startInclusive, boolean endInclusive)
    {
        return new KeyRange(start, end) {

            @Override
            public boolean startInclusive()
            {
                return startInclusive;
            }

            @Override
            public boolean endInclusive()
            {
                return endInclusive;
            }

            @Override
            public KeyRange subRange(RoutingKey start, RoutingKey end)
            {
                throw new UnsupportedOperationException("subRange");
            }

            @Override
            public int compareTo(RoutingKey key)
            {
                if (startInclusive)
                {
                    if (key.compareTo(start()) < 0)
                        return 1;
                }
                else
                {
                    if (key.compareTo(start()) <= 0)
                        return 1;
                }
                if (endInclusive)
                {
                    if (key.compareTo(end()) > 0)
                        return -1;
                }
                else
                {
                    if (key.compareTo(end()) >= 0)
                        return -1;
                }
                return 0;
            }
        };
    }

    private static KeyRange tryMergeExclusiveInclusive(KeyRange left, KeyRange right)
    {
        if (left.getClass() != right.getClass())
            return null;

        Preconditions.checkArgument(left instanceof EndInclusive || left instanceof StartInclusive);

        int cmp = left.compareIntersecting(right);

        if (cmp == 0)
            return left.subRange(left.start.compareTo(right.start) < 0 ? left.start : right.start,
                                 left.end.compareTo(right.end) > 0 ? left.end : right.end);

        if (cmp > 0 && right.end.equals(left.start))
            return left.subRange(right.start, left.end);

        if (cmp < 0 && left.end.equals(right.start))
            return left.subRange(left.start, right.end);

        return null;
    }

    private final RoutingKey start;
    private final RoutingKey end;

    private KeyRange(RoutingKey start, RoutingKey end)
    {
        if (start.compareTo(end) >= 0)
            throw new IllegalArgumentException(start + " >= " + end);
        if (startInclusive() == endInclusive())
            throw new IllegalStateException("KeyRange must have one side inclusive, and the other exclusive. KeyRange of different types should not be mixed.");
        this.start = start;
        this.end = end;
    }

    public final RoutingKey start()
    {
        return start;
    }
    public final RoutingKey end()
    {
        return end;
    }

    public abstract boolean startInclusive();
    public abstract boolean endInclusive();

    public abstract KeyRange subRange(RoutingKey start, RoutingKey end);

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyRange that = (KeyRange) o;
        return Objects.equals(start, that.start) && Objects.equals(end, that.end);
    }

    @Override
    public int hashCode()
    {
        return start.hashCode() * 31 + end.hashCode();
    }

    @Override
    public String toString()
    {
        return "Range[" + start + ", " + end + ']';
    }

    /**
     * Returns a negative integer, zero, or a positive integer as the provided key is less than, contained by,
     * or greater than this range.
     */
    public int compareKey(RoutingKey key)
    {
        return -compareTo(key);
    }

    /**
     * Returns a negative integer, zero, or a positive integer as the provided key is greater than, contained by,
     * or less than this range.
     */
    public abstract int compareTo(RoutingKey key);

    public boolean containsKey(RoutingKey key)
    {
        return compareKey(key) == 0;
    }

    /**
     * Returns a negative integer, zero, or a positive integer if both points of the provided range are less than, the
     * range intersects this range, or both points are greater than this range
     */
    public int compareIntersecting(KeyRange that)
    {
        if (that.getClass() != this.getClass())
            throw new IllegalArgumentException("Cannot mix KeyRange of different types");
        if (this.start.compareTo(that.end) >= 0)
            return 1;
        if (this.end.compareTo(that.start) <= 0)
            return -1;
        return 0;
    }

    public boolean intersects(KeyRange that)
    {
        return compareIntersecting(that) == 0;
    }

    public boolean fullyContains(KeyRange that)
    {
        return that.start.compareTo(this.start) >= 0 && that.end.compareTo(this.end) <= 0;
    }

    public boolean intersects(Keys keys)
    {
        return SortedArrays.binarySearch(keys.keys, 0, keys.size(), this, KeyRange::compareTo, FAST) >= 0;
    }

    /**
     * Returns a range covering the overlapping parts of this and the provided range, returns
     * null if the ranges do not overlap
     */
    public KeyRange intersection(KeyRange that)
    {
        if (this.compareIntersecting(that) != 0)
            return null;

        RoutingKey start = this.start.compareTo(that.start) > 0 ? this.start : that.start;
        RoutingKey end = this.end.compareTo(that.end) < 0 ? this.end : that.end;
        return subRange(start, end);
    }

    /**
     * returns the index of the first key larger than what's covered by this range
     */
    public int nextHigherKeyIndex(AbstractKeys<?, ?> keys, int from)
    {
        int i = SortedArrays.exponentialSearch(keys.keys, from, keys.size(), this, KeyRange::compareTo, Search.FLOOR);
        if (i < 0) i = -1 - i;
        else i += 1;
        return i;
    }

    /**
     * returns the index of the lowest key contained in this range. If the keys object contains no intersecting
     * keys, <code>(-(<i>insertion point</i>) - 1)</code> is returned. Where <i>insertion point</i> is where an
     * intersecting key would be inserted into the keys array
     * @param keys
     */
    public int nextCeilKeyIndex(Keys keys, int from)
    {
        return SortedArrays.exponentialSearch(keys.keys, from, keys.size(), this, KeyRange::compareTo, CEIL);
    }
}
