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
import accord.api.RoutingKey;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.SortedArrays.Search;

import java.util.Objects;

import javax.annotation.Nullable;

import static accord.utils.SortedArrays.Search.CEIL;
import static accord.utils.SortedArrays.Search.FAST;

/**
 * A range of keys
 */
public abstract class Range implements Comparable<RoutableKey>, Unseekable, Seekable
{
    public static class EndInclusive extends Range
    {
        public EndInclusive(RoutingKey start, RoutingKey end)
        {
            super(start, end);
        }

        @Override
        public int compareTo(RoutableKey key)
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
        public Range newRange(RoutingKey start, RoutingKey end)
        {
            return new EndInclusive(start, end);
        }

        @Override
        public String toString()
        {
            return "Range(" + start() + ", " + end() + ']';
        }
    }

    public static class StartInclusive extends Range
    {
        public StartInclusive(RoutingKey start, RoutingKey end)
        {
            super(start, end);
        }

        @Override
        public int compareTo(RoutableKey key)
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
        public Range newRange(RoutingKey start, RoutingKey end)
        {
            return new StartInclusive(start, end);
        }

        @Override
        public String toString()
        {
            return "Range[" + start() + ", " + end() + ')';
        }
    }

    public static Range range(RoutingKey start, RoutingKey end, boolean startInclusive, boolean endInclusive)
    {
        return new Range(start, end) {

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
            public Range newRange(RoutingKey start, RoutingKey end)
            {
                throw new UnsupportedOperationException("subRange");
            }

            @Override
            public int compareTo(RoutableKey key)
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

    private final RoutingKey start;
    private final RoutingKey end;

    private Range(RoutingKey start, RoutingKey end)
    {
        if (start.compareTo(end) >= 0)
            throw new IllegalArgumentException(start + " >= " + end);
        if (startInclusive() == endInclusive())
            throw new IllegalStateException("Range must have one side inclusive, and the other exclusive. Range of different types should not be mixed.");
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

    @Override
    public final Domain domain() { return Domain.Range; }

    public abstract boolean startInclusive();
    public abstract boolean endInclusive();

    public abstract Range newRange(RoutingKey start, RoutingKey end);

    @Override
    public Key asKey()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Range asRange()
    {
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Range that = (Range) o;
        return Objects.equals(start, that.start) && Objects.equals(end, that.end);
    }

    @Override
    public int hashCode()
    {
        return start.hashCode() * 31 + end.hashCode();
    }

    /**
     * Returns a negative integer, zero, or a positive integer as the provided key is greater than, contained by,
     * or less than this range.
     */
    @Override
    public abstract int compareTo(RoutableKey key);

    public boolean contains(RoutableKey key)
    {
        return compareTo(key) == 0;
    }

    /**
     * Returns a negative integer, zero, or a positive integer if both points of the provided range are less than, the
     * range intersects this range, or both points are greater than this range
     */
    public int compareIntersecting(Range that)
    {
        if (that.getClass() != this.getClass())
            throw new IllegalArgumentException("Cannot mix Range of different types");
        if (this.start.compareTo(that.end) >= 0)
            return 1;
        if (this.end.compareTo(that.start) <= 0)
            return -1;
        return 0;
    }

    /**
     * Sorts by start then end
     */
    public int compare(Range that)
    {
        if (that.getClass() != this.getClass())
            throw new IllegalArgumentException("Cannot mix Range of different types");
        int c = this.start.compareTo(that.start);
        if (c == 0) c = this.end.compareTo(that.end);
        return c;
    }

    public boolean contains(Range that)
    {
        return that.start.compareTo(this.start) >= 0 && that.end.compareTo(this.end) <= 0;
    }

    @Override
    public Range slice(Range truncateTo)
    {
        int cs = start.compareTo(truncateTo.start);
        int ce = end.compareTo(truncateTo.end);
        if (cs >= 0 && ce <= 0) return this;
        return newRange(cs >= 0 ? start : truncateTo.start, ce <= 0 ? end : truncateTo.end);
    }

    public int compareTo(Range range)
    {
        return compare(range);
    }

    public boolean intersects(AbstractKeys<?, ?> keys)
    {
        return SortedArrays.binarySearch(keys.keys, 0, keys.size(), this, Range::compareTo, FAST) >= 0;
    }

    /**
     * Returns a range covering the overlapping parts of this and the provided range, returns
     * null if the ranges do not overlap
     */
    public Range intersection(Range that)
    {
        if (this.compareIntersecting(that) != 0)
            return null;

        RoutingKey start = this.start.compareTo(that.start) > 0 ? this.start : that.start;
        RoutingKey end = this.end.compareTo(that.end) < 0 ? this.end : that.end;
        return newRange(start, end);
    }

    /**
     * returns the index of the first key larger than what's covered by this range
     */
    public int nextHigherKeyIndex(AbstractKeys<?, ?> keys, int from)
    {
        int i = SortedArrays.exponentialSearch(keys.keys, from, keys.size(), this, Range::compareTo, Search.FLOOR);
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
        return SortedArrays.exponentialSearch(keys.keys, from, keys.size(), this, Range::compareTo, CEIL);
    }

    @Override
    public RoutingKey someIntersectingRoutingKey(@Nullable Ranges ranges)
    {
        if (ranges == null)
            return startInclusive() ? start.toUnseekable() : end.toUnseekable();

        int i = ranges.indexOf(this);
        Range that = ranges.get(i);
        if (this.start().compareTo(that.start()) <= 0)
        {
            if (startInclusive())
                return that.start();

            if (this.end().compareTo(that.end()) <= 0)
                return this.end();

            return that.end();
        }
        else
        {
            if (startInclusive())
                return this.start();

            if (that.end().compareTo(this.end()) <= 0)
                return that.end();

            return this.end();
        }
    }

    public static Range slice(Range bound, Range toSlice)
    {
        Invariants.checkArgument(bound.compareIntersecting(toSlice) == 0);
        if (bound.contains(toSlice))
            return toSlice;

        return toSlice.newRange(
                toSlice.start().compareTo(bound.start()) >= 0 ? toSlice.start() : bound.start(),
                toSlice.end().compareTo(bound.end()) <= 0 ? toSlice.end() : bound.end()
        );
    }

    @Override
    public Range toUnseekable()
    {
        return this;
    }
}
