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

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.utils.ReducingRangeMap;

import static accord.api.ProtocolModifiers.isRangeEndInclusive;
import static accord.api.ProtocolModifiers.isRangeStartInclusive;
import static accord.primitives.Known.Definition.DefinitionErased;
import static accord.primitives.Known.Definition.DefinitionKnown;
import static accord.primitives.Known.KnownDeps.DepsErased;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.utils.SortedArrays.Search.FAST;

public class KnownMap extends ReducingRangeMap<KnownMap.MinAndMaxKnown>
{
    public static final class MinAndMaxKnown
    {
        public static final MinAndMaxKnown Nothing = new MinAndMaxKnown(Known.Nothing);

        public final Known max;
        // the minimum we witness on a shard that owns the relevant data
        public final @Nullable Known minOwned;

        public MinAndMaxKnown(Known minmax)
        {
            this(minmax, minmax);
        }

        public MinAndMaxKnown(@Nullable Known minOwned, Known max)
        {
            this.minOwned = minOwned;
            this.max  = max;
        }

        public MinAndMaxKnown atLeast(Known that)
        {
            Known max = this.max.atLeast(that);
            if (max.equals(this.max))
                return this;
            return new MinAndMaxKnown(minOwned, max);
        }

        MinAndMaxKnown merge(MinAndMaxKnown that)
        {
            Known max = this.max.atLeast(that.max);
            Known min = Known.nonNullOrMin(this.minOwned, that.minOwned);
            if (max.equals(this.max) && Objects.equals(min, this.minOwned))
                return this;
            if (max.equals(that.max) && Objects.equals(min, that.minOwned))
                return that;
            return new MinAndMaxKnown(min, max);
        }

        public Known nonNullOrMax(@Nullable Known known)
        {
            if (known == null)
                return max;
            return known.atLeast(max);
        }

        public Known nonNullOrMin(@Nullable Known known)
        {
            if (known == null)
                return minOwned;
            if (minOwned == null)
                return known;
            return known.min(minOwned);
        }

        public Known nonNullOrMinMax(@Nullable Known known)
        {
            if (known == null)
                return max;
            return known.min(max);
        }

        public Known minOwnedElse(Known ifNull)
        {
            return minOwned != null ? minOwned : ifNull;
        }

        public boolean isFullyTruncated()
        {
            return max.isTruncated() && (minOwned != null && minOwned.isTruncated());
        }

        @Override
        public boolean equals(Object that)
        {
            return that instanceof MinAndMaxKnown && equals((MinAndMaxKnown) that);
        }

        public boolean equals(MinAndMaxKnown that)
        {
            return max.equals(that.max) && Objects.equals(minOwned, that.minOwned);
        }

        @Override
        public String toString()
        {
            return "[" + minOwned + "..." + max + "]";
        }
    }

    public static class SerializerSupport
    {
        public static KnownMap create(RoutingKey[] ends, MinAndMaxKnown[] values)
        {
            return new KnownMap(ends, values);
        }
    }

    public static final KnownMap EMPTY = new KnownMap();

    private transient final Known validForAll;

    private KnownMap()
    {
        this.validForAll = Known.Nothing;
    }

    public KnownMap(RoutingKey[] starts, MinAndMaxKnown[] values)
    {
        this(starts, values, Known.Nothing);
    }

    private KnownMap(RoutingKey[] starts, MinAndMaxKnown[] values, Known validForAll)
    {
        super(starts, values);
        this.validForAll = validForAll;
    }

    public static KnownMap create(Unseekables<?> keysOrRanges, SaveStatus saveStatus)
    {
        return create(keysOrRanges, saveStatus.known);
    }

    public static KnownMap create(Unseekables<?> keysOrRanges, Known known)
    {
        return create(keysOrRanges, new MinAndMaxKnown(known));
    }

    public static KnownMap create(Unseekables<?> keysOrRanges, MinAndMaxKnown known)
    {
        if (keysOrRanges.isEmpty())
            return new KnownMap();

        return create(keysOrRanges, known, Builder::new);
    }

    public static KnownMap merge(KnownMap a, KnownMap b)
    {
        return ReducingRangeMap.merge(a, b, MinAndMaxKnown::merge, Builder::new);
    }

    public Known computeValidForAll(Unseekables<?> routeOrParticipants)
    {
        Known validForAll = foldlWithDefault(routeOrParticipants, KnownMap::reduceKnownFor, MinAndMaxKnown.Nothing, null);
        return this.validForAll.atLeast(validForAll).validForAll();
    }

    public KnownMap with(Known validForAll)
    {
        if (validForAll.equals(this.validForAll))
            return this;

        int i = 0;
        for (; i < size(); ++i)
        {
            MinAndMaxKnown pre = values[i];
            if (pre == null)
                continue;

            MinAndMaxKnown post = pre.atLeast(validForAll);
            if (!pre.equals(post))
                break;
        }

        if (i == size())
            return new KnownMap(starts, values, validForAll);

        RoutingKey[] newStarts = new RoutingKey[size() + 1];
        MinAndMaxKnown[] newValues = new MinAndMaxKnown[size()];
        System.arraycopy(starts, 0, newStarts, 0, i);
        System.arraycopy(values, 0, newValues, 0, i);
        int count = i;
        while (i < size())
        {
            MinAndMaxKnown pre = values[i++];
            MinAndMaxKnown post = pre == null ? null : pre.atLeast(validForAll);
            if (count == 0 || !Objects.equals(post, newValues[count - 1]))
            {
                newStarts[count] = starts[i-1];
                newValues[count++] = post;
            }
        }
        newStarts[count] = starts[size()];
        if (count != newValues.length)
        {
            newValues = Arrays.copyOf(newValues, count);
            newStarts = Arrays.copyOf(newStarts, count + 1);
        }
        return new KnownMap(newStarts, newValues, validForAll);
    }

    public boolean hasAnyFullyTruncated(Routables<?> routables)
    {
        return foldlWithDefault(routables, (known, prev) -> known.isFullyTruncated(), MinAndMaxKnown.Nothing, false, i -> i);
    }

    public boolean hasFullyTruncated(Routables<?> routables)
    {
        return foldlWithDefault(routables, (known, prev) -> known.isFullyTruncated(), MinAndMaxKnown.Nothing, true, i -> !i);
    }

    public boolean hasTruncated()
    {
        return foldl((known, prev) -> known.max.isTruncated(), false, i -> i);
    }

    public Known knownFor(Routables<?> owns, Routables<?> touches)
    {
        Known known;
        if (owns.isEmpty())
        {
            known = knownForAny();
            if (known.is(DefinitionErased))
                known = known.with(DefinitionKnown);
        }
        else
        {
             known = validForAll.atLeast(foldlWithDefault(owns, KnownMap::reduceKnownFor, MinAndMaxKnown.Nothing, null, i -> false));
        }
        if (touches.isEmpty())
        {
            if (known.is(DepsErased))
                known = known.with(DepsKnown);
        }
        else if (owns != touches)
        {
            Known knownDeps = validForAll.atLeast(foldlWithDefault(touches, KnownMap::reduceKnownFor, MinAndMaxKnown.Nothing, null, i -> false));
            known = known.with(knownDeps.deps());
        }
        return known;
    }

    public Known knownForAny()
    {
        return validForAll.atLeast(foldl(MinAndMaxKnown::nonNullOrMax, null, i -> false));
    }

    public Ranges matchingRanges(Predicate<MinAndMaxKnown> match)
    {
        return foldlWithBounds((known, ranges, start, end) -> match.test(known) ? ranges.with(Ranges.of(start.rangeFactory().newRange(start, end))) : ranges, Ranges.EMPTY, i -> false);
    }

    private static Known reduceKnownFor(MinAndMaxKnown bounds, @Nullable Known prev)
    {
        if (prev == null)
            return bounds.max;

        return prev.reduce(bounds.max);
    }

    // TODO (expected): test this
    public Participants<?> knownFor(Known required, Participants<?> expect)
    {
        return foldlWithBounds(expect, (known, prev, start, end) -> {
            if (known == null || !required.isSatisfiedBy(known.max))
            {
                if (start != null && end != null)
                    return prev.without(Ranges.of(start.rangeFactory().newAntiRange(start, end)));

                if (start == null && end == null)
                    return prev.slice(0, 0);

                if (start == null)
                {
                    int i = prev.find(end, FAST);
                    if (i < 0)
                        return prev.slice(-1 - i, prev.size());

                    if (prev.domain() == Routable.Domain.Key)
                        return prev.slice(i + (isRangeEndInclusive() ? 1 : 0), prev.size());

                    Range r = prev.get(i).asRange();
                    prev = prev.slice(i, prev.size());
                    if (!r.end().equals(end))
                        prev = prev.with((Participants)Ranges.of(r.newRange(r.end(), end)));
                }
                else
                {
                    int i = prev.find(start, FAST);
                    if (i < 0)
                        return prev.slice(0, -1 - i);

                    if (prev.domain() == Routable.Domain.Key)
                        return prev.slice(0, i + (isRangeStartInclusive() ? 0 : 1));

                    Range r = prev.get(i).asRange();
                    prev = prev.slice(0, i);
                    if (!r.start().equals(start))
                        prev = prev.with((Participants)Ranges.of(r.newRange(r.start(), start)));
                }
            }
            return prev;
        }, expect, i -> false);
    }

    public static class Builder extends AbstractBoundariesBuilder<RoutingKey, MinAndMaxKnown, KnownMap>
    {
        public Builder(int capacity)
        {
            super(capacity);
        }

        @Override
        protected KnownMap buildInternal()
        {
            if (values.isEmpty())
                return EMPTY;

            return new KnownMap(starts.toArray(new RoutingKey[0]), values.toArray(new MinAndMaxKnown[0]));
        }
    }
}

