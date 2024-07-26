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

public class KnownMap extends ReducingRangeMap<Known>
{
    public static class SerializerSupport
    {
        public static KnownMap create(boolean inclusiveEnds, RoutingKey[] ends, Known[] values)
        {
            return new KnownMap(inclusiveEnds, ends, values);
        }
    }

    private transient final Known validForAll;

    private KnownMap()
    {
        this.validForAll = Known.Nothing;
    }

    public KnownMap(boolean inclusiveEnds, RoutingKey[] starts, Known[] values)
    {
        this(inclusiveEnds, starts, values, Known.Nothing);
    }

    private KnownMap(boolean inclusiveEnds, RoutingKey[] starts, Known[] values, Known validForAll)
    {
        super(inclusiveEnds, starts, values);
        this.validForAll = validForAll;
    }

    public static KnownMap create(Unseekables<?> keysOrRanges, SaveStatus saveStatus)
    {
        if (keysOrRanges.isEmpty())
            return new KnownMap();

        return create(keysOrRanges, saveStatus.known, Builder::new);
    }

    public static KnownMap create(Unseekables<?> keysOrRanges, Known known)
    {
        if (keysOrRanges.isEmpty())
            return new KnownMap();

        return create(keysOrRanges, known, Builder::new);
    }

    public static KnownMap merge(KnownMap a, KnownMap b)
    {
        return ReducingRangeMap.merge(a, b, Known::atLeast, Builder::new);
    }

    public Known computeValidForAll(Unseekables<?> routeOrParticipants)
    {
        Known validForAll = foldlWithDefault(routeOrParticipants, KnownMap::reduceKnownFor, Known.Nothing, null, i -> false);
        return this.validForAll.atLeast(validForAll).validForAll();
    }

    public KnownMap with(Known validForAll)
    {
        if (validForAll.equals(this.validForAll))
            return this;

        int i = 0;
        for (; i < size(); ++i)
        {
            Known pre = values[i];
            if (pre == null)
                continue;

            Known post = pre.atLeast(validForAll);
            if (!pre.equals(post))
                break;
        }

        if (i == size())
            return new KnownMap(inclusiveEnds(), starts, values, validForAll);

        RoutingKey[] newStarts = new RoutingKey[size() + 1];
        Known[] newValues = new Known[size()];
        System.arraycopy(starts, 0, newStarts, 0, i);
        System.arraycopy(values, 0, newValues, 0, i);
        int count = i;
        while (i < size())
        {
            Known pre = values[i++];
            Known post = pre == null ? null : pre.atLeast(validForAll);
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
        return new KnownMap(inclusiveEnds(), newStarts, newValues, validForAll);
    }

    public boolean hasTruncated(Routables<?> routables)
    {
        return foldlWithDefault(routables, (known, prev) -> known.isTruncated(), Known.Nothing, false, i -> i);
    }

    public boolean hasTruncated()
    {
        return foldl((known, prev) -> known.isTruncated(), false, i -> i);
    }

    public Known knownFor(Routables<?> owns)
    {
        if (owns.isEmpty())
            return knownForAny();
        return validForAll.atLeast(foldlWithDefault(owns, KnownMap::reduceKnownFor, Known.Nothing, null, i -> false));
    }

    public Known knownForAny()
    {
        return validForAll.atLeast(foldl(Known::atLeast, Known.Nothing, i -> false));
    }

    public Ranges matchingRanges(Predicate<Known> match)
    {
        return foldlWithBounds((known, ranges, start, end) -> match.test(known) ? ranges.with(Ranges.of(start.rangeFactory().newRange(start, end))) : ranges, Ranges.EMPTY, i -> false);
    }

    private static Known reduceKnownFor(Known foundKnown, @Nullable Known prev)
    {
        if (prev == null)
            return foundKnown;

        return prev.reduce(foundKnown);
    }

    public Ranges knownForRanges(Known required, Ranges expect)
    {
        // TODO (desired): implement and use foldlWithDefaultAndBounds so can subtract rather than add
        return foldlWithBounds(expect, (known, prev, start, end) -> {
            if (!required.isSatisfiedBy(known))
                return prev;

            return prev.with(Ranges.of(start.rangeFactory().newRange(start, end)));
        }, Ranges.EMPTY, i -> false);
    }

    public static class Builder extends AbstractBoundariesBuilder<RoutingKey, Known, KnownMap>
    {
        public Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected KnownMap buildInternal()
        {
            return new KnownMap(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new Known[0]));
        }
    }
}

