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

import accord.api.Key;
import accord.impl.IntKey;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.assertj.core.api.Assertions;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

class AbstractRangesTest
{
    @Test
    public void testToString()
    {
        Ranges ranges = new Ranges(
                range("first", 0, 10),
                range("first", 10, 20),
                range("second", 20, 30),
                range("third", 30, 40)
        );
        assertThat(ranges.toString()).isEqualTo("[first:[[0,10), [10,20)], second:[[20,30)], third:[[30,40)]]");
    }

    @Test
    public void testContainsAll()
    {
        Gen<accord.primitives.Ranges> rangesGen = AccordGens.ranges(Gens.ints().between(1, 10), AccordGens.intRoutingKey(), IntKey::range);
        qt().forAll(Gens.random(), rangesGen).check((rs, ranges) -> {
            Keys inside = intKeyInside(rs, ranges);
            Assertions.assertThat(ranges.containsAll(inside)).isTrue();
            Assertions.assertThat(inside.slice(ranges, Routables.Slice.Minimal)).isEqualTo(inside);
            Keys outside = intKeyOutside(rs, ranges);
            if (outside != null)
            {
                Assertions.assertThat(ranges.containsAll(outside)).isFalse();
                Assertions.assertThat(outside.slice(ranges, Routables.Slice.Minimal)).isNotEqualTo(outside);
            }
        });
    }

    private Keys intKeyInside(RandomSource rs, accord.primitives.Ranges ranges)
    {
        return Gens.lists(AccordGens.intKeysInsideRanges(ranges))
               .unique()
               .ofSizeBetween(1, 10)
                   .map(l -> Keys.ofUnique(l.toArray(Key[]::new)))
                   .next(rs);
    }

    private Keys intKeyOutside(RandomSource rs, accord.primitives.Ranges ranges)
    {
        return Gens.lists(AccordGens.intKeysOutsideRanges(ranges))
                   .unique()
                   .ofSizeBetween(1, 10)
                   .map(l -> Keys.ofUnique(l.toArray(Key[]::new)))
                   .next(rs);
    }

    private static Range range(Object prefix, int start, int end)
    {
        return Range.range(new PrefixKey(start, prefix), new PrefixKey(end, prefix), true , false);
    }

    private static class Ranges extends AbstractRanges
    {
        Ranges(@Nonnull Range... ranges)
        {
            super(ranges);
        }

        @Override
        public Routables<?> slice(int from, int to)
        {
            return new Ranges(Arrays.copyOfRange(ranges, from, to));
        }

        @Override
        public Routables<?> slice(accord.primitives.Ranges ranges)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Routables<Range> slice(accord.primitives.Ranges ranges, Slice slice)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Routables<?> intersecting(Unseekables<?> intersecting)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Routables<Range> intersecting(Unseekables<?> intersecting, Slice slice)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class PrefixKey extends IntKey.Routing
    {
        private final Object prefix;

        public PrefixKey(int key, Object prefix)
        {
            super(key);
            this.prefix = prefix;
        }

        @Override
        public Object prefix()
        {
            return prefix;
        }
    }
}