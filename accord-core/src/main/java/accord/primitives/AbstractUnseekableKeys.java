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
import accord.utils.ArrayBuffers;
import accord.utils.SortedArrays;

import java.util.Arrays;

// TODO: do we need this class?
public abstract class AbstractUnseekableKeys extends AbstractKeys<RoutingKey>
implements Iterable<RoutingKey>, Unseekables<RoutingKey>, Participants<RoutingKey>
{
    AbstractUnseekableKeys(RoutingKey[] keys)
    {
        super(keys);
    }

    @Override
    public final int indexOf(RoutingKey key)
    {
        return Arrays.binarySearch(keys, key);
    }

    @Override
    public final AbstractUnseekableKeys intersect(Unseekables<?> keysOrRanges)
    {
        switch (keysOrRanges.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + keysOrRanges.domain());
            case Key:
            {
                AbstractUnseekableKeys that = (AbstractUnseekableKeys) keysOrRanges;
                return weakWrap(intersect(that, ArrayBuffers.cachedRoutingKeys()), that);
            }
            case Range:
            {
                AbstractRanges that = (AbstractRanges) keysOrRanges;
                return wrap(intersect(that, ArrayBuffers.cachedRoutingKeys()));
            }
        }
    }

    @Override
    public final Participants<RoutingKey> subtract(Unseekables<?> keysOrRanges)
    {
        switch (keysOrRanges.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + keysOrRanges.domain());
            case Key:
            {
                AbstractUnseekableKeys that = (AbstractUnseekableKeys) keysOrRanges;
                return weakWrap(SortedArrays.linearSubtract(this.keys, that.keys, RoutingKey[]::new), that);
            }
            case Range:
            {
                return subtract((AbstractRanges)keysOrRanges);
            }
        }
    }

    @Override
    public Participants<RoutingKey> subtract(Ranges ranges)
    {
        return subtract((AbstractRanges) ranges);
    }

    private Participants<RoutingKey> subtract(AbstractRanges ranges)
    {
        RoutingKey[] output = subtract(ranges, RoutingKey[]::new);
        return output == keys ? this : new RoutingKeys(output);
    }

    public Ranges toRanges()
    {
        Range[] ranges = new Range[keys.length];
        for (int i = 0 ; i < keys.length ; ++i)
            ranges[i] = keys[i].asRange();
        return Ranges.ofSortedAndDeoverlapped(ranges);
    }

    private AbstractUnseekableKeys weakWrap(RoutingKey[] wrap, AbstractUnseekableKeys that)
    {
        return wrap == keys ? this : wrap == that.keys ? that : new RoutingKeys(wrap);
    }

    private AbstractUnseekableKeys wrap(RoutingKey[] wrap)
    {
        return wrap == keys ? this : new RoutingKeys(wrap);
    }
}
