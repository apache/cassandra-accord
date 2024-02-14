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

package accord.local;

import javax.annotation.Nonnull;

import accord.api.RoutingKey;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;

// TODO (expected): track read/write conflicts separately
public class MaxConflicts extends ReducingRangeMap<Timestamp>
{
    public static final MaxConflicts EMPTY = new MaxConflicts();

    private MaxConflicts()
    {
        super();
    }

    private MaxConflicts(boolean inclusiveEnds, RoutingKey[] starts, Timestamp[] values)
    {
        super(inclusiveEnds, starts, values);
    }

    public Timestamp get(Seekables<?, ?> keysOrRanges)
    {
        return foldl(keysOrRanges, Timestamp::max, Timestamp.NONE);
    }

    public Timestamp get(Routables<?> keysOrRanges)
    {
        return foldl(keysOrRanges, Timestamp::max, Timestamp.NONE);
    }

    MaxConflicts update(Seekables<?, ?> keysOrRanges, Timestamp maxConflict)
    {
        return merge(this, create(keysOrRanges, maxConflict));
    }

    public static MaxConflicts create(Ranges ranges, @Nonnull Timestamp maxConflict)
    {
        if (ranges.isEmpty())
            return MaxConflicts.EMPTY;

        return create(ranges, maxConflict, MaxConflicts.Builder::new);
    }

    public static MaxConflicts create(Seekables<?, ?> keysOrRanges, @Nonnull Timestamp maxConflict)
    {
        if (keysOrRanges.isEmpty())
            return MaxConflicts.EMPTY;

        return create(keysOrRanges, maxConflict, Builder::new);
    }

    public static MaxConflicts merge(MaxConflicts a, MaxConflicts b)
    {
        return ReducingIntervalMap.merge(a, b, Timestamp::max, MaxConflicts.Builder::new);
    }

    static class Builder extends AbstractBoundariesBuilder<RoutingKey, Timestamp, MaxConflicts>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected MaxConflicts buildInternal()
        {
            return new MaxConflicts(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new Timestamp[0]));
        }
    }
}
