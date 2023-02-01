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
import accord.utils.Invariants;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface RoutableKey extends Routable, Comparable<RoutableKey>
{
    /**
     * A special RoutingKey that sorts before or after everything, so that exclusive bounds may still cover
     * the full range of possible RoutingKey.
     *
     * All RoutingKey implementations must sort correctly with this type.
     *
     * TODO (expected, testing): need to partition range from/to -/+ infinity as otherwise we exclude at least one key
     */
    class InfiniteRoutableKey implements RoutableKey
    {
        public static final InfiniteRoutableKey POSITIVE_INFINITY = new InfiniteRoutableKey(1);
        public static final InfiniteRoutableKey NEGATIVE_INFINITY = new InfiniteRoutableKey(-1);

        final int compareTo;

        public InfiniteRoutableKey(int compareTo)
        {
            this.compareTo = compareTo;
        }

        @Override
        public int compareTo(@Nonnull RoutableKey ignore)
        {
            return compareTo;
        }

        @Override
        public RoutingKey toUnseekable() { throw new UnsupportedOperationException(); }

        @Override
        public RoutingKey someIntersectingRoutingKey(@Nullable Ranges ranges) { throw new UnsupportedOperationException(); }
    }

    /**
     * Implementations must be comparable with {@link InfiniteRoutableKey}
     * @param that the object to be compared.
     * @return
     */
    @Override
    int compareTo(@Nonnull RoutableKey that);

    @Override
    default Domain domain() { return Domain.Key; }

    @Override
    RoutingKey toUnseekable();

    @Override default RoutingKey someIntersectingRoutingKey(@Nullable Ranges ranges)
    {
        Invariants.paranoid(ranges == null || ranges.contains(this));
        return toUnseekable();
    }
}
