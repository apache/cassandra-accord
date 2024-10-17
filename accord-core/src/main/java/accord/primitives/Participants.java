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

/**
 * A marker interface for a collection of Unseekables that make up some portion of a Route but without necessarily having a homeKey.
 */
public interface Participants<K extends Unseekable> extends Unseekables<K>
{
    @Override Participants<K> intersecting(Seekables<?, ?> intersecting);
    @Override Participants<K> intersecting(Seekables<?, ?> intersecting, Slice slice);
    @Override Participants<K> intersecting(Unseekables<?> intersecting);
    @Override Participants<K> intersecting(Unseekables<?> intersecting, Slice slice);

    @Override Participants<K> slice(int from, int to);
    @Override Participants<K> slice(Ranges ranges);
    @Override Participants<K> slice(Ranges ranges, Slice slice);

    Participants<K> with(Participants<K> with);
    @Override Participants<K> without(Ranges ranges);
    @Override Participants<K> without(Unseekables<?> without);

    Ranges toRanges();

    /**
     * If both left and right are a Route, invoke {@link Route#with} on them. Otherwise invoke {@link #with}.
     */
    static <K extends Unseekable> Participants<K> merge(Participants<K> left, Participants<K> right)
    {
        if (left == right) return left;
        if (left == null) return right;
        if (right == null) return left;
        return left.with(right);
    }

    static Participants<?> empty(Routable.Domain domain)
    {
        return domain == Routable.Domain.Range ? Ranges.EMPTY : RoutingKeys.EMPTY;
    }

    static Participants<?> singleton(Routable.Domain domain, RoutingKey key)
    {
        return domain == Routable.Domain.Range ? Ranges.of(key.asRange()) : RoutingKeys.of(key);
    }
}
