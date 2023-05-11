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

import accord.utils.Invariants;

import accord.api.RoutingKey;
import accord.utils.SortedArrays;

/**
 * A slice of a Route that covers
 */
public class PartialKeyRoute extends KeyRoute implements PartialRoute<RoutingKey>
{
    public static class SerializationSupport
    {
        public static PartialKeyRoute create(Ranges covering, RoutingKey homeKey, boolean isParticipatingHomeKey, RoutingKey[] keys)
        {
            return new PartialKeyRoute(covering, homeKey, isParticipatingHomeKey, keys);
        }
    }

    public final Ranges covering;

    public PartialKeyRoute(Ranges covering, RoutingKey homeKey, boolean isParticipatingHomeKey, RoutingKey[] keys)
    {
        super(homeKey, isParticipatingHomeKey, keys);
        this.covering = covering;
    }

    @Override
    public PartialKeyRoute sliceStrict(Ranges newRanges)
    {
        if (!covering.containsAll(newRanges))
            throw new IllegalArgumentException("Not covered");

        return slice(newRanges);
    }

    @Override
    public PartialKeyRoute slice(Ranges newRanges)
    {
        RoutingKey[] keys = slice(newRanges, RoutingKey[]::new);
        return new PartialKeyRoute(newRanges, homeKey, isParticipatingHomeKey, keys);
    }

    @Override
    public Route<RoutingKey> union(Route<RoutingKey> that)
    {
        if (that.kind().isFullRoute()) return that;
        return union((PartialKeyRoute) that);
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.PartialKeyRoute;
    }

    @Override
    public boolean covers(Ranges ranges)
    {
        return covering.containsAll(ranges);
    }

    @Override
    public AbstractUnseekableKeys<?> with(RoutingKey withKey)
    {
        if (withKey.equals(homeKey))
            return withHomeKey();

        if (contains(withKey))
            return this;

        return new RoutingKeys(toRoutingKeysArray(withKey));
    }

    @Override
    public PartialKeyRoute withHomeKey()
    {
        if (contains(homeKey))
            return this;
        return new PartialKeyRoute(covering.with(Ranges.of(homeKey.asRange())), homeKey, isParticipatingHomeKey, toRoutingKeysArray(homeKey));
    }

    @Override
    public PartialKeyRoute slice(Ranges newRanges, Slice slice)
    {
        if (newRanges.containsAll(covering))
            return this;

        RoutingKey[] keys = slice(newRanges, RoutingKey[]::new);
        return new PartialKeyRoute(newRanges, homeKey, isParticipatingHomeKey, keys);
    }

    @Override
    public Ranges covering()
    {
        return covering;
    }

    @Override
    public PartialKeyRoute union(PartialRoute<RoutingKey> with)
    {
        if (!(with instanceof PartialKeyRoute))
            throw new IllegalArgumentException();

        PartialKeyRoute that = (PartialKeyRoute) with;
        Invariants.checkState(homeKey.equals(that.homeKey));
        Invariants.checkState(isParticipatingHomeKey == that.isParticipatingHomeKey);
        RoutingKey[] keys = SortedArrays.linearUnion(this.keys, that.keys, RoutingKey[]::new);
        Ranges covering = this.covering.with(that.covering);
        if (covering == this.covering && keys == this.keys)
            return this;
        if (covering == that.covering && keys == that.keys)
            return that;
        return new PartialKeyRoute(covering, homeKey, isParticipatingHomeKey, keys);
    }
}
