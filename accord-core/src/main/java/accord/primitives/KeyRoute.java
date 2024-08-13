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

import javax.annotation.Nonnull;

import static accord.utils.ArrayBuffers.cachedRoutingKeys;

public abstract class KeyRoute extends AbstractUnseekableKeys implements Route<RoutingKey>
{
    public final RoutingKey homeKey;

    KeyRoute(@Nonnull RoutingKey homeKey, RoutingKey[] keys)
    {
        super(keys);
        this.homeKey = Invariants.nonNull(homeKey);
    }

    @Override
    public boolean participatesIn(Ranges ranges)
    {
        return intersects(ranges);
    }

    @SuppressWarnings("unchecked")
    @Override
    public KeyRoute with(Unseekables<RoutingKey> with)
    {
        AbstractKeys<RoutingKey> that = (AbstractKeys<RoutingKey>) with;
        RoutingKey[] mergedKeys = SortedArrays.linearUnion(this.keys, that.keys, cachedRoutingKeys());
        if (mergedKeys == this.keys)
            return this;
        Invariants.checkState(getClass() == PartialKeyRoute.class);
        if (mergedKeys == that.keys && that instanceof KeyRoute)
            return (KeyRoute) that;
        return new PartialKeyRoute(homeKey, mergedKeys);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Participants<RoutingKey> participants()
    {
        return this;
    }

    @Override
    public Participants<RoutingKey> participants(Ranges ranges)
    {
        RoutingKey[] keys = slice(ranges, RoutingKey[]::new);
        return keys == this.keys ? this : new RoutingKeys(keys);
    }

    @Override
    public Participants<RoutingKey> participants(Ranges ranges, Slice slice)
    {
        return participants(ranges);
    }

    @Override
    public RoutingKey homeKey()
    {
        return homeKey;
    }

    @Override
    public Route<RoutingKey> homeKeyOnlyRoute()
    {
        if (keys.length == 1 && keys[0].equals(homeKey))
            return this;
        return new PartialKeyRoute(homeKey, new RoutingKey[] { homeKey });
    }

    @Override
    public boolean isHomeKeyOnlyRoute()
    {
        return keys.length == 1 && keys[0].equals(homeKey);
    }

    @Override
    public PartialKeyRoute slice(Ranges select)
    {
        return new PartialKeyRoute(homeKey, slice(select, RoutingKey[]::new));
    }

    @Override
    public PartialKeyRoute slice(Ranges ranges, Slice slice)
    {
        return slice(ranges);
    }

    @Override
    public PartialKeyRoute intersecting(Unseekables<?> intersecting)
    {
        switch (intersecting.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + intersecting.domain());
            case Key: return new PartialKeyRoute(homeKey, intersecting((AbstractUnseekableKeys)intersecting, cachedRoutingKeys()));
            case Range: return new PartialKeyRoute(homeKey, slice((AbstractRanges)intersecting, RoutingKey[]::new));
        }
    }

    @Override
    public PartialKeyRoute intersecting(Unseekables<?> intersecting, Slice slice)
    {
        return intersecting(intersecting);
    }

    private AbstractUnseekableKeys wrap(RoutingKey[] wrap, AbstractKeys<RoutingKey> that)
    {
        return wrap == keys ? this : wrap == that.keys && that instanceof AbstractUnseekableKeys
                ? (AbstractUnseekableKeys) that
                : new RoutingKeys(wrap);
    }
}
