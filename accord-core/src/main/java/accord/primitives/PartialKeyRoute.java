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
        public static PartialKeyRoute create(RoutingKey homeKey, RoutingKey[] keys)
        {
            return new PartialKeyRoute(homeKey, keys);
        }
    }


    public PartialKeyRoute(RoutingKey homeKey, RoutingKey[] keys)
    {
        super(homeKey, keys);
    }

    @Override
    public PartialKeyRoute slice(Ranges select)
    {
        RoutingKey[] keys = slice(select, RoutingKey[]::new);
        if (keys == this.keys)
            return this;
        return new PartialKeyRoute(homeKey, keys);
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
    public AbstractUnseekableKeys with(RoutingKey withKey)
    {
        if (withKey.equals(homeKey))
            return withHomeKey();

        if (contains(withKey))
            return this;

        return new RoutingKeys(toRoutingKeysArray(withKey, true));
    }

    @Override
    public PartialKeyRoute withHomeKey()
    {
        int insertPos = Arrays.binarySearch(keys, homeKey);
        if (insertPos >= 0)
            return this;

        insertPos = -1 - insertPos;
        RoutingKey[] keys = new RoutingKey[1 + this.keys.length];
        System.arraycopy(this.keys, 0, keys, 0, insertPos);
        keys[insertPos] = homeKey;
        System.arraycopy(this.keys, insertPos, keys, insertPos + 1, this.keys.length - insertPos);

        return new PartialKeyRoute(homeKey, keys);
    }

    @Override
    public PartialKeyRoute slice(Ranges select, Slice slice)
    {
        RoutingKey[] keys = slice(select, RoutingKey[]::new);
        if (keys == this.keys)
            return this;

        return new PartialKeyRoute(homeKey, keys);
    }

    @Override
    public PartialKeyRoute union(PartialRoute<RoutingKey> with)
    {
        if (!(with instanceof PartialKeyRoute))
            throw new IllegalArgumentException();

        PartialKeyRoute that = (PartialKeyRoute) with;
        Invariants.checkState(homeKey.equals(that.homeKey));
        RoutingKey[] keys = SortedArrays.linearUnion(this.keys, that.keys, RoutingKey[]::new);
        if (keys == this.keys || keys == that.keys)
        {
            if (keys != that.keys) return this;
            if (keys != this.keys) return that;
            return this;
        }
        return new PartialKeyRoute(homeKey, keys);
    }
}
