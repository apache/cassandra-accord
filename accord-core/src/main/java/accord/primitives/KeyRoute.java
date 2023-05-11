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

import javax.annotation.Nonnull;

import static accord.utils.ArrayBuffers.cachedRoutingKeys;

public abstract class KeyRoute extends AbstractUnseekableKeys<Route<RoutingKey>> implements Route<RoutingKey>
{
    public final RoutingKey homeKey;
    public final boolean isParticipatingHomeKey;

    KeyRoute(@Nonnull RoutingKey homeKey, boolean isParticipatingHomeKey, RoutingKey[] keys)
    {
        super(keys);
        this.homeKey = Invariants.nonNull(homeKey);
        this.isParticipatingHomeKey = isParticipatingHomeKey;
    }

    @Override
    public boolean participatesIn(Ranges ranges)
    {
        if (isParticipatingHomeKey())
            return intersects(ranges);

        long ij = findNextIntersection(0, ranges, 0);
        if (ij < 0)
            return false;

        int i = (int)ij;
        if (!get(i).equals(homeKey))
            return true;

        int j = (int)(ij >>> 32);
        return findNextIntersection(i + 1, ranges, j) >= 0;
    }

    @Override
    public Unseekables<RoutingKey, ?> with(Unseekables<RoutingKey, ?> with)
    {
        AbstractKeys<RoutingKey, ?> that = (AbstractKeys<RoutingKey, ?>) with;
        return wrap(SortedArrays.linearUnion(keys, that.keys, cachedRoutingKeys()), that);
    }

    @Override
    public Unseekables<RoutingKey, ?> participants()
    {
        if (isParticipatingHomeKey)
            return this;

        int removePos = Arrays.binarySearch(keys, homeKey);
        if (removePos < 0)
            return this;

        RoutingKey[] result = new RoutingKey[keys.length - 1];
        System.arraycopy(keys, 0, result, 0, removePos);
        System.arraycopy(keys, removePos + 1, result, removePos, keys.length - (1 + removePos));
        return new RoutingKeys(result);
    }

    @Override
    public RoutingKey homeKey()
    {
        return homeKey;
    }

    @Override
    public boolean isParticipatingHomeKey()
    {
        return isParticipatingHomeKey;
    }

    @Override
    public abstract PartialKeyRoute slice(Ranges ranges);

    @Override
    public Unseekables<RoutingKey, ?> slice(Ranges ranges, Slice slice)
    {
        return slice(ranges);
    }

    private AbstractUnseekableKeys<?> wrap(RoutingKey[] wrap, AbstractKeys<RoutingKey, ?> that)
    {
        return wrap == keys ? this : wrap == that.keys && that instanceof AbstractUnseekableKeys<?>
                ? (AbstractUnseekableKeys<?>) that
                : new RoutingKeys(wrap);
    }
}
