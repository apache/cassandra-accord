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

package accord.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.primitives.KeyRange;
import accord.primitives.Keys;
import accord.primitives.PartialRoute;
import accord.primitives.RoutingKeys;

import javax.annotation.Nonnull;

import static accord.utils.Utils.toArray;

public class IntKey implements Key
{
    public static class Range extends KeyRange.EndInclusive
    {
        public Range(IntKey start, IntKey end)
        {
            super(start, end);
        }

        @Override
        public KeyRange subRange(RoutingKey start, RoutingKey end)
        {
            return new Range((IntKey)start, (IntKey)end);
        }
    }

    public final int key;

    public IntKey(int key)
    {
        this.key = key;
    }

    @Override
    public int compareTo(@Nonnull RoutingKey that)
    {
        if (that instanceof InfiniteRoutingKey)
            return -that.compareTo(this);
        return Integer.compare(this.key, ((IntKey)that).key);
    }

    public static IntKey key(int k)
    {
        return new IntKey(k);
    }

    public static Keys keys(int k0, int... kn)
    {
        Key[] keys = new Key[kn.length + 1];
        keys[0] = new IntKey(k0);
        for (int i=0; i<kn.length; i++)
            keys[i + 1] = new IntKey(kn[i]);

        return Keys.of(keys);
    }

    public static RoutingKeys scope(int k0, int... kn)
    {
        return keys(k0, kn).toRoutingKeys();
    }

    public static Keys keys(int[] keyArray)
    {
        Key[] keys = new Key[keyArray.length];
        for (int i=0; i<keyArray.length; i++)
            keys[i] = new IntKey(keyArray[i]);

        return Keys.of(keys);
    }

    public static KeyRange range(IntKey start, IntKey end)
    {
        return new Range(start, end);
    }

    public static KeyRange range(int start, int end)
    {
        return range(key(start), key(end));
    }

    public static KeyRange[] ranges(int count)
    {
        List<KeyRange> result = new ArrayList<>();
        long delta = (Integer.MAX_VALUE - (long)Integer.MIN_VALUE) / count;
        long start = Integer.MIN_VALUE;
        IntKey prev = new IntKey((int)start);
        for (int i = 1 ; i < count ; ++i)
        {
            IntKey next = new IntKey((int)Math.min(Integer.MAX_VALUE, start + i * delta));
            result.add(new Range(prev, next));
            prev = next;
        }
        result.add(new Range(prev, new IntKey(Integer.MAX_VALUE)));
        return toArray(result, KeyRange[]::new);
    }

    @Override
    public String toString()
    {
        return Integer.toString(key);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntKey intKey = (IntKey) o;
        return key == intKey.key;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(key);
    }

    @Override
    public int routingHash()
    {
        return hashCode();
    }

    @Override
    public RoutingKey toRoutingKey()
    {
        return this;
    }
}
