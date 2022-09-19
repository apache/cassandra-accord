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

import accord.api.RoutingKey;
import accord.primitives.RoutableKey;
import accord.primitives.Keys;
import accord.primitives.RoutingKeys;

import javax.annotation.Nonnull;

import static accord.utils.Utils.toArray;

public class IntKey implements RoutableKey
{
    public static class Raw extends IntKey implements accord.api.Key
    {
        public Raw(int key)
        {
            super(key);
        }
    }

    public static class Routing extends IntKey implements accord.api.RoutingKey
    {
        public Routing(int key)
        {
            super(key);
        }
    }

    public static class Range extends accord.primitives.Range.EndInclusive
    {
        public Range(Routing start, Routing end)
        {
            super(start, end);
        }

        @Override
        public accord.primitives.Range subRange(RoutingKey start, RoutingKey end)
        {
            return new Range((Routing)start, (Routing)end);
        }
    }

    public final int key;

    public IntKey(int key)
    {
        this.key = key;
    }

    @Override
    public int compareTo(@Nonnull RoutableKey that)
    {
        return Integer.compare(this.key, ((IntKey)that).key);
    }

    public static Raw key(int k)
    {
        return new Raw(k);
    }

    public static Routing routing(int k)
    {
        return new Routing(k);
    }

    public static Keys keys(int k0, int... kn)
    {
        Raw[] keys = new Raw[kn.length + 1];
        keys[0] = new Raw(k0);
        for (int i=0; i<kn.length; i++)
            keys[i + 1] = new Raw(kn[i]);

        return Keys.of(keys);
    }

    public static RoutingKeys scope(int k0, int... kn)
    {
        return keys(k0, kn).toUnseekables();
    }

    public static Keys keys(int[] keyArray)
    {
        Raw[] keys = new Raw[keyArray.length];
        for (int i=0; i<keyArray.length; i++)
            keys[i] = new Raw(keyArray[i]);

        return Keys.of(keys);
    }

    public static accord.primitives.Range range(Routing start, Routing end)
    {
        return new Range(start, end);
    }

    public static accord.primitives.Range range(int start, int end)
    {
        return range(routing(start), routing(end));
    }

    public static accord.primitives.Range[] ranges(int count)
    {
        List<accord.primitives.Range> result = new ArrayList<>();
        long delta = (Integer.MAX_VALUE - (long)Integer.MIN_VALUE) / count;
        long start = Integer.MIN_VALUE;
        Routing prev = new Routing((int)start);
        for (int i = 1 ; i < count ; ++i)
        {
            Routing next = new Routing((int)Math.min(Integer.MAX_VALUE, start + i * delta));
            result.add(new Range(prev, next));
            prev = next;
        }
        result.add(new Range(prev, new Routing(Integer.MAX_VALUE)));
        return toArray(result, accord.primitives.Range[]::new);
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
    public RoutingKey toUnseekable()
    {
        if (this instanceof IntKey.Routing)
            return (Routing)this;
        return new Routing(key);
    }
}
