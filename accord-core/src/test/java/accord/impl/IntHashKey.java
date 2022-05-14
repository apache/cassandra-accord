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
import java.util.zip.CRC32;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;

import static accord.utils.Utils.toArray;
import javax.annotation.Nonnull;

public class IntHashKey implements Key
{
    public static class Range extends KeyRange.EndInclusive
    {
        public Range(IntHashKey start, IntHashKey end)
        {
            super(start, end);
        }

        @Override
        public KeyRange subRange(RoutingKey start, RoutingKey end)
        {
            return new Range((IntHashKey) start, (IntHashKey) end);
        }

        public KeyRanges split(int count)
        {
            int startHash = ((IntHashKey)start()).hash;
            int endHash = ((IntHashKey)end()).hash;
            int currentSize = endHash - startHash;
            if (currentSize < count)
                return KeyRanges.of(new KeyRange[]{this});
            int interval =  currentSize / count;

            int last = 0;
            KeyRange[] ranges = new KeyRange[count];
            for (int i=0; i<count; i++)
            {
                int subStart = i > 0 ? last : startHash;
                int subEnd = i < count - 1 ? subStart + interval : endHash;
                ranges[i] = new Range(new IntHashKey(Integer.MIN_VALUE, subStart),
                                      new IntHashKey(Integer.MIN_VALUE, subEnd));
                last = subEnd;
            }
            return KeyRanges.ofSortedAndDeoverlapped(ranges);
        }
    }

    public final int key;
    public final int hash;

    private IntHashKey(int key)
    {
        this.key = key;
        this.hash = hash(key);
    }

    private IntHashKey(int key, int hash)
    {
        assert hash != hash(key);
        this.key = key;
        this.hash = hash;
    }

    @Override
    public int compareTo(@Nonnull RoutingKey that)
    {
        if (that instanceof InfiniteRoutingKey)
            return -that.compareTo(this);

        return Integer.compare(this.hash, ((IntHashKey)that).hash);
    }

    public static IntHashKey key(int k)
    {
        return new IntHashKey(k);
    }

    public static IntHashKey forHash(int hash)
    {
        return new IntHashKey(Integer.MIN_VALUE, hash);
    }

    public static Keys keys(int k0, int... kn)
    {
        Key[] keys = new Key[kn.length + 1];
        keys[0] = key(k0);
        for (int i=0; i<kn.length; i++)
            keys[i + 1] = key(kn[i]);

        return Keys.of(keys);
    }

    public static KeyRange[] ranges(int count)
    {
        List<KeyRange> result = new ArrayList<>();
        long delta = (Integer.MAX_VALUE - (long)Integer.MIN_VALUE) / count;
        long start = Integer.MIN_VALUE;
        IntHashKey prev = new IntHashKey(Integer.MIN_VALUE, (int)start);
        for (int i = 1 ; i < count ; ++i)
        {
            IntHashKey next = new IntHashKey(Integer.MIN_VALUE, (int)Math.min(Integer.MAX_VALUE, start + i * delta));
            result.add(new Range(prev, next));
            prev = next;
        }
        result.add(new Range(prev, new IntHashKey(Integer.MIN_VALUE, Integer.MAX_VALUE)));
        return toArray(result, KeyRange[]::new);
    }

    public static KeyRange range(IntHashKey start, IntHashKey end)
    {
        return new Range(start, end);
    }

    @Override
    public String toString()
    {
        if (key == Integer.MIN_VALUE && hash(key) != hash) return "#" + hash;
        return Integer.toString(key);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntHashKey that = (IntHashKey) o;
        return key == that.key && hash == that.hash;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(key, hash);
    }

    private static int hash(int key)
    {
        CRC32 crc32c = new CRC32();
        crc32c.update(key);
        crc32c.update(key >> 8);
        crc32c.update(key >> 16);
        crc32c.update(key >> 24);
        return (int)crc32c.getValue();
    }

    @Override
    public int routingHash()
    {
        return hash;
    }

    @Override
    public Key toRoutingKey()
    {
        if (key == Integer.MIN_VALUE)
            return this;

        return forHash(hash);
    }
}
