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

package accord.utils;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Stream;

public class SortedListMap<K extends Comparable<? super K>, V> extends AbstractMap<K, V>
{
    final SortedList<K> list;
    final V[] values;
    int size;

    public SortedListMap(SortedList<K> list, IntFunction<V[]> allocator)
    {
        this.list = list;
        this.values = allocator.apply(list.size());
    }

    @Override
    public V get(Object key)
    {
        int i = list.find((K)key);
        if (i < 0) return null;
        return values[i];
    }

    @Override
    public boolean containsValue(Object value)
    {
        for (V v : values)
        {
            if (v == value) return true;
        }
        return false;
    }

    @Override
    public boolean containsKey(Object key)
    {
        return list.contains((K) key);
    }

    @Override
    public V put(K key, V value)
    {
        Invariants.checkArgument(value != null);
        int i = list.find(key);
        if (i < 0)
            throw new IllegalArgumentException(key + " is not in the SortedList of keys");
        V prev = values[i];
        values[i] = value;
        if (prev == null)
            ++size;
        return prev;
    }

    @Override
    public V remove(Object key)
    {
        int i = list.find((K)key);
        if (i < 0) return null;
        V prev = values[i];
        values[i] = null;
        return prev;
    }

    @Override
    public Set<K> keySet()
    {
        return new SetView<K>()
        {
            @Override
            public boolean contains(Object o)
            {
                return get(o) != null;
            }

            @Override
            public Iterator<K> iterator()
            {
                return new Iter<K>()
                {
                    @Override
                    K get(K key, V value)
                    {
                        return key;
                    }
                };
            }
        };
    }

    @Override
    public Collection<V> values()
    {
        return new CollectionView<V>()
        {
            @Override
            public Iterator<V> iterator()
            {
                return new Iter<V>()
                {
                    @Override
                    V get(K key, V value)
                    {
                        return value;
                    }
                };
            }

            @Override
            public Stream<V> stream()
            {
                return Arrays.stream(values).filter(Objects::nonNull);
            }
        };
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet()
    {
        return new SetView<Entry<K, V>>()
        {
            @Override
            public Iterator<Entry<K, V>> iterator()
            {
                return new Iter<Map.Entry<K, V>>()
                {
                    @Override
                    Map.Entry<K, V> get(K key, V value)
                    {
                        return new AbstractMap.SimpleImmutableEntry<>(key, value);
                    }
                };
            }
        };
    }

    abstract public class SetView<T> extends AbstractSet<T>
    {
        @Override
        public int size()
        {
            return size;
        }
    }

    abstract public class CollectionView<T> extends AbstractCollection<T>
    {
        @Override
        public int size()
        {
            return size;
        }
    }

    abstract class Iter<T> implements Iterator<T>
    {
        int cur = -1, next = -1;

        abstract T get(K key, V value);

        @Override
        public void remove()
        {
            if (values[cur] != null)
            {
                --size;
                values[cur] = null;
            }
        }

        @Override
        public boolean hasNext()
        {
            while (++cur < values.length)
            {
                if (values[cur] != null)
                {
                    next = cur;
                    return true;
                }
            }
            return false;
        }

        @Override
        public T next()
        {
            T result = get(list.get(next), values[next]);
            next = -1;
            return result;
        }
    }

    public List<V> valuesAsNullableList()
    {
        return Arrays.asList(values);
    }

    public Stream<V> valuesAsNullableStream()
    {
        return Stream.of(values);
    }

    public int domainSize()
    {
        return list.size();
    }

    public K getKey(int keyIndex)
    {
        return list.get(keyIndex);
    }

    public V getValue(int keyIndex)
    {
        return values[keyIndex];
    }

    public List<V> select(List<K> select)
    {
        return list.select(values, select);
    }

    public V[] unsafeValuesBackingArray()
    {
        return values;
    }
}
