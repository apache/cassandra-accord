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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import java.util.*;
import java.util.function.IntFunction;

// TODO (low priority): remove when jdk8 support is dropped
public class Utils
{
    // reimplements Collection#toArray
    public static <T> T[] toArray(Collection<T> src, IntFunction<T[]> factory)
    {
        T[] dst = factory.apply(src.size());
        int i = 0;
        for (T item : src)
            dst[i++] = item;
        return dst;
    }

    // reimplements Collection#toArray
    public static <T> T[] toArray(List<T> src, IntFunction<T[]> factory)
    {
        T[] dst = factory.apply(src.size());
        for (int i=0; i<dst.length; i++)
            dst[i] = src.get(i);
        return dst;
    }

    // reimplements List#of
    public static <T> List<T> listOf(T... src)
    {
        List<T> dst = new ArrayList<>(src.length);
        for (int i=0; i<src.length; i++)
            dst.add(src[i]);
        return dst;
    }

    public static <T> ImmutableSortedSet<T> ensureSortedImmutable(NavigableSet<T> set)
    {
        if (set == null || set.isEmpty())
            return ImmutableSortedSet.of();
        return set instanceof ImmutableSortedSet ? (ImmutableSortedSet<T>) set : ImmutableSortedSet.copyOf(set);
    }

    public static <K, V> ImmutableSortedMap<K, V> ensureSortedImmutable(NavigableMap<K, V> map)
    {
        if (map == null || map.isEmpty())
            return ImmutableSortedMap.of();
        return map instanceof ImmutableSortedMap ? (ImmutableSortedMap<K, V>) map : ImmutableSortedMap.copyOf(map);
    }

    public static <T> ImmutableSet<T> ensureImmutable(java.util.Set<T> set)
    {
        if (set == null || set.isEmpty())
            return ImmutableSet.of();
        return set instanceof ImmutableSet ? (ImmutableSet<T>) set : ImmutableSet.copyOf(set);
    }

    public static <T extends Comparable<? super T>> NavigableSet<T> ensureSortedMutable(NavigableSet<T> set)
    {
        if (set == null)
            return new TreeSet<>();
        return set instanceof ImmutableSortedSet ? new TreeSet<>(set) : set;
    }

    public static <K extends Comparable<K>, V> NavigableMap<K, V> ensureSortedMutable(NavigableMap<K, V> map)
    {
        if (map == null)
            return new TreeMap<>();
        return map instanceof ImmutableSortedMap ? new TreeMap<>(map) : map;
    }

    public static <T> Set<T> ensureMutable(Set<T> set)
    {
        if (set == null)
            return new HashSet<>();
        return set instanceof ImmutableSet ? new HashSet<>(set) : set;
    }
}
