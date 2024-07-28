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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.IntFunction;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

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

    public static <T> ImmutableSortedSet<T> ensureSortedImmutable(SortedSet<T> set)
    {
        if (set == null || set.isEmpty())
            return ImmutableSortedSet.of();
        return set instanceof ImmutableSortedSet ? (ImmutableSortedSet<T>) set : ImmutableSortedSet.copyOf(set);
    }

    public static <K, V> ImmutableSortedMap<K, V> ensureSortedImmutable(SortedMap<K, V> map)
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

    public static SimpleBitSet ensureMutable(SimpleBitSet set)
    {
        if (set == null) return null;
        return set instanceof ImmutableBitSet ? new SimpleBitSet(set) : set;
    }

    public static ImmutableBitSet ensureImmutable(SimpleBitSet set)
    {
        if (set == null) return null;
        return set instanceof ImmutableBitSet ? (ImmutableBitSet) set : new ImmutableBitSet(set, true);
    }

    public static <T> T[] addAll(T[] first, T[] second)
    {
        T[] array = (T[]) Array.newInstance(first.getClass().getComponentType(), first.length + second.length);
        System.arraycopy(first, 0, array, 0, first.length);
        System.arraycopy(second, 0, array, first.length, second.length);
        return array;
    }

    public static void reverse(int[] array)
    {
        for (int i = 0; i < array.length / 2; i++)
        {
            int tmp = array[i];
            array[i] = array[array.length- 1 - i];
            array[array.length - 1 - i] = tmp;
        }
    }

    public static void shuffle(int[] array, RandomSource rs)
    {
        for (int i = array.length; i > 1; i--)
        {
            int j = i - 1;
            int k = rs.nextInt(j);
            int tmp = array[j];
            array[j] = array[k];
            array[k] = tmp;
        }
    }
}
