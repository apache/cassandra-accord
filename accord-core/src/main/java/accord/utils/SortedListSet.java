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

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.IntFunction;

import accord.utils.SortedArrays.SortedArrayList;

// TODO (expected): make a hashing version of this class; have Node.Id support both approaches
public abstract class SortedListSet<K extends Comparable<? super K>> extends AbstractSet<K>
{
    private static final SmallSortedListSet ALWAYS_EMPTY = new SmallSortedListSet(new SortedArrayList(new Comparable[0]));

    public static class SmallSortedListSet<K extends Comparable<? super K>> extends SortedListSet<K>
    {
        long bits;
        int size;

        protected SmallSortedListSet(SortedList<K> list)
        {
            super(list);
        }

        @Override
        boolean contains(int index)
        {
            long bit = 1L << index;
            return 0 != (bits & bit);
        }

        @Override
        boolean set(int index)
        {
            long bit = 1L << index;
            long newBits = bits | bit;
            if (bits == newBits)
                return false;
            bits = newBits;
            ++size;
            return true;
        }

        @Override
        boolean unset(int index)
        {
            long bit = 1L << index;
            long newBits = bits & ~bit;
            if (bits == newBits)
                return false;
            bits = newBits;
            --size;
            return true;
        }

        @Override
        int nextSet(int index)
        {
            long bits = this.bits >>> index;
            index += Long.numberOfTrailingZeros(bits);
            index |= -(index & ~63);
            return index;
        }

        @Override
        void addAll()
        {
            size = list.size();
            if (size == 0) bits = 0;
            else bits = -1L >>> (64 - size);
        }

        @Override
        public int size()
        {
            return size;
        }
    }

    public static class LargeSortedListSet<K extends Comparable<? super K>> extends SortedListSet<K>
    {
        final LargeBitSet bits;

        protected LargeSortedListSet(SortedList<K> list)
        {
            super(list);
            this.bits = new LargeBitSet(list.size());
        }

        @Override
        boolean contains(int index)
        {
            return bits.get(index);
        }

        @Override
        boolean set(int index)
        {
            return bits.set(index);
        }

        @Override
        void addAll()
        {
            bits.setRange(0, list.size());
        }

        @Override
        boolean unset(int index)
        {
            return bits.unset(index);
        }

        @Override
        int nextSet(int index)
        {
            return bits.nextSetBit(index, -1);
        }

        @Override
        public int size()
        {
            return bits.getSetBitCount();
        }
    }

    public static <K extends Comparable<? super K>> SortedListSet<K> noneOf(SortedList<K> list)
    {
        return empty(list);
    }

    public static <K extends Comparable<? super K>> SortedListSet<K> allOf(SortedList<K> list)
    {
        SortedListSet<K> result = empty(list);
        result.addAll();
        return result;
    }

    public static <K extends Comparable<? super K>> SortedListSet<K> empty(SortedList<K> list)
    {
        return list.size() <= 64 ? new SmallSortedListSet<>(list) : new LargeSortedListSet<>(list);
    }

    public static <K extends Comparable<? super K>> SortedListSet<K> alwaysEmpty()
    {
        return ALWAYS_EMPTY;
    }

    final SortedList<K> list;

    private SortedListSet(SortedList<K> list)
    {
        this.list = list;
    }

    abstract boolean contains(int index);
    abstract boolean set(int index);
    abstract void addAll();
    abstract boolean unset(int index);
    abstract int nextSet(int index);

    @Override
    public boolean contains(Object key)
    {
        int index = list.indexOf((K)key);
        return index >= 0 && contains(index);
    }

    @Override
    public boolean add(K key)
    {
        int i = list.find(key);
        if (i < 0)
            throw new IllegalArgumentException(key + " is not in the SortedList of keys");
        return set(i);
    }

    public boolean addIndex(int index)
    {
        return set(index);
    }

    @Override
    public boolean remove(Object key)
    {
        int i = list.find((K)key);
        return i >= 0 && unset(i);
    }

    @Override
    public boolean addAll(Collection<? extends K> c)
    {
        if (c == list)
        {
            if (size() == list.size())
                return false;
            addAll();
            return true;
        }
        return super.addAll(c);
    }

    public void addAll(SortedList<K> set)
    {
        if (list == set) addAll();
        else
        {
            int i = 0, j = 0;
            while (i < list.size() && j < set.size())
            {
                i = list.findNext(i, set.get(j));
                if (i < 0) i = -1 -i;
                else set(i++);
                j++;
            }
        }
    }

    public SortedArrayList<K> toSortedArrayList(IntFunction<K[]> alloc)
    {
        if (size() == domainSize())
            return SortedArrayList.of(list, alloc);

        K[] out = alloc.apply(size());
        int count = 0;
        for (int i = nextSet(0); i >= 0 ; i = nextSet(i + 1))
            out[count++] = list.get(i);

        return SortedArrayList.ofSorted(out);
    }

    @Override
    public Iterator<K> iterator()
    {
        return new Iter();
    }

    class Iter implements Iterator<K>
    {
        int cur = -1, next = nextSet(0);

        @Override
        public void remove()
        {
            if (cur >= 0)
            {
                unset(cur);
                cur = -1;
            }
        }

        @Override
        public boolean hasNext()
        {
            return next >= 0;
        }

        @Override
        public K next()
        {
            if (!hasNext())
                throw new NoSuchElementException();

            cur = next;
            next = nextSet(next + 1);
            return list.get(cur);
        }
    }

    public SortedList<K> domain()
    {
        return list;
    }

    public int domainSize()
    {
        return list.size();
    }

    public K get(int index)
    {
        return list.get(index);
    }
}
