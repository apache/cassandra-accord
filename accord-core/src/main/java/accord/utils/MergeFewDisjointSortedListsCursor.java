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

import java.util.Arrays;
import javax.annotation.Nonnull;

public class MergeFewDisjointSortedListsCursor<T extends Comparable<? super T>> implements SortedCursor<T>
{
    // Holds and is comparable by the head item of an iterator it owns
    protected static final class Candidate<T extends Comparable<? super T>> implements Comparable<Candidate<T>>
    {
        private final SortedList<? extends T> list;
        private int itemIdx;
        private T item;

        public Candidate(@Nonnull SortedList<? extends T> list)
        {
            Invariants.checkState(!list.isEmpty());
            this.list = list;
            this.item = list.get(0);
        }

        /** @return this if our iterator had an item, and it is now available, otherwise null */
        private Candidate<T> advance()
        {
            if (++itemIdx >= list.size())
                return null;

            item = list.get(itemIdx);
            return this;
        }

        private boolean find(Comparable<? super T> find)
        {
            boolean found;
            int i = list.findNext(itemIdx, find);
            found = i >= 0;
            if (i < 0) i = -1 - i;
            if ((itemIdx = i) < list.size())
                item = list.get(itemIdx = i);
            else
                item = null;
            return found;
        }

        public int compareTo(Candidate<T> that)
        {
            return this.item.compareTo(that.item);
        }
    }

    protected final Candidate<T>[] heap;

    /** Number of non-exhausted iterators. */
    int size;

    public MergeFewDisjointSortedListsCursor(int capacity)
    {
        this.heap = (Candidate<T>[]) new Candidate[capacity];
    }

    public void add(SortedList<? extends T> list)
    {
        Candidate<T> candidate = new Candidate<>(list);
        heap[size++] = candidate;
    }

    public void init()
    {
        Arrays.sort(heap, 0, size);
    }

    @Override
    public boolean hasCur()
    {
        return size > 0;
    }

    @Override
    public T cur()
    {
        return heap[0].item;
    }

    public void advance()
    {
        Candidate<T> sink = heap[0].advance();
        if (sink == null)
        {
            heap[0] = null;
            if (--size > 0)
                System.arraycopy(heap, 1, heap, 0, size);
        }
        else
        {
            int i = 1;
            for (; i < size && sink.compareTo(heap[i]) > 0; ++i)
                heap[i - 1] = heap[i];
            heap[i - 1] = sink;
        }
    }

    @Override
    public boolean find(Comparable<? super T> find)
    {
        int i = 0;
        int removedCount = 0;
        boolean found = false;
        while (i < size)
        {
            int c = find.compareTo(heap[i].item);
            if (c <= 0)
            {
                found |= c == 0;
                if (removedCount > 0)
                    System.arraycopy(heap, removedCount, heap, 0, size - removedCount);
                break;
            }

            Candidate<T> candidate = heap[i];
            found |= candidate.find(find);
            if (candidate.item == null)
            {
                Invariants.checkState(candidate.itemIdx == candidate.list.size());
                ++removedCount;
            }
            else if (removedCount > 0)
            {
                heap[i - removedCount] = candidate;
            }
            i++;
        }

        while (removedCount-- > 0)
            heap[--size] = null;

        Arrays.sort(heap, 0, size);
        return found;
    }
}