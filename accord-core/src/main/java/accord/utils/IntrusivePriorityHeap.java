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
import java.util.Comparator;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

/**
 * A simple array-based priority heap with intrusive elements permitting worst case logarithmic removals.
 * This collection however also defers the imposition of the heap property, so that an element which is inserted
 * and removed without an intervening poll/peek incurs only constant time costs.
 * @param <N>
 */
public abstract class IntrusivePriorityHeap<N extends IntrusivePriorityHeap.Node> implements Comparator<N>
{
    private static final Node[] EMPTY = new Node[0];

    public static abstract class Node
    {
        int heapIndex = -1;

        protected boolean isInHeap()
        {
            return heapIndex >= 0;
        }

        protected void removedFromHeap()
        {
            heapIndex = -1;
        }
    }

    Node[] heap = EMPTY;
    int heapifiedSize;
    int size;

    /**
     * insert unsorted; can be used as a simple list
     */
    protected void append(N node)
    {
        Invariants.checkState(node.heapIndex < 0);
        if (size == heap.length)
            heap = Arrays.copyOf(heap, Math.max(size * 2, 8));

        node.heapIndex = size;
        heap[size++] = node;
    }

    /**
     * insert unsorted; can be used as a simple list
     */
    protected void update(N node)
    {
        int index = node.heapIndex;
        Invariants.checkState(heap[index] == node);
        if (index >= heapifiedSize)
            return;

        if (index == 0 || compare((N)heap[(index-1)/2], node) <= 0) siftDown(node, index);
        else siftUp(node, index);
    }

    protected boolean contains(N node)
    {
        int i = node.heapIndex;
        return i >= 0 && i < size && heap[i] == node;
    }

    /**
     * remove; can be used as a simple list
     */
    protected void remove(N node)
    {
        int i = node.heapIndex;
        Invariants.checkArgument(i >= 0 && i < heap.length && heap[i] == node);
        if (size > 1)
        {
            N tail = (N) heap[--size];
            if (heapifiedSize > i)
            {
                N heapifiedTail = (N) heap[--heapifiedSize];
                heap[heapifiedSize] = tail;
                tail.heapIndex = heapifiedSize;
                if (heapifiedSize != i)
                    replace(node, heapifiedTail, i);
            }
            else
            {
                heap[i] = tail;
                tail.heapIndex = i;
            }
        }
        else size = heapifiedSize = 0;

        heap[size] = null;
        node.heapIndex = -1;
    }

    protected N peekNode()
    {
        if (size == 0)
            return null;

        Invariants.checkState(heapifiedSize == size);
        return (N) heap[0];
    }

    protected N pollNode()
    {
        if (size == 0)
            return null;

        Invariants.checkState(isHeapified());
        N result = (N) heap[0];
        result.heapIndex = -1;

        replaceHead();
        return result;
    }

    private void replace(N replacing, N with, int i)
    {
        Invariants.checkArgument(replacing == heap[i]);
        if (compare(with, replacing) <= 0) siftUp(with, i);
        else siftDown(with, i);
    }

    protected void replaceHead()
    {
        --size;
        --heapifiedSize;
        if (size == 0)
        {
            heap[0] = null;
            return;
        }

        N siftDown = (N) heap[size];
        heap[size] = null;
        siftDown(siftDown, 0);
    }

    /**
     * {@code i} is a free position in the heap, siftDown must be safely inserted at a position >= i
     */
    protected void siftDown(N siftDown, int i)
    {
        while (true)
        {
            N swap = null;
            int childIndex = i * 2 + 1;
            int nexti = childIndex;

            if (childIndex < heapifiedSize)
            {
                swap = (N) heap[childIndex];
                if (childIndex + 1 < heapifiedSize)
                {
                    N right = (N) heap[childIndex + 1];
                    if (compare(right, swap) <= 0)
                    {
                        ++nexti;
                        swap = right;
                    }
                }
                if (compare(swap, siftDown) >= 0)
                    swap = null;
            }

            if (swap == null)
            {
                siftDown.heapIndex = i;
                heap[i] = siftDown;
                break;
            }
            else
            {
                heap[i] = swap;
                swap.heapIndex = i;
                i = nexti;
            }
        }
    }

    /**
     * {@code i} is a free position in the heap, and the node at heap[i] must sort correctly
     * at position <= i
     */
    private void siftUp(N siftUp, int i)
    {
        while (i > 0)
        {
            int parentIndex = (i - 1) / 2;
            N parent = (N) heap[parentIndex];
            if (compare(parent, siftUp) <= 0)
            {
                heap[i] = siftUp;
                siftUp.heapIndex = i;
                return;
            }

            heap[i] = parent;
            parent.heapIndex = i;
            i = parentIndex;
        }

        heap[0] = siftUp;
        siftUp.heapIndex = i;
    }

    /**
     * remove; can be used as a simple list
     */
    protected void heapify()
    {
        while (heapifiedSize < size)
            siftUp((N)heap[heapifiedSize], heapifiedSize++);
    }

    protected N get(int i)
    {
        return (N) heap[i];
    }

    public int size()
    {
        return size;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    protected void ensureHeapified()
    {
        if (heapifiedSize != size)
            heapify();
    }

    protected boolean isHeapified()
    {
        return heapifiedSize == size;
    }

    protected void clear()
    {
        Arrays.fill(heap, 0, size, null);
        heapifiedSize = size = 0;
    }

    protected <P> void drain(P param, BiConsumer<P, N> consumer)
    {
        for (int i = 0 ; i < size ; ++i)
        {
            N node = (N) heap[i];
            node.heapIndex = -1;
            consumer.accept(param, node);
        }
        Arrays.fill(heap, 0, size, null);
        heapifiedSize = size = 0;
    }

    /**
     * Note that this heap immediately passes ownership of any removed node to the caller;
     * if the Node is not inserted into another heap then {@link Node#removedFromHeap)}
     * should be invoked to reset the heapIndex to -1.
     */
    protected <P> void filterUnheapified(P param, BiPredicate<P, N> remove)
    {
        int removedCount = 0;
        for (int i = heapifiedSize ; i < size ; ++i)
        {
            if (remove.test(param, (N) heap[i]))
            {
                // we don't update the heapIndex to -1 here, as we assume it has already been re-used
                ++removedCount;
            }
            else if (removedCount > 0)
            {
                Node n = heap[i];
                heap[i - removedCount] = n;
                n.heapIndex = i - removedCount;
            }
        }
        if (removedCount > 0)
        {
            Arrays.fill(heap, size - removedCount, size, null);
            size -= removedCount;
        }
    }
}
