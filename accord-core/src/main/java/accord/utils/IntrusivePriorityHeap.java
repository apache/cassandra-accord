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
import java.util.stream.Stream;

/**
 * A simple array-based priority heap with intrusive elements permitting worst case logarithmic removals.
 * This collection however also defers the imposition of the heap property, so that an element which is inserted
 * and removed without an intervening poll/peek incurs only constant time costs.
 * @param <N>
 */
public abstract class IntrusivePriorityHeap<N extends IntrusivePriorityHeap.Node> implements Comparator<N>
{
    private static final int NORMAL_MIN_SIZE = 8;
    private static final int MAX_EMPTY_SIZE = 1024;
    private static final Node[] EMPTY = new Node[0];
    private static final Node[] TINY_EMPTY = new Node[0];

    public static abstract class Node
    {
        private int heapIndex = -1;

        protected boolean isInHeap()
        {
            return heapIndex >= 0;
        }

        final void setHeapIndex(int heapIndex)
        {
            this.heapIndex = heapIndex;
        }

        protected final int heapIndex()
        {
            return heapIndex;
        }
    }

    Node[] heap = EMPTY;
    int heapifiedSize;
    int size;

    public IntrusivePriorityHeap()
    {
        this(false);
    }

    public IntrusivePriorityHeap(boolean tiny)
    {
        if (tiny)
            heap = TINY_EMPTY;
    }

    /**
     * insert unsorted; can be used as a simple list
     */
    protected void append(N node)
    {
        Invariants.require(node.heapIndex() < 0);
        if (size == heap.length)
        {
            if (heap.length >= NORMAL_MIN_SIZE) heap = Arrays.copyOf(heap, size * 2);
            else if (heap == EMPTY) heap = new Node[NORMAL_MIN_SIZE];
            else heap = Arrays.copyOf(heap, Math.max(size + 2, size * 2));
        }

        node.setHeapIndex(size);
        heap[size++] = node;
    }

    /**
     * insert unsorted; can be used as a simple list
     */
    protected void update(N node)
    {
        int index = node.heapIndex();
        Invariants.require(heap[index] == node);
        if (index >= heapifiedSize)
            return;

        if (index == 0 || compare((N)heap[(index-1)/2], node) <= 0) siftDown(node, index);
        else siftUp(node, index);
    }

    protected boolean contains(N node)
    {
        int i = node.heapIndex();
        return i >= 0 && i < size && heap[i] == node;
    }

    protected boolean removeIfContains(N node)
    {
        int i = node.heapIndex();
        if (i < 0 || i >= heap.length || heap[i] != node)
            return false;
        removeInternal(i, node);
        return true;
    }

    /**
     * remove; can be used as a simple list
     */
    protected void remove(N node)
    {
        int i = node.heapIndex();
        Invariants.requireArgument(i >= 0 && i < heap.length && heap[i] == node);
        removeInternal(i, node);
    }

    private void removeInternal(int i, N node)
    {
        if (size > 1)
        {
            N tail = (N) heap[--size];
            if (heapifiedSize > i)
            {
                N heapifiedTail = (N) heap[--heapifiedSize];
                heap[heapifiedSize] = tail;
                tail.setHeapIndex(heapifiedSize);
                if (heapifiedSize != i)
                    replace(node, heapifiedTail, i);
            }
            else
            {
                heap[i] = tail;
                tail.setHeapIndex(i);
            }
        }
        else
        {
            size = heapifiedSize = 0;
            maybeShrink();
        }

        heap[size] = null;
        node.setHeapIndex(-1);
    }

    protected N peekNode()
    {
        if (size == 0)
            return null;

        Invariants.require(heapifiedSize == size);
        return (N) heap[0];
    }

    protected N pollNode()
    {
        if (size == 0)
            return null;

        Invariants.require(isHeapified());
        N result = (N) heap[0];
        result.setHeapIndex(-1);

        replaceHead();
        return result;
    }

    private void replace(N replacing, N with, int i)
    {
        Invariants.requireArgument(replacing == heap[i]);
        if (compare(with, replacing) <= 0) siftUp(with, i);
        else siftDown(with, i);
    }

    protected void replaceHead()
    {
        --size;
        --heapifiedSize;
        if (size == 0)
        {
            if (!maybeShrink())
                heap[0] = null;
            return;
        }

        N siftDown = (N) heap[size];
        heap[size] = null;
        siftDown(siftDown, 0);
    }

    private boolean maybeShrink()
    {
        if (heap.length <= MAX_EMPTY_SIZE)
            return false;

        heap = new Node[MAX_EMPTY_SIZE];
        return true;
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
                siftDown.setHeapIndex(i);
                heap[i] = siftDown;
                break;
            }
            else
            {
                heap[i] = swap;
                swap.setHeapIndex(i);
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
                siftUp.setHeapIndex(i);
                return;
            }

            heap[i] = parent;
            parent.setHeapIndex(i);
            i = parentIndex;
        }

        heap[0] = siftUp;
        siftUp.setHeapIndex(i);
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
        maybeShrink();
    }

    protected <P> void drain(P param, BiConsumer<P, N> consumer)
    {
        for (int i = 0 ; i < size ; ++i)
        {
            N node = (N) heap[i];
            node.setHeapIndex(-1);
            consumer.accept(param, node);
        }
        Arrays.fill(heap, 0, size, null);
        heapifiedSize = size = 0;
        maybeShrink();
    }

    /**
     * Note that this heap immediately passes ownership of any removed node to the caller;
     * if the Node is not inserted into another heap then {@link Node#setHeapIndex(-1)}
     * should be invoked.
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
            if (size == 0)
                maybeShrink();
        }
    }

    protected Stream<N> stream()
    {
        return Arrays.stream(heap, 0, size).map(n -> (N)n);
    }
}
