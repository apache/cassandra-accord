package accord.utils;

import java.util.Arrays;
import java.util.function.IntUnaryOperator;

import com.google.common.base.Preconditions;

import org.apache.cassandra.utils.concurrent.Inline;

/**
 * Lifted directly from MergeIterator in Cassandra source tree, with some slight modifications to support operation
 * over a simple int[] containing pairs of integers representing some stream value and a stream identifier.
 *
 * Usage:
 * <pre>{@code
 *
 * int[] heap = InlineHeap.create(size);
 * for (int i = 0 ; i < size ; ++i)
 *     InlineHeap.set(heap, i, streamValue, streamId)
 * InlineHeap.heapify(heap, size);
 *
 * while (size > 0)
 * {
 *     p = InlineHeap.consume(heap, size, (streamValue, streamId, param) -> {
 *         return doSomething(param);
 *     }, p)
 *
 *     size = InlineHeap.advance(heap, size, streamId -> nextStreamValue);
 * }
 *
 * }</pre>
 */
public class InlineHeap
{
    public interface IntHeapFold
    {
        int apply(int key, int stream, int v);
    }

    public static final int NIL = Integer.MIN_VALUE;

    /**
     * The number of elements to keep in order before the binary heap starts, exclusive of the top heap element.
     */
    static final int SORTED_SECTION_SIZE = 4;

    // returns the new size of the heap
    public static int heapify(int[] heap, int size)
    {
        for (int i = size - 1; i >= 0; --i)
        {
            int key = key(heap, i);
            int stream = stream(heap, i);
            if (key == NIL)
                throw new AssertionError();
            size = replaceAndSink(heap, key, stream, i, size);
        }
        return size;
    }

    @Inline
    public static int advance(int[] heap, int size, IntUnaryOperator next)
    {
        for (int i = maxConsumed(heap, size); i >= 0; --i)
        {
            int key = key(heap, i);
            if (key == NIL)
            {
                int stream = stream(heap, i);
                key = next.applyAsInt(stream);
                int newSize = replaceAndSink(heap, key, stream, i, size);
                if (key == NIL && newSize != size - 1)
                    throw new AssertionError();
                size = newSize;
            }
        }
        return size;
    }

    // yield the number of items to consume from the head of the heap (i.e. indexes heap[0..consume(heap)])
    @Inline
    public static int consume(int[] heap, int size, IntHeapFold consumer, int v)
    {
        int key = key(heap, 0);
        if (key == NIL)
            throw new AssertionError();

        int i = 0;
        int limit = Math.min(size, SORTED_SECTION_SIZE + 1);
        int increment = 2;
        boolean extend = false;
        while (true)
        {
            if (key(heap, i) == key)
            {
                v = consumer.apply(key, stream(heap, i), v);
                clearKey(heap, i);
                extend = true;
            }
            else if (i <= SORTED_SECTION_SIZE)
            {
                return v;
            }

            ++i;
            if (i == limit)
            {
                if (i == size || !extend)
                    return v;

                extend = false;
                limit = Math.min(limit + increment, size);
                increment *= 2;
            }
        }
    }

    static int maxConsumed(int[] heap, int size)
    {
        // TODO: might be fastest if we made SIMD friendly, and just scanned for NIL?
        for (int i = 1 ; ; ++i)
        {
            if (i == size || key(heap, i) != NIL)
                return i - 1;
            if (i >= SORTED_SECTION_SIZE)
                break;
        }

        int start = SORTED_SECTION_SIZE + 1;
        int result = SORTED_SECTION_SIZE;
        int end = Math.min(size, SORTED_SECTION_SIZE + 3);
        // walk backwards looking for the first NIL item - this is the max at this depth
        int i = end;
        while (true)
        {
            if (key(heap, --i) == NIL)
            {
                if (end == size)
                    return i;

                result = i;
                i = Math.min(size, end + 2 * (end - start));
                start = end;
                end = i;
            }
            else if (i == start)
            {
                return result;
            }
        }
    }

    /**
     * Replace an iterator in the heap with the given position and move it down the heap until it finds its proper
     * position, pulling lighter elements up the heap.
     *
     * Whenever an equality is found between two elements that form a new parent-child relationship, the child's
     * equalParent flag is set to true if the elements are equal.
     */
    private static int replaceAndSink(int[] heap, int key, int stream, int currIdx, int size)
    {
        int headIndex = currIdx;
        if (key == NIL)
        {
            // Drop stream by replacing it with the last one in the heap.
            if (--size == currIdx)
            {
                clear(heap, size);
                return size;
            }

            key = key(heap, size);
            if (key == NIL)
                throw new AssertionError();
            stream = stream(heap, size);
            clear(heap, size);
        }

        final int sortedSectionSize = Math.min(size - 1, SORTED_SECTION_SIZE);

        int nextIdx;

        // Advance within the sorted section, pulling up items lighter than candidate.
        while ((nextIdx = currIdx + 1) <= sortedSectionSize)
        {
            if (!equalParent(heap, headIndex, nextIdx)) // if we were greater then an (or were the) equal parent, we are >= the child
            {
                int cmp = Integer.compare(key, key(heap, nextIdx));
                if (cmp <= 0)
                {
                    set(heap, currIdx, key, stream);
                    return size;
                }
            }

            copy(heap, nextIdx, currIdx);
            currIdx = nextIdx;
        }
        // If size <= SORTED_SECTION_SIZE, nextIdx below will be no less than size,
        // because currIdx == sortedSectionSize == size - 1 and nextIdx becomes
        // (size - 1) * 2) - (size - 1 - 1) == size.

        // Advance in the binary heap, pulling up the lighter element from the two at each level.
        while ((nextIdx = (currIdx * 2) - (sortedSectionSize - 1)) + 1 < size)
        {
            if (!equalParent(heap, headIndex, nextIdx))
            {
                if (!equalParent(heap, headIndex, nextIdx + 1))
                {
                    // pick the smallest of the two children
                    int siblingCmp = Integer.compare(key(heap, nextIdx + 1), key(heap, nextIdx));
                    if (siblingCmp < 0)
                        ++nextIdx;

                    // if we're smaller than this, we are done, and must only restore the heap and equalParent properties
                    int cmp = Integer.compare(key, key(heap, nextIdx));
                    if (cmp <= 0)
                    {
                        set(heap, currIdx, key, stream);
                        return size;
                    }
                }
                else
                    ++nextIdx;  // descend down the path where we found the equal child
            }

            copy(heap, nextIdx, currIdx);
            currIdx = nextIdx;
        }

        // our loop guard ensures there are always two siblings to process; typically when we exit the loop we will
        // be well past the end of the heap and this next condition will match...
        if (nextIdx >= size)
        {
            set(heap, currIdx, key, stream);
            return size;
        }

        // ... but sometimes we will have one last child to compare against, that has no siblings
        if (!equalParent(heap, headIndex, nextIdx))
        {
            int cmp = Integer.compare(key, key(heap, nextIdx));
            if (cmp <= 0)
            {
                set(heap, currIdx, key, stream);
                return size;
            }
        }

        copy(heap, nextIdx, currIdx);
        set(heap, nextIdx, key, stream);
        return size;
    }

    static boolean equalParent(int[] heap, int headIndex, int index)
    {
        int parentIndex = parentIndex(index);
        if (parentIndex <= headIndex)
            return false;
        return key(heap, parentIndex) == key(heap, index);
    }

    public static void set(int[] heap, int index, int key, int stream)
    {
        if (key == NIL)
            throw new AssertionError();
        heap[index * 2] = key;
        heap[index * 2 + 1] = stream;
    }

    public static void clearKey(int[] heap, int index)
    {
        heap[index * 2] = NIL;
    }

    static void copy(int[] heap, int src, int trg)
    {
        heap[trg * 2] = heap[src * 2];
        heap[trg * 2 + 1] = heap[src * 2 + 1];
    }

    public static int key(int[] heap, int index)
    {
        return heap[index * 2];
    }

    public static int stream(int[] heap, int index)
    {
        return heap[index * 2 + 1];
    }

    static void clear(int[] heap, int index)
    {
        Arrays.fill(heap, index * 2, (index + 1) * 2, NIL);
    }

    private static int parentIndex(int index)
    {
        if (index <= SORTED_SECTION_SIZE)
            return index - 1;
        return ((index + SORTED_SECTION_SIZE - 1) / 2);
    }

    public static int[] create(int size)
    {
        return new int[size * 2];
    }

    public static void validate(int[] heap, int size)
    {
        for (int i = 1 ; i < size ; i++)
        {
            if (key(heap, i) < key(heap, parentIndex(i)))
                throw new AssertionError();
        }
    }
}
