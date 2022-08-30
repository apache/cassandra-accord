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

import accord.api.Key;
import accord.primitives.TxnId;
import com.google.common.base.Preconditions;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.function.IntFunction;

/**
 * A set of utility classes and interfaces for managing a collection of buffers for arrays of certain types.
 *
 * These buffers are designed to be used to combine simple one-shot methods that consume and produce one or more arrays
 * with methods that may (or may not) call them repeatedly. Specifically, {@link accord.primitives.Deps#linearUnion},
 * {@link SortedArrays#linearUnion} and {@link SortedArrays#linearIntersection}
 *
 * To support this efficiently and ergonomically for users of the one-shot methods, the cache management must
 * support fetching buffers for re-use, but also returning either the buffer that was used (in the case where we
 * intend to re-invoke this or another method with the buffer as input), or a properly sized final output array
 * if the result of the method is to be consumed immediately.
 *
 * This functionality is implemented in {@link ObjectBuffers#complete(Object[], int)} and {@link IntBuffers#complete(int[], int)}
 * which may either shrink the output array, or capture the size and return the buffer.
 *
 * Since these methods also may return either of their inputs, which may themselves be buffers, we support capturing
 * the size of the input we have returned via {@link ObjectBuffers#completeWithExisting(Object[], int)}}
 */
public class ArrayBuffers
{
    private static final boolean FULLY_UNCACHED = true;

    // TODO: we should periodically clear the thread locals to ensure we aren't slowly accumulating unnecessarily large objects on every thread
    private static final ThreadLocal<IntBufferCache> INTS = ThreadLocal.withInitial(() -> new IntBufferCache(4, 1 << 14));
    private static final ThreadLocal<ObjectBufferCache<Key>> KEYS = ThreadLocal.withInitial(() -> new ObjectBufferCache<>(3, 1 << 9, Key[]::new));
    private static final ThreadLocal<ObjectBufferCache<TxnId>> TXN_IDS = ThreadLocal.withInitial(() -> new ObjectBufferCache<>(3, 1 << 12, TxnId[]::new));

    public static IntBuffers cachedInts()
    {
        return INTS.get();
    }

    public static ObjectBuffers<Key> cachedKeys()
    {
        return KEYS.get();
    }

    public static ObjectBuffers<TxnId> cachedTxnIds()
    {
        return TXN_IDS.get();
    }

    public static <T> ObjectBuffers<T> uncached(IntFunction<T[]> allocator) { return new UncachedObjectBuffers<>(allocator); }

    public static IntBuffers uncachedInts() { return UncachedIntBuffers.INSTANCE; }

    public interface IntBufferAllocator
    {
        /**
         * Return an {@code int[]} of size at least {@code minSize}, possibly from a pool
         */
        int[] getInts(int minSize);
    }

    public interface IntBuffers extends IntBufferAllocator
    {
        /**
         * To be invoked on the result buffer with the number of elements contained;
         * either the buffer will be returned and the size optionally captured, or else the result may be
         * shrunk to the size of the contents, depending on implementation.
         */
        int[] complete(int[] buffer, int size);

        /**
         * The buffer is no longer needed by the caller, which is discarding the array;
         * if {@link #complete(int[], int)} returned the buffer as its result this buffer should NOT be
         * returned to any pool.
         *
         * Note that this method assumes {@link #complete(int[], int)} was invoked on this buffer previously.
         * However, it is guaranteed that a failure to do so does not leak memory or pool space, only produces some
         * additional garbage.
         *
         * @return true if the buffer is discarded (and discard-able), false if it was retained or is believed to be in use
         */
        boolean discard(int[] buffer, int size);

        /**
         * Indicate this buffer is definitely unused, and return it to a pool if possible
         * @return true if the buffer is discarded (and discard-able), false if it was retained
         */
        boolean forceDiscard(int[] buffer);
    }

    public interface ObjectBuffers<T>
    {
        /**
         * Return an {@code T[]} of size at least {@code minSize}, possibly from a pool
         */
        T[] get(int minSize);

        /**
         * To be invoked on the result buffer with the number of elements contained;
         * either the buffer will be returned and the size optionally captured, or else the result may be
         * shrunk to the size of the contents, depending on implementation.
         */
        T[] complete(T[] buffer, int size);

        /**
         * To be invoked on an input buffer that constitutes the result, with the number of elements it contained;
         * either the buffer will be returned and the size optionally captured, or else the result may be
         * shrunk to the size of the contents, depending on implementation.
         */
        T[] completeWithExisting(T[] buffer, int size);

        /**
         * The buffer is no longer needed by the caller, which is discarding the array;
         * if {@link #complete(Object[], int)} returned the buffer as its result, this buffer should NOT be
         * returned to any pool.
         *
         * Note that this method assumes {@link #complete(Object[], int)} was invoked on this buffer previously.
         * However, it is guaranteed that a failure to do so does not leak memory or pool space, only produces some
         * additional garbage.
         *
         * Note also that {@code size} should represent the size of the used space in the array, even if it was later
         * truncated.
         *
         * @return true if the buffer is discarded (and discard-able), false if it was retained or is believed to be in use
         */
        boolean discard(T[] buffer, int size);

        /**
         * Indicate this buffer is definitely unused, and return it to a pool if possible
         *
         * Note that {@code size} should represent the size of the used space in the array, even if it was later truncated.
         *
         * @return true if the buffer is discarded (and discard-able), false if it was retained
         */
        boolean forceDiscard(T[] buffer, int size);

        /**
         * Returns the {@code size} parameter provided to the most recent {@link #complete(Object[], int)} or {@link #completeWithExisting(Object[], int)}
         *
         * Depending on implementation, this is either saved from the last such invocation, or else simply returns the size of the buffer parameter.
         */
        int lengthOfLast(T[] buffer);
    }

    private static final class UncachedIntBuffers implements IntBuffers
    {
        static final UncachedIntBuffers INSTANCE = new UncachedIntBuffers();
        private UncachedIntBuffers()
        {
        }

        @Override
        public int[] getInts(int minSize)
        {
            return new int[minSize];
        }

        @Override
        public int[] complete(int[] buffer, int size)
        {
            if (size == buffer.length)
                return buffer;

            return Arrays.copyOf(buffer, size);
        }

        @Override
        public boolean discard(int[] buffer, int size)
        {
            return forceDiscard(buffer);
        }

        @Override
        public boolean forceDiscard(int[] buffer)
        {
            // if FULLY_UNCACHED we want our caller to also not cache us, so we indicate the buffer has been retained
            return !FULLY_UNCACHED;
        }
    }

    private static final class UncachedObjectBuffers<T> implements ObjectBuffers<T>
    {
        final IntFunction<T[]> allocator;
        private UncachedObjectBuffers(IntFunction<T[]> allocator)
        {
            this.allocator = allocator;
        }

        @Override
        public T[] get(int minSize)
        {
            return allocator.apply(minSize);
        }

        @Override
        public T[] complete(T[] buffer, int size)
        {
            if (size == buffer.length)
                return buffer;

            return Arrays.copyOf(buffer, size);
        }

        @Override
        public T[] completeWithExisting(T[] buffer, int size)
        {
            Preconditions.checkArgument(buffer.length == size);
            return buffer;
        }

        @Override
        public int lengthOfLast(T[] buffer)
        {
            return buffer.length;
        }

        @Override
        public boolean discard(T[] buffer, int size)
        {
            return forceDiscard(buffer, size);
        }

        @Override
        public boolean forceDiscard(T[] buffer, int size)
        {
            // if FULLY_UNCACHED we want our caller to also not cache us, so we indicate the buffer has been retained
            return !FULLY_UNCACHED;
        }
    }

    /**
     * A very simple cache that simply stores the largest {@code maxCount} arrays smaller than {@code maxSize}.
     * Works on both primitive and Object arrays.
     */
    private static abstract class AbstractBufferCache<B>
    {
        interface Clear<B>
        {
            void clear(B array, int usedSize);
        }

        final IntFunction<B> allocator;
        final Clear<B> clear;
        final B empty;
        final B[] cached;
        final int maxSize;

        AbstractBufferCache(IntFunction<B> allocator, Clear<B> clear, int maxCount, int maxSize)
        {
            this.allocator = allocator;
            this.maxSize = maxSize;
            this.cached = (B[])new Object[maxCount];
            this.empty = allocator.apply(0);
            this.clear = clear;
        }

        B getInternal(int minSize)
        {
            if (minSize == 0)
                return empty;

            if (minSize > maxSize)
                return allocator.apply(minSize);

            for (int i = 0 ; i < cached.length ; ++i)
            {
                if (cached[i] != null && Array.getLength(cached[i]) >= minSize)
                {
                    B result = cached[i];
                    cached[i] = null;
                    return result;
                }
            }

            return allocator.apply(minSize);
        }

        boolean discardInternal(B buffer, int bufferSize, int usedSize, boolean force)
        {
            if (bufferSize > maxSize)
                return true;

            if (bufferSize == usedSize && !force)
                return false;

            for (int i = 0 ; i < cached.length ; ++i)
            {
                if (cached[i] == null || Array.getLength(cached[i]) < bufferSize)
                {
                    clear.clear(buffer, usedSize);
                    cached[i] = buffer;
                    return false;
                }
            }

            return true;
        }
    }

    public static class IntBufferCache extends AbstractBufferCache<int[]> implements IntBuffers
    {
        IntBufferCache(int maxCount, int maxSize)
        {
            super(int[]::new, (i1, i2) -> {}, maxCount, maxSize);
        }

        @Override
        public int[] complete(int[] buffer, int size)
        {
            if (size == buffer.length)
                return buffer;

            return Arrays.copyOf(buffer, size);
        }

        @Override
        public boolean discard(int[] buffer, int size)
        {
            return discardInternal(buffer, buffer.length, size, false);
        }

        @Override
        public boolean forceDiscard(int[] buffer)
        {
            return discardInternal(buffer, buffer.length, -1, true);
        }

        @Override
        public int[] getInts(int minSize)
        {
            return getInternal(minSize);
        }
    }

    public static class ObjectBufferCache<T> extends AbstractBufferCache<T[]> implements ObjectBuffers<T>
    {
        final IntFunction<T[]> allocator;

        ObjectBufferCache(int maxCount, int maxSize, IntFunction<T[]> allocator)
        {
            super(allocator, (array, usedSize) -> Arrays.fill(array, 0, usedSize, null), maxCount, maxSize);
            this.allocator = allocator;
        }

        public T[] complete(T[] buffer, int size)
        {
            if (size == buffer.length)
                return buffer;

            return Arrays.copyOf(buffer, size);
        }

        @Override
        public T[] completeWithExisting(T[] buffer, int size)
        {
            return buffer;
        }

        public int lengthOfLast(T[] buffer)
        {
            return buffer.length;
        }

        public boolean discard(T[] buffer, int size)
        {
            return discardInternal(buffer, buffer.length, size, false);
        }

        @Override
        public boolean forceDiscard(T[] buffer, int size)
        {
            return discardInternal(buffer, buffer.length, size, true);
        }

        @Override
        public T[] get(int minSize)
        {
            return getInternal(minSize);
        }
    }

    /**
     * Returns the buffer to the caller, saving the length if necessary
     */
    public static class PassThroughObjectBuffers<T> implements ObjectBuffers<T>
    {
        final ObjectBuffers<T> objs;
        T[] savedObjs; // permit saving of precisely one unused buffer of any size to assist LinearMerge
        int length = -1;

        public PassThroughObjectBuffers(ObjectBuffers<T> objs)
        {
            this.objs = objs;
        }

        @Override
        public T[] get(int minSize)
        {
            length = -1;
            if (savedObjs != null && savedObjs.length >= minSize)
            {
                T[] result = savedObjs;
                savedObjs = null;
                return result;
            }
            return objs.get(minSize);
        }

        @Override
        public T[] complete(T[] buffer, int size)
        {
            length = size;
            return buffer;
        }

        @Override
        public T[] completeWithExisting(T[] buffer, int size)
        {
            length = size;
            return buffer;
        }

        /**
         * Invoke {@link #complete(Object[], int)} on the wrapped ObjectBuffers
         */
        public T[] realComplete(T[] buffer, int size)
        {
            return objs.complete(buffer, size);
        }

        @Override
        public boolean discard(T[] buffer, int size)
        {
            return true;
        }

        @Override
        public boolean forceDiscard(T[] buffer, int size)
        {
            length = -1;
            if (!objs.forceDiscard(buffer, size))
                return false;

            return discardInternal(buffer);
        }

        /**
         * Invoke {@link #discard(Object[], int)} on the wrapped ObjectBuffers
         */
        public void realDiscard(T[] buffer, int size)
        {
            length = -1;
            if (!objs.discard(buffer, size))
                return;

            discardInternal(buffer);
        }

        private boolean discardInternal(T[] buffer)
        {
            if (savedObjs != null && savedObjs.length >= buffer.length)
                return false;

            savedObjs = buffer;
            return true;
        }

        public int lengthOfLast(T[] buffer)
        {
            if (length == -1)
                throw new IllegalStateException("Attempted to get last length but no call to complete called");
            return length;
        }
    }

    /**
     * Returns the buffer to the caller, saving the length if necessary
     */
    public static class PassThroughObjectAndIntBuffers<T> extends PassThroughObjectBuffers<T> implements IntBuffers
    {
        final IntBuffers ints;
        int[] savedInts;

        public PassThroughObjectAndIntBuffers(ObjectBuffers<T> objs, IntBuffers ints)
        {
            super(objs);
            this.ints = ints;
        }

        @Override
        public int[] getInts(int minSize)
        {
            if (savedInts != null && savedInts.length >= minSize)
            {
                int[] result = savedInts;
                savedInts = null;
                return result;
            }

            return ints.getInts(minSize);
        }

        @Override
        public int[] complete(int[] buffer, int size)
        {
            return buffer;
        }

        @Override
        public boolean discard(int[] buffer, int size)
        {
            return false;
        }

        @Override
        public boolean forceDiscard(int[] buffer)
        {
            if (!ints.forceDiscard(buffer))
                return false;

            if (savedInts != null && savedInts.length >= buffer.length)
                return true;

            savedInts = buffer;
            return false;
        }

        public int[] realComplete(int[] buffer, int size)
        {
            return ints.complete(buffer, size);
        }

        /**
         * Pass-through the discard
         */
        public void realDiscard(int[] buffer, int size)
        {
            if (!ints.discard(buffer, size))
                return;

            if (savedInts != null && savedInts.length >= buffer.length)
                return;

            savedInts = buffer;
            return;
        }
    }

}
