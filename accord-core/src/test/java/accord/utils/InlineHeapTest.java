package accord.utils;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.utils.InlineHeap.NIL;

public class InlineHeapTest
{
    @Test
    public void testSmall()
    {
        int streams = 2;
        int range = 50;
        int minStreamLength = 2;
        int maxStreamLength = 10;
        test(ThreadLocalRandom.current().nextLong(), streams, range, minStreamLength, maxStreamLength);
    }

    @Test
    public void testMedium()
    {
        int streams = 16;
        int range = 200;
        int minStreamLength = 10;
        int maxStreamLength = 100;
        test(ThreadLocalRandom.current().nextLong(), streams, range, minStreamLength, maxStreamLength);
    }

    @Test
    public void testMany()
    {
        int streams = 32;
        int range = 400;
        int minStreamLength = 50;
        int maxStreamLength = 100;
        test(ThreadLocalRandom.current().nextLong(), streams, range, minStreamLength, maxStreamLength);
    }

    @Test
    public void testSparse()
    {
        int streams = 32;
        int range = 2000;
        int minStreamLength = 50;
        int maxStreamLength = 100;
        test(ThreadLocalRandom.current().nextLong(), streams, range, minStreamLength, maxStreamLength);
    }

    @Test
    public void testConsume()
    {
        // hard coded for SORTED_SECTION_SIZE=4 (meaning indexes 0..3 are "sorted" and 4 is the head of the binary heap)
        testConsume(new int[] { 1, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 }, 1);
        testConsume(new int[] { 1, 1, 9, 9, 9, 9, 9, 9, 9, 9, 9 }, 2);
        testConsume(new int[] { 1, 1, 1, 9, 9, 9, 9, 9, 9, 9, 9 }, 3);
        testConsume(new int[] { 1, 1, 1, 1, 9, 9, 9, 9, 9, 9, 9 }, 4);
        testConsume(new int[] { 1, 1, 1, 1, 1, 9, 9, 9, 9, 9, 9 }, 5);
        testConsume(new int[] { 1, 1, 1, 1, 1, 1, 9, 9, 9, 9, 9 }, 6);
        testConsume(new int[] { 1, 1, 1, 1, 1, 9, 1, 9, 9, 9, 9 }, 6);
        testConsume(new int[] { 1, 1, 1, 1, 1, 1, 9, 1, 9, 9, 9 }, 7);
        testConsume(new int[] { 1, 1, 1, 1, 1, 1, 9, 9, 1, 9, 9 }, 7);
        testConsume(new int[] { 1, 1, 1, 1, 1, 9, 1, 9, 9, 1, 9 }, 7);
        testConsume(new int[] { 1, 1, 1, 1, 1, 9, 1, 9, 9, 9, 1 }, 7);
    }

    private static void testConsume(int[] keys, int expect)
    {
        int size = keys.length;
        int[] heap = InlineHeap.create(size);
        for (int i = 0 ; i < size ; ++i)
            InlineHeap.set(heap, i, keys[i], 0);

        int count = InlineHeap.consume(heap, size, (key, stream, v) -> v + 1, 0);
        Assertions.assertEquals(expect, count);
        count = 0;
        int max = 0;
        for (int i = 0 ; i < size ; ++i)
        {
            if (InlineHeap.key(heap, i) == NIL)
            {
                max = i;
                count++;
            }
        }
        Assertions.assertEquals(expect, count);
        count = 0;
        for (int i = 0 ; i < size ; ++i)
            count += InlineHeap.key(heap, i) == 1 ? 1 : 0;
        Assertions.assertEquals(0, count);
        Assertions.assertEquals(max, InlineHeap.maxConsumed(heap, size));
        int newSize = InlineHeap.advance(heap, size, s -> 2);
        Assertions.assertEquals(size, newSize);
        count = 0;
        for (int i = 0 ; i < size ; ++i)
            count += InlineHeap.key(heap, i) == 2 ? 1 : 0;
        Assertions.assertEquals(expect, count);
    }

    public static void test(long seed, int streams, int range, int minStreamLength, int maxStreamLength)
    {
        Random random = new Random();
        random.setSeed(seed);

        List<Map.Entry<Integer, Integer>> canonical = new ArrayList<>();

        int[] heap = InlineHeap.create(streams);
        int[][] keys = new int[streams][];
        int[] indexes = new int[streams];
        for (int i = 0 ; i < streams ; ++i)
        {
            BitSet used = new BitSet(range);
            int count = minStreamLength + random.nextInt(maxStreamLength - minStreamLength);
            keys[i] = new int[count];
            while (--count >= 0)
            {
                int key = random.nextInt(range);
                while (used.get(key))
                    key = random.nextInt(range);

                used.set(key);
                keys[i][count] = key;
                canonical.add(new SimpleEntry<>(key, i));
            }

            Arrays.sort(keys[i]);

            InlineHeap.set(heap, i, keys[i][0], i);
        }

        canonical.sort((a, b) -> {
            int c = Integer.compare(a.getKey(), b.getKey());
            if (c == 0) c = Integer.compare(a.getValue(), b.getValue());
            return c;
        });
        int count = 0;
        List<Map.Entry<Integer, Integer>> tmp = new ArrayList<>();
        int heapSize = InlineHeap.heapify(heap, streams);
        while (heapSize > 0)
        {
            tmp.clear();
            InlineHeap.consume(heap, heapSize, (key, stream, v) -> {
                tmp.add(new SimpleEntry<>(key, stream));
                return v;
            }, 0);
            tmp.sort(Entry.comparingByValue());
            for (int i = 0 ; i < tmp.size() ; i++)
                Assertions.assertEquals(canonical.get(count + i), tmp.get(i), "Seed: " + seed);
            count += tmp.size();
            heapSize = InlineHeap.advance(heap, heapSize, stream -> {
                return ++indexes[stream] == keys[stream].length ? Integer.MIN_VALUE : keys[stream][indexes[stream]];
            });
            InlineHeap.validate(heap, heapSize);
        }

        Assertions.assertEquals(canonical.size(), count, "Seed: " + seed);
    }

    public static void main(String[] args)
    {
        for (long seed = 0 ; seed < 10000 ; ++seed)
        {
            test(seed, 2, 50, 2, 10);
            test(seed, 16, 200, 10, 100);
            test(seed, 32, 400, 50, 100);
            test(seed, 32, 2000, 50, 100);
        }

    }

}
