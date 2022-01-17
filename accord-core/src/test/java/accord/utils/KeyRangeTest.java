package accord.utils;

import accord.api.Key;
import accord.topology.KeyRange;
import accord.impl.IntKey;
import accord.topology.KeyRanges;
import accord.txn.Keys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KeyRangeTest
{
    static IntKey k(int v)
    {
        return new IntKey(v);
    }

    private static KeyRange<IntKey> r(int start, int end)
    {
        return IntKey.range(start, end);
    }

    private static class EndInclusiveIntRange extends KeyRange.EndInclusive<IntKey>
    {
        public EndInclusiveIntRange(IntKey start, IntKey end)
        {
            super(start, end);
        }

        @Override
        public KeyRange<IntKey> subRange(IntKey start, IntKey end)
        {
            return new EndInclusiveIntRange(start, end);
        }
    }

    private static class StartInclusiveIntRange extends KeyRange.StartInclusive<IntKey>
    {
        public StartInclusiveIntRange(IntKey start, IntKey end)
        {
            super(start, end);
        }

        @Override
        public KeyRange<IntKey> subRange(IntKey start, IntKey end)
        {
            return new StartInclusiveIntRange(start, end);
        }
    }

    static KeyRange<IntKey> rangeEndIncl(int start, int end)
    {
        return new EndInclusiveIntRange(k(start), k(end));
    }

    static KeyRange<IntKey> rangeStartIncl(int start, int end)
    {
        return new StartInclusiveIntRange(k(start), k(end));
    }

    static Keys keys(int... values)
    {
        Key[] keys = new Key[values.length];
        for (int i=0; i<values.length; i++)
            keys[i] = IntKey.key(values[i]);
        return new Keys(keys);
    }

    private static void assertInvalidKeyRange(int start, int end)
    {
        try
        {
            rangeStartIncl(start, end);
            Assertions.fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }

        try
        {
            rangeEndIncl(start, end);
            Assertions.fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    @Test
    void invalidRangeTest()
    {
        assertInvalidKeyRange(1, 1);
        assertInvalidKeyRange(2, 1);
    }

    @Test
    void containsTest()
    {
        KeyRange<IntKey> endInclRange = rangeEndIncl(10, 20);
        Assertions.assertFalse(endInclRange.containsKey(k(10)));
        Assertions.assertFalse(endInclRange.startInclusive());
        Assertions.assertTrue(endInclRange.containsKey(k(20)));
        Assertions.assertTrue(endInclRange.endInclusive());

        KeyRange<IntKey> startInclRange = rangeStartIncl(10, 20);
        Assertions.assertTrue(startInclRange.containsKey(k(10)));
        Assertions.assertTrue(startInclRange.startInclusive());
        Assertions.assertFalse(startInclRange.containsKey(k(20)));
        Assertions.assertFalse(startInclRange.endInclusive());
    }

    private static void assertHigherKeyIndex(int expectedIdx, KeyRange range, Keys keys)
    {
        if (expectedIdx > 0 && expectedIdx < keys.size())
            Assertions.assertTrue(range.containsKey(keys.get(expectedIdx - 1)));
        int actualIdx = range.higherKeyIndex(keys);
        Assertions.assertEquals(expectedIdx, actualIdx);
    }

    @Test
    void higherKeyIndexTest()
    {
        Keys keys = keys(10, 11, 12, 13, 14, 15, 16);
        assertHigherKeyIndex(0, rangeEndIncl(0, 9), keys);
        assertHigherKeyIndex(0, rangeStartIncl(0, 10), keys);
        assertHigherKeyIndex(0, rangeEndIncl(0, 5), keys);
        assertHigherKeyIndex(0, rangeStartIncl(0, 5), keys);

        assertHigherKeyIndex(1, rangeEndIncl(9, 10), keys);
        assertHigherKeyIndex(0, rangeStartIncl(9, 10), keys);
        assertHigherKeyIndex(5, rangeEndIncl(11, 14), keys);
        assertHigherKeyIndex(4, rangeStartIncl(11, 14), keys);
        assertHigherKeyIndex(6, rangeEndIncl(11, 15), keys);
        assertHigherKeyIndex(5, rangeStartIncl(11, 15), keys);

        assertHigherKeyIndex(7, rangeEndIncl(16, 25), keys);
        assertHigherKeyIndex(7, rangeStartIncl(16, 25), keys);
        assertHigherKeyIndex(7, rangeEndIncl(20, 25), keys);
        assertHigherKeyIndex(7, rangeStartIncl(20, 25), keys);
    }

    private static void assertLowKeyIndex(int expectedIdx, KeyRange range, Keys keys, int lowerBound, int upperBound)
    {
        if (expectedIdx >= 0 && expectedIdx < keys.size())
        {
            Assertions.assertTrue(range.containsKey(keys.get(expectedIdx)));
        }
        else
        {
            Assertions.assertFalse(range.containsKey(keys.get(lowerBound)));
            Assertions.assertFalse(range.containsKey(keys.get(upperBound - 1)));
        }

        int actualIdx = range.lowKeyIndex(keys, lowerBound, upperBound);
        Assertions.assertEquals(expectedIdx, actualIdx);
    }

    private static void assertLowKeyIndex(int expectedIdx, KeyRange range, Keys keys)
    {

        assertLowKeyIndex(expectedIdx, range, keys, 0, keys.size());
    }

    @Test
    void lowKeyIndexTest()
    {
        Keys keys = keys(10, 11, 12, 13, 14, 15, 16);
        assertLowKeyIndex(-1, rangeEndIncl(0, 5), keys);
        assertLowKeyIndex(-1, rangeStartIncl(0, 5), keys);
        assertLowKeyIndex(-1, rangeEndIncl(0, 9), keys);
        assertLowKeyIndex(-1, rangeStartIncl(0, 9), keys);

        assertLowKeyIndex(0, rangeEndIncl(5, 10), keys);
        assertLowKeyIndex(-1, rangeStartIncl(5, 10), keys);
        assertLowKeyIndex(2, rangeEndIncl(11, 15), keys);
        assertLowKeyIndex(1, rangeStartIncl(11, 15), keys);
        assertLowKeyIndex(3, rangeEndIncl(12, 14), keys);
        assertLowKeyIndex(2, rangeStartIncl(12, 14), keys);
        assertLowKeyIndex(6, rangeEndIncl(15, 20), keys);
        assertLowKeyIndex(5, rangeStartIncl(15, 20), keys);

        assertLowKeyIndex(-8, rangeEndIncl(16, 20), keys);
        assertLowKeyIndex(6, rangeStartIncl(16, 20), keys);
        assertLowKeyIndex(-8, rangeEndIncl(20, 25), keys);
        assertLowKeyIndex(-8, rangeStartIncl(20, 25), keys);

        // non-intersecting
        assertLowKeyIndex(-2, rangeStartIncl(12, 14), keys(10, 16));
        assertLowKeyIndex(-2, rangeStartIncl(12, 14), keys(10, 16, 18), 1, 3);
        assertLowKeyIndex(-3, rangeStartIncl(12, 14), keys(10, 16, 18), 2, 3);
    }

    @Test
    void fullyContainsTest()
    {
        Assertions.assertTrue(r(100, 200).fullyContains(r(100, 200)));
        Assertions.assertTrue(r(100, 200).fullyContains(r(150, 200)));
        Assertions.assertTrue(r(100, 200).fullyContains(r(100, 150)));
        Assertions.assertTrue(r(100, 200).fullyContains(r(125, 175)));

        Assertions.assertFalse(r(100, 200).fullyContains(r(50, 60)));
        Assertions.assertFalse(r(100, 200).fullyContains(r(100, 250)));
        Assertions.assertFalse(r(100, 200).fullyContains(r(150, 250)));
        Assertions.assertFalse(r(100, 200).fullyContains(r(50, 200)));
        Assertions.assertFalse(r(100, 200).fullyContains(r(50, 150)));
        Assertions.assertFalse(r(100, 200).fullyContains(r(250, 260)));
    }

    @Test
    void compareIntersectingTest()
    {
        Assertions.assertEquals(1, r(100, 200).compareIntersecting(r(0, 100)));
        Assertions.assertEquals(1, r(100, 200).compareIntersecting(r(0, 99)));

        Assertions.assertEquals(0, r(100, 200).compareIntersecting(r(0, 101)));

        Assertions.assertEquals(0, r(100, 200).compareIntersecting(r(99, 199)));
        Assertions.assertEquals(0, r(100, 200).compareIntersecting(r(99, 200)));
        Assertions.assertEquals(0, r(100, 200).compareIntersecting(r(99, 201)));
        Assertions.assertEquals(0, r(100, 200).compareIntersecting(r(101, 199)));
        Assertions.assertEquals(0, r(100, 200).compareIntersecting(r(125, 175)));
        Assertions.assertEquals(0, r(100, 200).compareIntersecting(r(100, 201)));
        Assertions.assertEquals(0, r(100, 200).compareIntersecting(r(101, 201)));

        Assertions.assertEquals(0, r(100, 200).compareIntersecting(r(199, 300)));

        Assertions.assertEquals(-1, r(100, 200).compareIntersecting(r(200, 300)));
        Assertions.assertEquals(-1, r(100, 200).compareIntersecting(r(201, 300)));
    }

    private static void assertIntersection(KeyRange<IntKey> expected, KeyRange<IntKey> a, KeyRange<IntKey> b)
    {
        Assertions.assertEquals(expected, a.intersection(b));
        Assertions.assertEquals(expected, b.intersection(a));
    }

    @Test
    void intersectionTest()
    {
        assertIntersection(r(25, 75), r(0, 75), r(25, 100));
        assertIntersection(r(0, 75), r(0, 75), r(0, 100));
        assertIntersection(r(25, 100), r(0, 100), r(25, 100));
        assertIntersection(r(25, 75), r(0, 100), r(25, 75));
        assertIntersection(r(0, 100), r(0, 100), r(0, 100));
    }

    @Test
    void intersectsTest()
    {
        KeyRange range = r(100, 200);
        Assertions.assertTrue(range.intersects(keys(50, 150, 250)));
        Assertions.assertTrue(range.intersects(keys(150, 250)));
        Assertions.assertTrue(range.intersects(keys(50, 150)));

        Assertions.assertFalse(range.intersects(keys()));
        Assertions.assertFalse(range.intersects(keys(50, 75)));
        Assertions.assertFalse(range.intersects(keys(50, 75, 250, 300)));
        Assertions.assertFalse(range.intersects(keys(250, 300)));
    }

    @Test
    void tryMergeTest()
    {
        // touching
        Assertions.assertEquals(r(0, 100), r(0, 50).tryMerge(r(50, 100)));
        Assertions.assertEquals(r(0, 100), r(50, 100).tryMerge(r(0, 50)));

        // intersecting
        Assertions.assertEquals(r(0, 100), r(0, 75).tryMerge(r(25, 100)));
        Assertions.assertEquals(r(0, 100), r(25, 100).tryMerge(r(0, 75)));

        // can't merge
        Assertions.assertNull(r(0, 40).tryMerge(r(60, 100)));
        Assertions.assertNull(r(60, 100).tryMerge(r(0, 40)));
    }
}
