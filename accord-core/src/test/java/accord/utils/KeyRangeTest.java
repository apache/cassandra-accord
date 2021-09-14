package accord.utils;

import accord.api.Key;
import accord.api.KeyRange;
import accord.impl.IntKey;
import accord.txn.Keys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KeyRangeTest
{
    static IntKey k(int v)
    {
        return new IntKey(v);
    }

    static KeyRange<IntKey> rangeEndIncl(int start, int end)
    {
        return new KeyRange.EndInclusive<>(k(start), k(end)) {};
    }

    static KeyRange<IntKey> rangeStartIncl(int start, int end)
    {
        return new KeyRange.StartInclusive<>(k(start), k(end)) {};
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

    private static void assertLowKeyIndex(int expectedIdx, KeyRange range, Keys keys)
    {
        if (expectedIdx >= 0 && expectedIdx < keys.size())
        {
            Assertions.assertTrue(range.containsKey(keys.get(expectedIdx)));
        }
        else
        {
            Assertions.assertFalse(range.containsKey(keys.get(0)));
            Assertions.assertFalse(range.containsKey(keys.get(keys.size() - 1)));
        }

        int actualIdx = range.lowKeyIndex(keys);
        Assertions.assertEquals(expectedIdx, actualIdx);
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

        assertLowKeyIndex(7, rangeEndIncl(16, 20), keys);
        assertLowKeyIndex(6, rangeStartIncl(16, 20), keys);
        assertLowKeyIndex(7, rangeEndIncl(20, 25), keys);
        assertLowKeyIndex(7, rangeStartIncl(20, 25), keys);
    }
}
