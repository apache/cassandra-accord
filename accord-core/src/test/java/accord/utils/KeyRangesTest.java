package accord.utils;

import accord.topology.KeyRange;
import accord.impl.IntKey;
import accord.topology.KeyRanges;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KeyRangesTest
{
    private static KeyRange<IntKey> r(int start, int end)
    {
        return IntKey.range(start, end);
    }

    private static KeyRanges ranges(KeyRange... ranges)
    {
        return new KeyRanges(ranges);
    }

    @Test
    void rangeIndexForKeyTest()
    {
        KeyRanges ranges = ranges(r(100, 200), r(300, 400));
        Assertions.assertEquals(-1, ranges.rangeIndexForKey(IntKey.key(50)));
        Assertions.assertEquals(0, ranges.rangeIndexForKey(IntKey.key(150)));
        Assertions.assertEquals(-2, ranges.rangeIndexForKey(IntKey.key(250)));
        Assertions.assertEquals(1, ranges.rangeIndexForKey(IntKey.key(350)));
        Assertions.assertEquals(-3, ranges.rangeIndexForKey(IntKey.key(450)));
    }

    @Test
    void differenceTest()
    {
        Assertions.assertEquals(ranges(r(100, 125), r(175, 200)),
                                ranges(r(100, 200)).difference(
                                        ranges(r(125, 175))));
        Assertions.assertEquals(ranges(r(125, 175)),
                                ranges(r(100, 200)).difference(
                                        ranges(r(100, 125), r(175, 200))));
        Assertions.assertEquals(ranges(r(100, 175)),
                                ranges(r(100, 200)).difference(
                                        ranges(r(0, 75), r(175, 200))));
        Assertions.assertEquals(ranges(r(100, 200)),
                                ranges(r(100, 200)).difference(
                                        ranges(r(0, 75), r(200, 205))));

        Assertions.assertEquals(ranges(r(125, 175), r(300, 350)),
                                ranges(r(100, 200), r(250, 350)).difference(
                                        ranges(r(0, 125), r(175, 300))));
        Assertions.assertEquals(ranges(r(125, 200), r(300, 350)),
                                ranges(r(100, 200), r(250, 350)).difference(
                                        ranges(r(0, 125), r(225, 300))));

        Assertions.assertEquals(ranges(r(125, 135), r(140, 160), r(175, 200)),
                                ranges(r(100, 200)).difference(
                                        ranges(r(0, 125), r(135, 140), r(160, 170), r(170, 175))));
    }

    @Test
    void addTest()
    {
        Assertions.assertEquals(ranges(r(0, 50), r(50, 100), r(100, 150), r(150, 200)),
                                ranges(r(0, 50), r(100, 150)).combine(ranges(r(50, 100), r(150, 200))));

        Assertions.assertThrows(IllegalArgumentException.class, () -> ranges(r(0, 50)).combine(ranges(r(25, 75))));
    }

    private static void assertMergeResult(KeyRanges expected, KeyRanges input1, KeyRanges input2)
    {
        Assertions.assertEquals(expected, input1.union(input2));
        Assertions.assertEquals(expected, input2.union(input1));
    }

    @Test
    void mergeTest()
    {
        assertMergeResult(ranges(r(0, 50), r(100, 350)),
                          ranges(r(100, 250), r(300, 350)),
                          ranges(r(0, 50), r(200, 300), r(310, 315)));
        assertMergeResult(ranges(r(0, 100)),
                          KeyRanges.EMPTY,
                          ranges(r(0, 100)));
    }

    @Test
    void mergeTouchingTest()
    {
        Assertions.assertEquals(ranges(r(0, 400)), ranges(r(0, 100), r(100, 200), r(200, 300), r(300, 400)).mergeTouching());
        Assertions.assertEquals(ranges(r(0, 200), r(300, 400)), ranges(r(0, 100), r(100, 200), r(300, 400)).mergeTouching());
        Assertions.assertEquals(ranges(r(0, 100), r(200, 400)), ranges(r(0, 100), r(200, 300), r(300, 400)).mergeTouching());
    }

    @Test
    void selectTest()
    {
        KeyRanges testRanges = ranges(r(0, 100), r(100, 200), r(200, 300), r(300, 400), r(400, 500));
        Assertions.assertEquals(ranges(testRanges.get(1), testRanges.get(3)), testRanges.select(new int[]{1, 3}));
    }

    @Test
    void keyIntersectionTest()
    {
        KeyRanges allRanges = ranges(r(0, 40), r(50, 100), r(100, 150), r(150, 160), r(200, 250), r(250, 300));
        Assertions.assertEquals(ranges(r(50, 100), r(100, 150), r(200, 250)),
                                allRanges.intersection(IntKey.keys(45, 61, 62, 99, 100, 101, 175, 225)));
        Assertions.assertEquals(ranges(r(0, 40)),
                                allRanges.intersection(IntKey.keys(-20, 20, 400)));

    }
}
