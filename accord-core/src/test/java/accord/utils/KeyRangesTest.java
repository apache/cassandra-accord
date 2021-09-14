package accord.utils;

import accord.api.KeyRange;
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
}
