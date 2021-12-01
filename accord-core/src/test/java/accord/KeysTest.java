package accord;

import accord.api.KeyRange;
import accord.impl.IntKey;
import accord.topology.KeyRanges;
import org.junit.jupiter.api.Test;

import static accord.impl.IntKey.keys;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KeysTest
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
    void intersectionTest()
    {
        assertEquals(keys(150, 250),
                     keys(100, 150, 200, 250, 300)
                             .intersection(ranges(r(125, 175), r(225, 275))));
        assertEquals(keys(101, 199, 200),
                     keys(99, 100, 101, 199, 200, 201)
                             .intersection(ranges(r(100, 200))));
        assertEquals(keys(101, 199, 200, 201, 299, 300),
                     keys(99, 100, 101, 199, 200, 201, 299, 300, 301)
                             .intersection(ranges(r(100, 200), r(200, 300))));
    }

    @Test
    void mergeTest()
    {
        assertEquals(keys(0, 1, 2, 3, 4),
                     keys(0, 1, 2, 3, 4).merge(keys(0, 1, 2, 3, 4)));
        assertEquals(keys(0, 1, 2, 3, 4),
                     keys(0, 1).merge(keys(2, 3, 4)));
        assertEquals(keys(0, 1, 2, 3, 4),
                     keys(0, 2, 4).merge(keys(1, 3)));
    }
}
