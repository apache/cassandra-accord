package accord;

import java.util.ArrayList;
import java.util.List;

import accord.api.Key;
import accord.api.KeyRange;
import accord.impl.IntKey;
import accord.topology.KeyRanges;
import accord.txn.Keys;

import org.junit.jupiter.api.Assertions;
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

    @Test
    void foldlTest()
    {
        List<Key> keys = new ArrayList<>();
        long result = keys(150, 250, 350, 450, 550).foldl(ranges(r(200, 400)), (key, p, v) -> { keys.add(key); return v * p + 1; }, 15, 0, -1);
        assertEquals(16, result);
        assertEquals(keys(250, 350), new Keys(keys));

        keys.clear();
        result = keys(150, 250, 350, 450, 550).foldl(ranges(r(0, 500)), (key, p, v) -> { keys.add(key); return v * p + 1; }, 15, 0, -1);
        assertEquals(3616, result);
        assertEquals(keys(150, 250, 350, 450), new Keys(keys));

        keys.clear();
        result = keys(150, 250, 350, 450, 550).foldl(ranges(r(500, 1000)), (key, p, v) -> { keys.add(key); return v * p + 1; }, 15, 0, -1);
        assertEquals(1, result);
        assertEquals(keys(550), new Keys(keys));

        keys.clear();
        result = keys(150, 250, 350, 450, 550).foldl(ranges(r(0, 20), r(100, 140), r(149, 151), r(560, 2000)), (key, p, v) -> { keys.add(key); return v * p + 1; }, 15, 0, -1);
        assertEquals(1, result);
        assertEquals(keys(150), new Keys(keys));
    }

    @Test
    void containsAll()
    {
        Keys keys = keys(150, 200, 250, 300, 350);
        Assertions.assertTrue(keys.containsAll(keys(150, 200)));
        Assertions.assertTrue(keys.containsAll(keys(150, 250)));
        Assertions.assertTrue(keys.containsAll(keys(200, 250)));
        Assertions.assertTrue(keys.containsAll(keys(200, 300)));
        Assertions.assertTrue(keys.containsAll(keys(250, 300)));
        Assertions.assertTrue(keys.containsAll(keys(250, 350)));

        Assertions.assertFalse(keys.containsAll(keys(100, 150)));
        Assertions.assertFalse(keys.containsAll(keys(100, 250)));
        Assertions.assertFalse(keys.containsAll(keys(200, 225)));
        Assertions.assertFalse(keys.containsAll(keys(225, 300)));
        Assertions.assertFalse(keys.containsAll(keys(250, 235)));
        Assertions.assertFalse(keys.containsAll(keys(250, 400)));

    }
}
