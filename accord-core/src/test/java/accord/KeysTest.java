package accord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import accord.api.Key;
import accord.impl.IntKey;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;

import accord.utils.Gen;
import accord.utils.Gens;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.utils.Property.qt;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KeysTest
{
    private static KeyRange r(int start, int end)
    {
        return IntKey.range(start, end);
    }

    private static KeyRanges ranges(KeyRange... ranges)
    {
        return KeyRanges.of(ranges);
    }

    private static KeyRanges ranges(int a, int b)
    {
        return ranges(range(a, b));
    }

    private static KeyRanges ranges(int a, int b, int c, int d)
    {
        return ranges(range(a, b), range(c, d));
    }

    private static KeyRanges ranges(Keys keys)
    {
        if (keys.isEmpty())
            return KeyRanges.EMPTY;
        if (keys.size() == 1)
            return ranges(Integer.MIN_VALUE, Integer.MAX_VALUE);

        int first = key(keys.get(0));
        List<KeyRange> ranges = new ArrayList<>(keys.size());
        ranges.add(range(first - 1, first));
        for (int i = 1; i < keys.size(); i++)
            ranges.add(range(key(keys.get(i - 1)), key(keys.get(i))));
        return new KeyRanges(ranges);
    }

    private static KeyRanges ranges(Keys keys, Key... include)
    {
        if (keys.isEmpty())
            return KeyRanges.EMPTY;

        return new KeyRanges(IntStream.range(0, include.length)
                .mapToObj(i -> {
                    Key key = include[i];
                    int value = key(key);
                    int idx = keys.indexOf(key);
                    return idx == 0 ? range(value - 1, value) : range(key(keys.get(idx - 1)), value);
                })
                .collect(Collectors.toList()));
    }

    private static int key(Key key)
    {
        return ((IntKey) key).key;
    }

    @Test
    void intersectionTest()
    {
        assertEquals(keys(150, 250),
                     keys(100, 150, 200, 250, 300)
                             .slice(ranges(r(125, 175), r(225, 275))));
        assertEquals(keys(101, 199, 200),
                     keys(99, 100, 101, 199, 200, 201)
                             .slice(ranges(r(100, 200))));
        assertEquals(keys(101, 199, 200, 201, 299, 300),
                     keys(99, 100, 101, 199, 200, 201, 299, 300, 301)
                             .slice(ranges(r(100, 200), r(200, 300))));
    }

    @Test
    void mergeTest()
    {
        assertEquals(keys(0, 1, 2, 3, 4),
                     keys(0, 1, 2, 3, 4).union(keys(0, 1, 2, 3, 4)));
        assertEquals(keys(0, 1, 2, 3, 4),
                     keys(0, 1).union(keys(2, 3, 4)));
        assertEquals(keys(0, 1, 2, 3, 4),
                     keys(0, 2, 4).union(keys(1, 3)));
    }

    @Test
    void foldlTest()
    {
        List<Key> keys = new ArrayList<>();
        long result = keys(150, 250, 350, 450, 550).foldl(ranges(r(200, 400)), (i, key, p, v) -> { keys.add(key); return v * p + 1; }, 15, 0, -1);
        assertEquals(16, result);
        assertEquals(keys(250, 350), new Keys(keys));

        keys.clear();
        result = keys(150, 250, 350, 450, 550).foldl(ranges(r(0, 500)), (i, key, p, v) -> { keys.add(key); return v * p + 1; }, 15, 0, -1);
        assertEquals(3616, result);
        assertEquals(keys(150, 250, 350, 450), new Keys(keys));

        keys.clear();
        result = keys(150, 250, 350, 450, 550).foldl(ranges(r(500, 1000)), (i, key, p, v) -> { keys.add(key); return v * p + 1; }, 15, 0, -1);
        assertEquals(1, result);
        assertEquals(keys(550), new Keys(keys));

        keys.clear();
        result = keys(150, 250, 350, 450, 550).foldl(ranges(r(0, 20), r(100, 140), r(149, 151), r(560, 2000)), (i, key, p, v) -> { keys.add(key); return v * p + 1; }, 15, 0, -1);
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

    @Test
    public void slice()
    {
        qt().forAll(keysGen()).check(list -> {
            Keys keys = new Keys(list);
            // end-inclusive
            int first = list.get(0).key;
            int last = list.get(list.size() - 1).key;
            // exclusive, inclusive
            KeyRange before = IntKey.range(Integer.MIN_VALUE, first - 1);
            KeyRange after = IntKey.range(last, Integer.MAX_VALUE);

            Assertions.assertEquals(Keys.EMPTY, keys.slice(ranges(before, after)));

            // remove from the middle
            for (int i = 1; i < keys.size() - 1; i++)
            {
                IntKey previous = list.get(i - 1);
                IntKey exclude = list.get(i);
                List<IntKey> expected = new ArrayList<>(list);
                expected.remove(exclude);

                KeyRanges allButI = ranges(
                        before,
                        // exclusive, inclusive
                        range(first - 2, previous.key), // use first - 2 to make sure we don't do range(first, first)
                        range(previous.key - 1, exclude.key - 1),
                        range(exclude.key, last),
                        after);
                Assertions.assertEquals(new Keys(expected), keys.slice(allButI), "Expected to exclude " + exclude + " at index " + i);
            }

            // remove the first
            {
                List<IntKey> expected = new ArrayList<>(list);
                expected.remove(IntKey.key(first));

                KeyRanges allButI = ranges(
                        before,
                        // exclusive, inclusive
                        range(first, last),
                        after);
                Assertions.assertEquals(new Keys(expected), keys.slice(allButI), "Expected to exclude " + first + " at index " + 0);
            }
            // remove the last
            {
                List<IntKey> expected = new ArrayList<>(list);
                expected.remove(IntKey.key(last));

                KeyRanges allButI = ranges(
                        before,
                        // exclusive, inclusive
                        range(first - 1, last - 1),
                        range(last, Integer.MAX_VALUE),
                        after);
                Assertions.assertEquals(new Keys(expected), keys.slice(allButI), "Expected to exclude " + first + " at index " + 0);
            }
        });
    }

    @Test
    public void foldl()
    {
        qt().forAll(keysGen()).check(list -> {
            Keys keys = new Keys(list);

            Assertions.assertEquals(keys.size(), keys.foldl(ranges(range(Integer.MIN_VALUE, Integer.MAX_VALUE)), (index, key, accum) -> accum + 1, 0));
            Assertions.assertEquals(keys.size(), keys.foldl(ranges(range(Integer.MIN_VALUE, Integer.MAX_VALUE)), (index, key, ignore, accum) -> accum + 1, -1, 0, Long.MAX_VALUE));
            Assertions.assertEquals(keys.size(), keys.foldl((index, key, ignore, accum) -> accum + 1, -1, 0, Long.MAX_VALUE));

            // early termination
            Assertions.assertEquals(1, keys.foldl(ranges(range(Integer.MIN_VALUE, Integer.MAX_VALUE)), (index, key, ignore, accum) -> accum + 1, -1, 0, 1));
            Assertions.assertEquals(1, keys.foldl((index, key, ignore, accum) -> accum + 1, -1, 0, 1));

            Assertions.assertEquals(keys.size(), keys.foldl(ranges(keys), (index, key, accum) -> accum + 1, 0));
            Assertions.assertEquals(keys.size(), keys.foldl(ranges(keys), (index, key, ignore, accum) -> accum + 1, -1, 0, Long.MAX_VALUE));

            for (Key k : keys)
            {
                Assertions.assertEquals(1, keys.foldl(ranges(keys, k), (index, key, accum) -> accum + 1, 0));
                Assertions.assertEquals(1, keys.foldl(ranges(keys, k), (index, key, ignore, accum) -> accum + 1, -1, 0, Long.MAX_VALUE));
            }
        });
    }

    //TODO test foldlIntersect

    private static Gen<List<IntKey>> keysGen() {
        return Gens.lists(Gens.ints().between(-1000, 1000).map(IntKey::key))
                .unique()
                .ofSizeBetween(2, 40)
                .map(a -> {
                    Collections.sort(a);
                    return a;
                });
    }
}
