package accord.txn;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import accord.api.Key;

@SuppressWarnings("rawtypes")
public class Keys implements Iterable<Key>
{
    public static final Keys EMPTY = new Keys(new Key[0]);

    final Key[] keys;

    public Keys(SortedSet<? extends Key> keys)
    {
        this.keys = keys.toArray(Key[]::new);
    }

    public Keys(Collection<? extends Key> keys)
    {
        this.keys = keys.toArray(Key[]::new);
        Arrays.sort(this.keys);
    }

    public Keys(Key[] keys)
    {
        this.keys = keys;
        Arrays.sort(keys);
    }

    public int indexOf(Key key)
    {
        return Arrays.binarySearch(keys, key);
    }

    public Key get(int indexOf)
    {
        return keys[indexOf];
    }

    public Stream<Key> subSet(Key start, boolean isInclusiveStart, Key end, boolean isInclusiveEnd)
    {
        int i = Arrays.binarySearch(keys, start);
        if (i < 0) i = -1 -i;
        else if (!isInclusiveStart) ++i;
        int j = Arrays.binarySearch(keys, end);
        if (j < 0) j = -1 -j;
        else if (isInclusiveEnd) ++j;
        return Arrays.stream(keys, i, j);
    }

    public Keys select(int[] indexes)
    {
        Key[] selection = new Key[indexes.length];
        for (int i = 0 ; i < indexes.length ; ++i)
            selection[i] = keys[indexes[i]];
        return new Keys(selection);
    }

    public int size()
    {
        return keys.length;
    }

    public int ceilIndex(int lowerBound, int upperBound, Key key)
    {
        int i = Arrays.binarySearch(keys, lowerBound, upperBound, key);
        if (i < 0) i = -1 - i;
        return i;
    }

    public int ceilIndex(Key key)
    {
        return ceilIndex(0, keys.length, key);
    }

    public int higherIndex(int lowerBound, int upperBound, Key key)
    {
        int i = Arrays.binarySearch(keys, lowerBound, upperBound, key);
        if (i < 0) i = -1 - i;
        else ++i;
        return i;
    }

    public int higherIndex(Key key)
    {
        return higherIndex(0, keys.length, key);
    }

    public int floorIndex(int lowerBound, int upperBound, Key key)
    {
        int i = Arrays.binarySearch(keys, lowerBound, upperBound, key);
        if (i < 0) i = -2 - i;
        return i;
    }

    public int floorIndex(Key key)
    {
        return floorIndex(0, keys.length, key);
    }

    public int lowerIndex(int lowerBound, int upperBound, Key key)
    {
        int i = Arrays.binarySearch(keys, lowerBound, upperBound, key);
        if (i < 0) i = -2 - i;
        else --i;
        return i;
    }

    public int lowerIndex(Key key)
    {
        return lowerIndex(0, keys.length, key);
    }

    public Stream<Key> stream()
    {
        return Stream.of(keys);
    }

    @Override
    public Iterator<Key> iterator()
    {
        return new Iterator<>()
        {
            int i = 0;
            @Override
            public boolean hasNext()
            {
                return i < keys.length;
            }

            @Override
            public Key next()
            {
                return keys[i++];
            }
        };
    }

    @Override
    public String toString()
    {
        return stream().map(Object::toString).collect(Collectors.joining(",", "[", "]"));
    }

    public static Keys of(Key k0, Key... kn)
    {
        Key[] keys = new Key[kn.length + 1];
        keys[0] = k0;
        for (int i=0; i<kn.length; i++)
            keys[i + 1] = kn[i];

        return new Keys(keys);
    }
}
