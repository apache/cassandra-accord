package accord.primitives;

import java.util.*;
import java.util.function.IntFunction;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.utils.SortedArrays;

public class Keys extends AbstractKeys<Key, Keys>
{
    public static final Keys EMPTY = new Keys(new Key[0]);

    public Keys(SortedSet<? extends Key> keys)
    {
        this(keys.toArray(Key[]::new));
    }

    public Keys(Collection<? extends Key> keys)
    {
        this(sort(keys.toArray(Key[]::new)));
    }

    public Keys(Key[] keys)
    {
        super(keys);
    }

    public Keys union(Keys that)
    {
        return wrap(SortedArrays.linearUnion(keys, that.keys, factory()), that);
    }

    public Keys slice(KeyRanges ranges)
    {
        return wrap(slice(ranges, factory()));
    }

    private Keys wrap(Key[] wrap, Keys that)
    {
        return wrap == keys ? this : wrap == that.keys ? that : new Keys(wrap);
    }

    private Keys wrap(Key[] wrap)
    {
        return wrap == keys ? this : new Keys(wrap);
    }

    private static IntFunction<Key[]> factory()
    {
        return Key[]::new;
    }

    public static Keys of(Key key)
    {
        return new Keys(new Key[] { key });
    }

    public static Keys of(Key ... keys)
    {
        return new Keys(sort(keys));
    }

    private static Key[] sort(Key[] array)
    {
        Arrays.sort(array);
        return array;
    }
}
