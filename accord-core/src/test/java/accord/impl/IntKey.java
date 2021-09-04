package accord.impl;

import java.util.ArrayList;
import java.util.List;

import accord.api.Key;
import accord.utils.KeyRange;
import accord.txn.Keys;

public class IntKey implements Key<IntKey>
{
    public final int key;

    private IntKey(int key)
    {
        this.key = key;
    }

    @Override
    public int compareTo(IntKey that)
    {
        return Integer.compare(this.key, that.key);
    }

    public static IntKey key(int k)
    {
        return new IntKey(k);
    }

    public static Keys keys(int k0, int... kn)
    {
        Key[] keys = new Key[kn.length + 1];
        keys[0] = key(k0);
        for (int i=0; i<kn.length; i++)
            keys[i + 1] = key(kn[i]);

        return new Keys(keys);
    }

    public static KeyRange<IntKey> range(int start, int end)
    {
        return KeyRange.of(key(start), key(end));
    }

    public static KeyRange<IntKey>[] ranges(int count)
    {
        List<KeyRange<IntKey>> result = new ArrayList<>();
        long delta = (Integer.MAX_VALUE - (long)Integer.MIN_VALUE) / count;
        long start = Integer.MIN_VALUE;
        IntKey prev = new IntKey((int)start);
        for (int i = 1 ; i < count ; ++i)
        {
            IntKey next = new IntKey((int)Math.min(Integer.MAX_VALUE, start + i * delta));
            result.add(KeyRange.of(prev, next));
            prev = next;
        }
        result.add(KeyRange.of(prev, IntKey.key(Integer.MAX_VALUE)));
        return result.toArray(KeyRange[]::new);
    }

    @Override
    public String toString()
    {
        return Integer.toString(key);
    }
}
