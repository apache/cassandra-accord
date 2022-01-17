package accord.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import accord.api.Key;
import accord.topology.KeyRange;
import accord.topology.KeyRanges;
import accord.txn.Keys;

public class IntKey implements Key<IntKey>
{
    private static class Range extends KeyRange.EndInclusive<IntKey>
    {
        public Range(IntKey start, IntKey end)
        {
            super(start, end);
        }

        @Override
        public KeyRange<IntKey> subRange(IntKey start, IntKey end)
        {
            return new Range(start, end);
        }
    }

    public final int key;

    public IntKey(int key)
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
        keys[0] = new IntKey(k0);
        for (int i=0; i<kn.length; i++)
            keys[i + 1] = new IntKey(kn[i]);

        return new Keys(keys);
    }

    public static Keys keys(int[] keyArray)
    {
        Key[] keys = new Key[keyArray.length];
        for (int i=0; i<keyArray.length; i++)
            keys[i] = new IntKey(keyArray[i]);

        return new Keys(keys);
    }

    public static KeyRange<IntKey> range(IntKey start, IntKey end)
    {
        return new Range(start, end);
    }

    public static KeyRange<IntKey> range(int start, int end)
    {
        return range(key(start), key(end));
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
            result.add(new Range(prev, next));
            prev = next;
        }
        result.add(new Range(prev, new IntKey(Integer.MAX_VALUE)));
        return result.toArray(KeyRange[]::new);
    }

    @Override
    public String toString()
    {
        return Integer.toString(key);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntKey intKey = (IntKey) o;
        return key == intKey.key;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(key);
    }

    @Override
    public int keyHash()
    {
        return hashCode();
    }
}
