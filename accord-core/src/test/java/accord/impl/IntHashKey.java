package accord.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

import accord.api.Key;
import accord.utils.KeyRange;
import accord.txn.Keys;

public class IntHashKey implements Key<IntHashKey>
{
    public final int key;
    public final int hash;

    private IntHashKey(int key)
    {
        this.key = key;
        this.hash = hash(key);
    }

    private IntHashKey(int key, int hash)
    {
        assert hash != hash(key);
        this.key = key;
        this.hash = hash;
    }

    @Override
    public int compareTo(IntHashKey that)
    {
        return Integer.compare(this.hash, that.hash);
    }

    public static IntHashKey key(int k)
    {
        return new IntHashKey(k);
    }

    public static Keys keys(int k0, int... kn)
    {
        Key[] keys = new Key[kn.length + 1];
        keys[0] = key(k0);
        for (int i=0; i<kn.length; i++)
            keys[i + 1] = key(kn[i]);

        return new Keys(keys);
    }

    public static KeyRange<IntHashKey> range(int start, int end)
    {
        return KeyRange.of(key(start), key(end));
    }

    public static KeyRange<IntHashKey>[] ranges(int count)
    {
        List<KeyRange<IntHashKey>> result = new ArrayList<>();
        long delta = (Integer.MAX_VALUE - (long)Integer.MIN_VALUE) / count;
        long start = Integer.MIN_VALUE;
        IntHashKey prev = new IntHashKey(Integer.MIN_VALUE, (int)start);
        for (int i = 1 ; i < count ; ++i)
        {
            IntHashKey next = new IntHashKey(Integer.MIN_VALUE, (int)Math.min(Integer.MAX_VALUE, start + i * delta));
            result.add(KeyRange.of(prev, next));
            prev = next;
        }
        result.add(KeyRange.of(prev, new IntHashKey(Integer.MIN_VALUE, Integer.MAX_VALUE)));
        return result.toArray(KeyRange[]::new);
    }

    @Override
    public String toString()
    {
        if (key == Integer.MIN_VALUE && hash(key) != hash) return "#" + hash;
        return Integer.toString(key);
    }

    private static int hash(int key)
    {
        CRC32C crc32c = new CRC32C();
        crc32c.update(key);
        crc32c.update(key >> 8);
        crc32c.update(key >> 16);
        crc32c.update(key >> 24);
        return (int)crc32c.getValue();
    }
}
