package accord.maelstrom;

import accord.api.*;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.Timestamp;

public class MaelstromRead implements Read
{
    final Keys readKeys;
    final Keys keys;

    public MaelstromRead(Keys readKeys, Keys keys)
    {
        this.readKeys = readKeys;
        this.keys = keys;
    }

    @Override
    public Keys keys()
    {
        return keys;
    }

    @Override
    public Data read(Key key, Timestamp executeAt, DataStore store)
    {
        MaelstromStore s = (MaelstromStore)store;
        MaelstromData result = new MaelstromData();
        result.put(key, s.get(key));
        return result;
    }

    @Override
    public Read slice(KeyRanges ranges)
    {
        return new MaelstromRead(readKeys.slice(ranges), keys.slice(ranges));
    }

    @Override
    public Read merge(Read other)
    {
        MaelstromRead that = (MaelstromRead) other;
        Keys readKeys = this.readKeys.union(that.readKeys);
        Keys keys = this.keys.union(that.keys);
        if (readKeys == this.readKeys && keys == this.keys)
            return this;
        if (readKeys == that.readKeys && keys == that.keys)
            return that;
        return new MaelstromRead(readKeys.union(that.readKeys), keys.union(that.keys));
    }
}
