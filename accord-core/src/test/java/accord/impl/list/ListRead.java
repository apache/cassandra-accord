package accord.impl.list;

import accord.api.*;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListRead implements Read
{
    private static final Logger logger = LoggerFactory.getLogger(ListRead.class);

    public final Keys readKeys;
    public final Keys keys;

    public ListRead(Keys readKeys, Keys keys)
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
        ListStore s = (ListStore)store;
        ListData result = new ListData();
        int[] data = s.get(key);
        logger.trace("READ on {} at {} key:{} -> {}", s.node, executeAt, key, data);
        result.put(key, data);
        return result;
    }

    @Override
    public Read slice(KeyRanges ranges)
    {
        return new ListRead(readKeys, keys.slice(ranges));
    }

    @Override
    public Read merge(Read other)
    {
        return new ListRead(readKeys, keys.union(((ListRead)other).keys));
    }

    @Override
    public String toString()
    {
        return keys.toString();
    }
}
