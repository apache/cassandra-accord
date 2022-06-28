package accord.impl.list;

import accord.api.*;
import accord.txn.Keys;
import accord.txn.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListRead implements Read
{
    private static final Logger logger = LoggerFactory.getLogger(ListRead.class);

    public final Keys keys;

    public ListRead(Keys keys)
    {
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
    public String toString()
    {
        return keys.toString();
    }
}
