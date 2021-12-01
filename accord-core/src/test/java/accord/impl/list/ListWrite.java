package accord.impl.list;

import java.util.TreeMap;

import accord.api.Key;
import accord.api.Store;
import accord.api.Write;
import accord.txn.Timestamp;
import accord.utils.Timestamped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListWrite extends TreeMap<Key, int[]> implements Write
{
    private static final Logger logger = LoggerFactory.getLogger(ListWrite.class);
    @Override
    public void apply(Key key, Timestamp executeAt, Store store)
    {
        ListStore s = (ListStore) store;
        if (!containsKey(key))
            return;
        int[] data = get(key);
        s.data.merge(key, new Timestamped<>(executeAt, data), Timestamped::merge);
        logger.trace("WRITE on {} at {} key:{} -> {}", s.node, executeAt, key, data);
    }
}
