package accord.impl.list;

import java.util.Map;
import java.util.TreeMap;

import accord.api.Key;
import accord.api.Store;
import accord.api.Write;
import accord.txn.Timestamp;
import accord.utils.Timestamped;

public class ListWrite extends TreeMap<Key, int[]> implements Write
{
    @Override
    public void apply(Key start, Key end, Timestamp executeAt, Store store)
    {
        ListStore s = (ListStore) store;
        for (Map.Entry<Key, int[]> e : subMap(start, true, end, false).entrySet())
            s.data.merge(e.getKey(), new Timestamped<>(executeAt, e.getValue()), Timestamped::merge);
    }
}
