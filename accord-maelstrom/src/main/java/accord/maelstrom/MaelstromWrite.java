package accord.maelstrom;

import java.util.Map;
import java.util.TreeMap;

import accord.api.Key;
import accord.api.Store;
import accord.api.Write;
import accord.txn.Timestamp;
import accord.utils.Timestamped;

public class MaelstromWrite extends TreeMap<Key, Value> implements Write
{
    @Override
    public void apply(Key start, Key end, Timestamp executeAt, Store store)
    {
        MaelstromStore s = (MaelstromStore) store;
        for (Map.Entry<Key, Value> e : subMap(start, true, end, false).entrySet())
            s.data.merge(e.getKey(), new Timestamped<>(executeAt, e.getValue()), Timestamped::merge);
    }
}
