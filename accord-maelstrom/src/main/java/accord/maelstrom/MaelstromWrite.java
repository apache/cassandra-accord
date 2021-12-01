package accord.maelstrom;

import accord.api.Key;
import accord.api.Store;
import accord.api.Write;
import accord.txn.Timestamp;
import accord.utils.Timestamped;

import java.util.TreeMap;

public class MaelstromWrite extends TreeMap<Key, Value> implements Write
{
    @Override
    public void apply(Key key, Timestamp executeAt, Store store)
    {
        MaelstromStore s = (MaelstromStore) store;
        if (containsKey(key))
            s.data.merge(key, new Timestamped<>(executeAt, get(key)), Timestamped::merge);
    }
}
