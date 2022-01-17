package accord.maelstrom;

import accord.api.Key;
import accord.api.DataStore;
import accord.api.Write;
import accord.txn.Timestamp;
import accord.utils.Timestamped;

import java.util.TreeMap;

public class MaelstromWrite extends TreeMap<Key, Value> implements Write
{
    @Override
    public void apply(Key key, Timestamp executeAt, DataStore store)
    {
        MaelstromStore s = (MaelstromStore) store;
        if (containsKey(key))
            s.data.merge(key, new Timestamped<>(executeAt, get(key)), Timestamped::merge);
    }
}
