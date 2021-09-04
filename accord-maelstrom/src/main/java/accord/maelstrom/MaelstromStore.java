package accord.maelstrom;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import accord.api.Key;
import accord.api.Store;
import accord.utils.Timestamped;

public class MaelstromStore implements Store
{
    final Map<Key, Timestamped<Value>> data = new ConcurrentHashMap<>();

    public Value read(Key key)
    {
        Timestamped<Value> v = data.get(key);
        return v == null ? Value.EMPTY : v.data;
    }

    public Value get(Key key)
    {
        Timestamped<Value> v = data.get(key);
        return v == null ? Value.EMPTY : v.data;
    }
}
