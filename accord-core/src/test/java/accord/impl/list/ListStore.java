package accord.impl.list;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import accord.api.Key;
import accord.api.Store;
import accord.utils.Timestamped;

public class ListStore implements Store
{
    static final int[] EMPTY = new int[0];
    final Map<Key, Timestamped<int[]>> data = new ConcurrentHashMap<>();

    public int[] get(Key key)
    {
        Timestamped<int[]> v = data.get(key);
        return v == null ? EMPTY : v.data;
    }
}
