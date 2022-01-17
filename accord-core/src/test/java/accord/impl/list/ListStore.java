package accord.impl.list;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import accord.api.Key;
import accord.local.Node;
import accord.api.DataStore;
import accord.utils.Timestamped;

public class ListStore implements DataStore
{
    static final int[] EMPTY = new int[0];
    final Map<Key, Timestamped<int[]>> data = new ConcurrentHashMap<>();

    // adding here to help trace burn test queries
    public final Node.Id node;

    public ListStore(Node.Id node)
    {
        this.node = node;
    }

    public int[] get(Key key)
    {
        Timestamped<int[]> v = data.get(key);
        return v == null ? EMPTY : v.data;
    }
}
