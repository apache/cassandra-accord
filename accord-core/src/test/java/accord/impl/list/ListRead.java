package accord.impl.list;

import accord.api.Data;
import accord.api.Key;
import accord.api.Store;
import accord.api.Read;
import accord.txn.Keys;

public class ListRead implements Read
{
    public final Keys keys;

    public ListRead(Keys keys)
    {
        this.keys = keys;
    }

    @Override
    public Data read(Key start, Key end, Store store)
    {
        ListStore s = (ListStore)store;
        ListData result = new ListData();
        for (int i = keys.ceilIndex(start), limit = keys.ceilIndex(end) ; i < limit ; ++i)
            result.put(keys.get(i), s.get(keys.get(i)));
        return result;
    }

    @Override
    public String toString()
    {
        return keys.toString();
    }
}
