package accord.impl.list;

import accord.api.*;
import accord.txn.Keys;

import static java.lang.Math.max;

public class ListRead implements Read
{
    public final Keys keys;

    public ListRead(Keys keys)
    {
        this.keys = keys;
    }

    @Override
    public Data read(KeyRange range, Store store)
    {
        ListStore s = (ListStore)store;
        ListData result = new ListData();
        int lowIdx = range.lowKeyIndex(keys);
        if (lowIdx < 0)
            return result;
        for (int i = lowIdx, limit = range.higherKeyIndex(keys) ; i < limit ; ++i)
            result.put(keys.get(i), s.get(keys.get(i)));
        return result;
    }

    @Override
    public String toString()
    {
        return keys.toString();
    }
}
