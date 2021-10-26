package accord.impl.list;

import accord.api.*;
import accord.topology.KeyRanges;
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
    public Data read(KeyRanges ranges, Store store)
    {
        ListStore s = (ListStore)store;
        ListData result = new ListData();
        for (KeyRange range : ranges)
        {
            int lowIdx = range.lowKeyIndex(keys);
            if (lowIdx < -keys.size())
                return result;
            if (lowIdx < 0)
                continue;
            for (int i = lowIdx, limit = range.higherKeyIndex(keys) ; i < limit ; ++i)
                result.put(keys.get(i), s.get(keys.get(i)));
        }
        return result;
    }

    @Override
    public String toString()
    {
        return keys.toString();
    }
}
