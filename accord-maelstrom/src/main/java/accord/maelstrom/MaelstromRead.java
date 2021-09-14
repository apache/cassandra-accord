package accord.maelstrom;

import accord.api.*;
import accord.txn.Keys;

import static java.lang.Math.max;

public class MaelstromRead implements Read
{
    final Keys keys;

    public MaelstromRead(Keys keys)
    {
        this.keys = keys;
    }

    @Override
    public Data read(KeyRange range, Store store)
    {
        MaelstromStore s = (MaelstromStore)store;
        MaelstromData result = new MaelstromData();
        int lowIdx = range.lowKeyIndex(keys);
        if (lowIdx < 0)
            return result;
        for (int i = lowIdx, limit = range.higherKeyIndex(keys) ; i < limit ; ++i)
            result.put(keys.get(i), s.get(keys.get(i)));
        return result;
    }
}
