package accord.maelstrom;

import accord.api.*;
import accord.topology.KeyRanges;
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
    public Data read(KeyRanges ranges, Store store)
    {
        MaelstromStore s = (MaelstromStore)store;
        MaelstromData result = new MaelstromData();
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
}
