package accord.maelstrom;

import accord.api.Data;
import accord.api.Key;
import accord.api.Store;
import accord.api.Read;
import accord.txn.Keys;

public class MaelstromRead implements Read
{
    final Keys keys;

    public MaelstromRead(Keys keys)
    {
        this.keys = keys;
    }

    @Override
    public Data read(Key start, Key end, Store store)
    {
        MaelstromStore s = (MaelstromStore)store;
        MaelstromData result = new MaelstromData();
        for (int i = keys.ceilIndex(start), limit = keys.ceilIndex(end) ; i < limit ; ++i)
            result.put(keys.get(i), s.get(keys.get(i)));
        return result;
    }
}
