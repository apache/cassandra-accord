package accord.impl.mock;

import accord.api.Data;
import accord.api.Key;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.DataStore;
import accord.api.Update;
import accord.api.Write;
import accord.txn.Keys;
import accord.txn.Timestamp;

public class MockStore implements DataStore
{
    public static final Data DATA = new Data() {
        @Override
        public Data merge(Data data)
        {
            return DATA;
        }
    };

    public static final Result RESULT = new Result() {};
    public static final Query QUERY = data -> RESULT;
    public static final Write WRITE = (key, executeAt, store) -> {};
    public static final Update UPDATE = data -> WRITE;

    public static Read read(Keys keys)
    {
        return new Read()
        {
            @Override
            public Keys keys()
            {
                return keys;
            }

            @Override
            public Data read(Key key, Timestamp executeAt, DataStore store)
            {
                return DATA;
            }
        };
    }
}
