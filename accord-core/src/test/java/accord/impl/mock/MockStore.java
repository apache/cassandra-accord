package accord.impl.mock;

import accord.api.Data;
import accord.api.Key;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.DataStore;
import accord.api.Update;
import accord.api.Write;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.Timestamp;

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
    public static final Query QUERY = (data, read, update) -> RESULT;
    public static final Write WRITE = (key, executeAt, store) -> {};

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

            @Override
            public Read slice(KeyRanges ranges)
            {
                return MockStore.read(keys.slice(ranges));
            }

            @Override
            public Read merge(Read other)
            {
                return MockStore.read(keys.union(other.keys()));
            }

            @Override
            public String toString()
            {
                return keys.toString();
            }
        };
    }

    public static Update update(Keys keys)
    {
        return new Update()
        {
            @Override
            public Keys keys()
            {
                return keys;
            }

            @Override
            public Write apply(Data data)
            {
                return WRITE;
            }

            @Override
            public Update slice(KeyRanges ranges)
            {
                return MockStore.update(keys.slice(ranges));
            }

            @Override
            public Update merge(Update other)
            {
                return MockStore.update(keys.union(other.keys()));
            }

            @Override
            public String toString()
            {
                return keys.toString();
            }
        };
    }
}
