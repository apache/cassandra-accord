package accord.impl.mock;

import accord.api.Data;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Store;
import accord.api.Update;
import accord.api.Write;

public class MockStore implements Store
{
    public static final Data DATA = new Data() {
        @Override
        public Data merge(Data data)
        {
            return DATA;
        }
    };

    public static final Result RESULT = new Result() {};
    public static final Read READ = (key, executeAt, store) -> DATA;
    public static final Query QUERY = data -> RESULT;
    public static final Write WRITE = (key, executeAt, store) -> {};
    public static final Update UPDATE = data -> WRITE;
}
