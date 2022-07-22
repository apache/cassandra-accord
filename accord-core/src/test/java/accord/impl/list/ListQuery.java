package accord.impl.list;

import java.util.Map;

import accord.api.Read;
import accord.api.Update;
import accord.local.Node.Id;
import accord.api.Data;
import accord.api.Key;
import accord.api.Query;
import accord.api.Result;

public class ListQuery implements Query
{
    final Id client;
    final long requestId;

    public ListQuery(Id client, long requestId)
    {
        this.client = client;
        this.requestId = requestId;
    }

    @Override
    public Result compute(Data data, Read untypedRead, Update update)
    {
        ListRead read = (ListRead) untypedRead;
        int[][] values = new int[read.readKeys.size()][];
        for (Map.Entry<Key, int[]> e : ((ListData)data).entrySet())
        {
            int i = read.readKeys.indexOf(e.getKey());
            if (i >= 0)
                values[i] = e.getValue();
        }
        return new ListResult(client, requestId, read.readKeys, values, (ListUpdate) update);
    }
}
