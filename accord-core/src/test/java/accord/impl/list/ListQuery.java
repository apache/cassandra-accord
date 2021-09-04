package accord.impl.list;

import java.util.Map;

import accord.local.Node.Id;
import accord.api.Data;
import accord.api.Key;
import accord.api.Query;
import accord.api.Result;
import accord.txn.Keys;

public class ListQuery implements Query
{
    final Id client;
    final long requestId;
    final Keys read;
    final ListUpdate update; // we have to return the writes as well for some reason

    public ListQuery(Id client, long requestId, Keys read, ListUpdate update)
    {
        this.client = client;
        this.requestId = requestId;
        this.read = read;
        this.update = update;
    }

    @Override
    public Result compute(Data data)
    {
        int[][] values = new int[read.size()][];
        for (Map.Entry<Key, int[]> e : ((ListData)data).entrySet())
            values[read.indexOf(e.getKey())] = e.getValue();
        return new ListResult(client, requestId, read, values, update);
    }
}
