package accord.maelstrom;

import java.util.Map;

import accord.api.Read;
import accord.api.Update;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Data;
import accord.api.Key;
import accord.api.Query;
import accord.api.Result;
import accord.primitives.Keys;

public class MaelstromQuery implements Query
{
    final Node.Id client;
    final long requestId;

    public MaelstromQuery(Id client, long requestId)
    {
        this.client = client;
        this.requestId = requestId;
    }

    @Override
    public Result compute(Data data, Read untypedRead, Update update)
    {
        MaelstromRead read = (MaelstromRead) untypedRead;
        Value[] values = new Value[read.readKeys.size()];
        for (Map.Entry<Key, Value> e : ((MaelstromData)data).entrySet())
            values[read.readKeys.indexOf(e.getKey())] = e.getValue();
        return new MaelstromResult(client, requestId, read.readKeys, values, (MaelstromUpdate) update);
    }
}
