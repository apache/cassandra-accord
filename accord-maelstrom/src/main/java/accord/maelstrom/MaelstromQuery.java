package accord.maelstrom;

import java.util.Map;

import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Data;
import accord.api.Key;
import accord.api.Query;
import accord.api.Result;
import accord.txn.Keys;
import accord.txn.NamespaceKeys;

public class MaelstromQuery implements Query
{
    final Node.Id client;
    final long requestId;
    final Keys read;
    final MaelstromUpdate update; // we have to return the writes as well for some reason

    public MaelstromQuery(Id client, long requestId, Keys read, MaelstromUpdate update)
    {
        this.client = client;
        this.requestId = requestId;
        this.read = read;
        this.update = update;
    }

    @Override
    public Result compute(Data data)
    {
        NamespaceKeys readKeys = read.get(MaelstromKey.NAMESPACE);
        Value[] values = new Value[readKeys.size()];
        for (Map.Entry<Key, Value> e : ((MaelstromData)data).entrySet())
            values[readKeys.indexOf(e.getKey())] = e.getValue();
        return new MaelstromResult(client, requestId, read, values, update);
    }
}
