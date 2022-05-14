package accord.impl.list;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import accord.local.Node.Id;
import accord.api.Result;
import accord.messages.MessageType;
import accord.primitives.Keys;
import accord.messages.Reply;

public class ListResult implements Result, Reply
{
    public final Id client;
    public final long requestId;
    public final Keys readKeys;
    public final int[][] read; // equal in size to keys.size()
    public final ListUpdate update;

    public ListResult(Id client, long requestId, Keys readKeys, int[][] read, ListUpdate update)
    {
        this.client = client;
        this.requestId = requestId;
        this.readKeys = readKeys;
        this.read = read;
        this.update = update;
    }

    @Override
    public MessageType type()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "{client:" + client + ", "
               + "requestId:" + requestId + ", "
               + (readKeys == null
                  ? "invalidated!}"
                  : "reads:" + IntStream.range(0, readKeys.size())
                                      .mapToObj(i -> readKeys.get(i) + ":" + Arrays.toString(read[i]))
                                      .collect(Collectors.joining(", ", "{", "}")) + ", "
                    + "writes:" + update + "}");
    }
}
