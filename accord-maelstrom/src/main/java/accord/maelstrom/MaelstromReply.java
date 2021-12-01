package accord.maelstrom;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import accord.local.Node;
import accord.api.Key;
import accord.messages.MessageType;
import accord.txn.Keys;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import accord.maelstrom.Packet.Type;
import accord.messages.Reply;

public class MaelstromReply extends Body implements Reply
{
    final MaelstromResult result;

    public MaelstromReply(long in_reply_to, MaelstromResult result)
    {
        super(Type.txn_ok, SENTINEL_MSG_ID, in_reply_to);
        this.result = result;
    }

    @Override
    public MessageType type()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    void writeBody(JsonWriter out) throws IOException
    {
        super.writeBody(out);
        out.name("txn");
        Keys keys = result.keys;
        Value[] reads = result.read;
        MaelstromUpdate update = result.update;
        out.beginArray();
        for (int i = 0 ; i < keys.size() ; ++i)
        {
            MaelstromKey key = (MaelstromKey) keys.get(i);
            if (reads[i] != null)
            {
                out.beginArray();
                out.value("r");
                key.write(out);
                reads[i].writeVerbose(out);
                out.endArray();
            }
            if (update != null && update.containsKey(key))
            {
                for (Datum append : update.get(key).contents)
                {
                    out.beginArray();
                    out.value("append");
                    key.write(out);
                    append.write(out);
                    out.endArray();
                }
            }
        }
        out.endArray();
    }

    public static MaelstromResult readResultExternal(JsonReader in, Node.Id client, long requestId) throws IOException
    {
        if (in.peek() == JsonToken.NULL)
            return null;

        NavigableMap<Key, Value> reads = new TreeMap<>();
        MaelstromUpdate update = new MaelstromUpdate();
        in.beginArray();
        while (in.hasNext())
        {
            in.beginArray();
            String op = in.nextString();
            Key key = MaelstromKey.read(in);
            switch (op)
            {
                default: throw new IllegalStateException("Invalid op: " + op);
                case "r":
                {
                    Value value = Value.read(in);
                    reads.put(key, value);
                    break;
                }
                case "append":
                    Datum value = Datum.read(in);
                    update.merge(key, new Value(value), Value::append);
            }
            in.endArray();
        }
        in.endArray();

        for (Key key : update.keySet())
            reads.putIfAbsent(key, null);

        Keys keys = new Keys(reads.keySet());
        Value[] values = reads.values().toArray(new Value[0]);

        return new MaelstromResult(client, requestId, keys, values, update);
    }

}
