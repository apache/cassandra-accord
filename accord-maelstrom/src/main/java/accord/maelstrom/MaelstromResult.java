package accord.maelstrom;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import accord.local.Node;
import accord.local.Node.Id;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import accord.api.Key;
import accord.api.Result;
import accord.primitives.Keys;

public class MaelstromResult implements Result
{
    final Node.Id client;
    final long requestId;
    final Keys keys;
    final Value[] read;
    final MaelstromUpdate update;

    public MaelstromResult(Id client, long requestId, Keys keys, Value[] read, MaelstromUpdate update)
    {
        this.client = client;
        this.requestId = requestId;
        this.keys = keys;
        this.read = read;
        this.update = update;
    }

    public static final TypeAdapter<Result> GSON_ADAPTER = new TypeAdapter<>()
    {
        @Override
        public void write(JsonWriter out, Result value) throws IOException
        {
            if (value == null)
            {
                out.nullValue();
                return;
            }

            MaelstromResult result = (MaelstromResult) value;
            Keys keys = result.keys;
            Value[] reads = result.read;
            MaelstromUpdate update = result.update;
            out.beginObject();
            out.name("r");
            out.beginArray();
            for (int i = 0 ; i < keys.size() ; ++i)
            {
                MaelstromKey key = (MaelstromKey) keys.get(i);
                if (reads[i] != null)
                {
                    out.beginArray();
                    key.write(out);
                    reads[i].write(out);
                    out.endArray();
                }
            }
            out.endArray();
            out.name("append");
            out.beginArray();
            for (int i = 0 ; i < keys.size() ; ++i)
            {
                MaelstromKey key = (MaelstromKey) keys.get(i);
                if (update != null && update.containsKey(key))
                {
                    out.beginArray();
                    key.write(out);
                    update.get(key).write(out);
                    out.endArray();
                }
            }
            out.endArray();
            out.name("client");
            out.value(result.client.id);
            out.name("requestId");
            out.value(result.requestId);
            out.endObject();
        }

        @Override
        public Result read(JsonReader in) throws IOException
        {
            if (in.peek() == JsonToken.NULL)
                return null;

            Node.Id client = null;
            long requestId = Long.MIN_VALUE;
            NavigableMap<Key, Value> reads = new TreeMap<>();
            MaelstromUpdate update = new MaelstromUpdate();
            in.beginObject();
            while (in.hasNext())
            {
                String kind = in.nextName();
                switch (kind)
                {
                    default: throw new IllegalStateException("Invalid kind: " + kind);
                    case "r":
                        in.beginArray();
                        while (in.hasNext())
                        {
                            in.beginArray();
                            Key key = MaelstromKey.read(in);
                            Value value = Value.read(in);
                            reads.put(key, value);
                            in.endArray();
                        }
                        in.endArray();
                        break;
                    case "append":
                        in.beginArray();
                        while (in.hasNext())
                        {
                            in.beginArray();
                            Key key = MaelstromKey.read(in);
                            Value append = Value.read(in);
                            update.put(key, append);
                            in.endArray();
                        }
                        in.endArray();
                        break;
                    case "client":
                        client = Json.ID_ADAPTER.read(in);
                        break;
                    case "requestId":
                        requestId = in.nextLong();
                        break;
                }
            }
            in.endObject();

            if (client == null)
                throw new IllegalStateException();

            for (Key key : update.keySet())
                reads.putIfAbsent(key, null);

            Keys keys = new Keys(reads.keySet());
            Value[] values = reads.values().toArray(new Value[0]);
            return new MaelstromResult(client, requestId, keys, values, update);
        }
    };
}
