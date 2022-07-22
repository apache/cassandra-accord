package accord.maelstrom;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import accord.local.Node.Id;
import accord.primitives.Txn;
import accord.maelstrom.Packet.Type;

public class Body
{
    public static final long SENTINEL_MSG_ID = Long.MIN_VALUE;

    final Type type;
    final long msg_id;
    final long in_reply_to;

    public Body(Type type, long msg_id, long in_reply_to)
    {
        this.type = type;
        this.msg_id = msg_id;
        this.in_reply_to = in_reply_to;
    }

    void writeBody(JsonWriter out) throws IOException
    {
        out.name("type");
        out.value(type.name());
        if (msg_id > SENTINEL_MSG_ID)
        {
            out.name("msg_id");
            out.value(msg_id);
        }
        if (in_reply_to > SENTINEL_MSG_ID)
        {
            out.name("in_reply_to");
            out.value(in_reply_to);
        }
    }

    public static final TypeAdapter<Body> GSON_ADAPTER = new TypeAdapter<>()
    {
        @Override
        public void write(JsonWriter out, Body value) throws IOException
        {
            out.beginObject();
            value.writeBody(out);
            out.endObject();
        }

        @Override
        public Body read(JsonReader in) throws IOException
        {
            return Body.read(in, null);
        }
    };

    public static final TypeAdapter<Body> FAIL_READ = new TypeAdapter<>()
    {
        @Override
        public void write(JsonWriter out, Body value) throws IOException
        {
            out.beginObject();
            value.writeBody(out);
            out.endObject();
        }

        @Override
        public Body read(JsonReader in)
        {
            throw new UnsupportedOperationException();
        }
    };

    public static Body read(JsonReader in, Id from) throws IOException
    {
        Type type = null;
        long msg_id = 0, in_reply_to = 0;
        int code = -1;
        String text = null;
        Txn txn = null;
        MaelstromResult txn_ok = null;
        Object body = null;
        Id node_id = null;
        List<Id> node_ids = null;
        String deferredTxn = null;

        in.beginObject();
        while (in.hasNext())
        {
            String field = in.nextName();
            switch (field)
            {
                case "type":
                    String v = in.nextString();
                    type = Type.valueOf(v);
                    break;
                case "msg_id":
                    msg_id = in.nextLong();
                    break;
                case "in_reply_to":
                    in_reply_to = in.nextLong();
                    break;
                case "code":
                    code = in.nextInt();
                    break;
                case "text":
                    text = in.nextString();
                    break;
                case "body":
                    body = Json.GSON.fromJson(in, type.type);
                    break;
                case "txn":
                    if (from == null)
                        throw new IllegalStateException();
                    if (msg_id == 0 || type == null) deferredTxn = Json.GSON.fromJson(in, JsonArray.class).toString();
                    else if (type == Type.txn) txn = MaelstromRequest.readTxnExternal(in, from, msg_id);
                    else txn_ok = MaelstromReply.readResultExternal(in, from, msg_id);
                    break;
                case "node_id":
                    node_id = Json.ID_ADAPTER.read(in);
                    break;
                case "node_ids":
                    node_ids = new ArrayList<>();
                    in.beginArray();
                    while (in.hasNext())
                        node_ids.add(Json.ID_ADAPTER.read(in));
                    in.endArray();
                    break;
                default:
                    throw new IllegalStateException("Unexpected field " + field);
            }
        }
        in.endObject();

        if (deferredTxn != null)
        {
            JsonReader in2 = new JsonReader(new StringReader(deferredTxn));
            if (type == Type.txn) txn = MaelstromRequest.readTxnExternal(in2, from, msg_id);
            else txn_ok = MaelstromReply.readResultExternal(in2, from, msg_id);
        }

        switch (type)
        {
            case init: return new MaelstromInit(msg_id, node_id, node_ids.toArray(Id[]::new));
            case init_ok: return new Body(Type.init_ok, msg_id, in_reply_to);
            case txn: return new MaelstromRequest(msg_id, txn);
            case txn_ok: return new MaelstromReply(in_reply_to, txn_ok);
            case error: return new Error(in_reply_to, code, text);
            default: return new Wrapper(type, msg_id, in_reply_to, body);
        }
    }
}
