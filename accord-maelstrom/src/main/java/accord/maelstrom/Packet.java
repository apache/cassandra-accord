package accord.maelstrom;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import accord.messages.*;
import com.google.gson.JsonObject;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import accord.local.Node.Id;

public class Packet implements ReplyContext
{
    public enum Type
    {
        init(MaelstromInit.class, MaelstromInit.GSON_ADAPTER),
        init_ok(Body.class, Body.GSON_ADAPTER),
        txn(MaelstromRequest.class, MaelstromRequest.GSON_ADAPTER),
        txn_ok(MaelstromReply.class, MaelstromReply.GSON_ADAPTER),
        error(Error.class, Error.GSON_ADAPTER),
        PreAccept(accord.messages.PreAccept.class, Json.DEFAULT_ADAPTER),
        PreAcceptOk(accord.messages.PreAccept.PreAcceptOk.class, Json.DEFAULT_ADAPTER),
        PreAcceptNack(accord.messages.PreAccept.PreAcceptNack.class, Json.DEFAULT_ADAPTER),
        Accept(accord.messages.Accept.class, Json.DEFAULT_ADAPTER),
        AcceptOk(accord.messages.Accept.AcceptOk.class, Json.DEFAULT_ADAPTER),
        AcceptNack(accord.messages.Accept.AcceptNack.class, Json.DEFAULT_ADAPTER),
        Commit(accord.messages.Commit.class, Json.DEFAULT_ADAPTER),
        Apply(Apply.class, Json.DEFAULT_ADAPTER),
        Read(ReadData.class, Json.DEFAULT_ADAPTER),
        ReadOk(ReadData.ReadOk.class, Json.DEFAULT_ADAPTER),
        ReadWaiting(ReadData.ReadWaiting.class, Json.DEFAULT_ADAPTER),
        ReadNack(ReadData.ReadNack.class, Json.DEFAULT_ADAPTER),
        WaitOnCommit(accord.messages.WaitOnCommit.class, Json.DEFAULT_ADAPTER),
        WaitOnCommitOk(accord.messages.WaitOnCommit.WaitOnCommitOk.class, Json.DEFAULT_ADAPTER),
        Recover(BeginRecovery.class, Json.DEFAULT_ADAPTER),
        RecoverOk(BeginRecovery.RecoverOk.class, Json.DEFAULT_ADAPTER),
        RecoverNack(BeginRecovery.RecoverNack.class, Json.DEFAULT_ADAPTER);

        public static final Function<Class<?>, Type> LOOKUP = Arrays.stream(Type.values())
                                                                    .filter(t -> t.type != null)
                                                                    .<Map<Class<?>, Type>>collect(HashMap::new, (m, t) -> m.put(t.type, t), Map::putAll)::get;
        public final Class<?> type;
        public final TypeAdapter<?> adapter;

        Type(Class<?> type, TypeAdapter<?> adapter)
        {
            this.type = type;
            this.adapter = adapter;
        }
    }

    final Id src;
    final Id dest;
    final Body body;

    public Packet(Id src, Id dest, Body body)
    {
        this.src = src;
        this.dest = dest;
        this.body = body;
    }

    public Packet(Id src, Id dest, long messageId, Request body)
    {
        this.src = src;
        this.dest = dest;
        this.body = new Wrapper(Type.LOOKUP.apply(body.getClass()), messageId, Body.SENTINEL_MSG_ID, body);
    }

    public Packet(Id src, Id dest, long replyId, Reply body)
    {
        this.src = src;
        this.dest = dest;
        this.body = body instanceof Body ? (Body) body : new Wrapper(Type.LOOKUP.apply(body.getClass()), Body.SENTINEL_MSG_ID, replyId, body);
    }

    public static Packet parse(String str)
    {
        return Json.GSON.fromJson(str, Packet.class);
    }

    public String toString()
    {
        return Json.GSON.toJson(this);
    }

    public static final TypeAdapter<Packet> GSON_ADAPTER = new TypeAdapter<>()
    {
        @Override
        public void write(JsonWriter out, Packet value) throws IOException
        {
            out.beginObject();
            out.name("src");
            out.value(value.src.id);
            out.name("dest");
            out.value(value.dest.id);
            out.name("body");
            Json.GSON.toJson(value.body, Body.class, out);
            out.endObject();
        }

        @Override
        public Packet read(JsonReader in) throws IOException
        {
            in.beginObject();
            Id src = null, dest = null;
            Body body = null;
            String deferredBody = null;
            while (in.hasNext())
            {
                String field = in.nextName();
                switch (field)
                {
                    case "src": src = Json.ID_ADAPTER.read(in); break;
                    case "dest": dest = Json.ID_ADAPTER.read(in); break;
                    case "body":
                        if (src == null) deferredBody = Json.GSON.fromJson(in, JsonObject.class).toString();
                        else body = Body.read(in, src);
                        break;
                    case "id": in.nextLong(); break;
                    default:
                        throw new IllegalStateException("Unexpected field " + field);
                }
            }
            in.endObject();
            if (body == null && deferredBody != null)
                body = Body.read(new JsonReader(new StringReader(deferredBody)), src);
            return new Packet(src, dest, body);
        }
    };

}
