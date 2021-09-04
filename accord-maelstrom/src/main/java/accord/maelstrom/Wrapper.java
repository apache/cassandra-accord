package accord.maelstrom;

import java.io.IOException;

import com.google.gson.stream.JsonWriter;
import accord.maelstrom.Packet.Type;

public class Wrapper extends Body
{
    final Object body;

    public Wrapper(Type type, long msg_id, long in_reply_to, Object body)
    {
        super(type, msg_id, in_reply_to);
        this.body = body;
    }

    @Override
    void writeBody(JsonWriter out) throws IOException
    {
        super.writeBody(out);
        out.name("body");
        Json.GSON.toJson(body, type.type, out);
    }
}
