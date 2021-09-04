package accord.maelstrom;

import java.io.IOException;

import accord.maelstrom.Packet.Type;
import com.google.gson.stream.JsonWriter;
import accord.local.Node.Id;

public class MaelstromInit extends Body
{
    final Id self;
    final Id[] cluster;

    public MaelstromInit(long msg_id, Id self, Id[] cluster)
    {
        super(Type.init, msg_id, SENTINEL_MSG_ID);
        this.self = self;
        this.cluster = cluster;
    }

    @Override
    void writeBody(JsonWriter out) throws IOException
    {
        super.writeBody(out);
        out.name("node_id");
        out.value(self.id);
        out.name("node_ids");
        out.beginArray();
        for (Id node : cluster)
            out.value(node.id);
        out.endArray();
    }
}
