package accord.maelstrom;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;

import accord.api.Key;
import accord.messages.MessageType;
import accord.messages.ReplyContext;
import accord.txn.Keys;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import accord.local.Node;
import accord.local.Node.Id;
import accord.txn.Txn;
import accord.maelstrom.Packet.Type;
import accord.messages.Request;

public class MaelstromRequest extends Body implements Request
{
    final Txn txn;

    public MaelstromRequest(long msg_id, Txn txn)
    {
        super(Type.txn, msg_id, SENTINEL_MSG_ID);
        this.txn = txn;
    }

    public void process(Node node, Id client, ReplyContext replyContext)
    {
        // TODO (now): error handling
        node.coordinate(txn).addCallback((success, fail) -> {
            if (success != null) node.reply(client, replyContext, new MaelstromReply(MaelstromReplyContext.messageIdFor(replyContext), (MaelstromResult) success));
//            else node.reply(client, messageId, new Error(messageId, 13, fail.getMessage()));
        });
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
        writeTxnExternal(out, txn);
    }

    static void writeTxnExternal(JsonWriter out, Txn txn) throws IOException
    {
        if (txn == null)
        {
            out.nullValue();
            return;
        }

        out.beginArray();
        Keys keys = txn.keys;
        MaelstromQuery query = (MaelstromQuery) txn.query;
        MaelstromUpdate update = (MaelstromUpdate) txn.update;
        for (int i = 0 ; i < keys.size() ; ++i)
        {
            MaelstromKey key = (MaelstromKey) keys.get(i);
            if (query.read.indexOf(key) >= 0)
            {
                out.beginArray();
                out.value("r");
                key.write(out);
                out.nullValue();
                out.endArray();
            }
            if (update.containsKey(key))
            {
                out.beginArray();
                out.value("append");
                key.write(out);
                update.get(key).write(out);
                out.endArray();
            }
        }
        out.endArray();
    }

    public static Txn readTxnExternal(JsonReader in, Node.Id client, long requestId) throws IOException
    {
        if (in.peek() == JsonToken.NULL)
            return null;

        NavigableSet<Key> buildReadKeys = new TreeSet<>();
        NavigableSet<Key> buildKeys = new TreeSet<>();
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
                    in.nextNull();
                    buildReadKeys.add(key);
                    break;
                case "append":
                    Datum value = Datum.read(in);
                    buildKeys.add(key);
                    update.merge(key, new Value(value), Value::append);
            }
            in.endArray();
        }
        in.endArray();

        buildKeys.addAll(buildReadKeys);
        Keys readKeys = new Keys(buildReadKeys);
        Keys keys = new Keys(buildKeys);
        MaelstromRead read = new MaelstromRead(keys);
        MaelstromQuery query = new MaelstromQuery(client, requestId, readKeys, update);

        return new Txn(keys, read, query, update);
    }

}
