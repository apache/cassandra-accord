package accord.maelstrom;

import java.io.IOException;

import accord.maelstrom.Packet.Type;
import accord.messages.MessageType;
import com.google.gson.stream.JsonWriter;
import accord.messages.Reply;

public class Error extends Body implements Reply
{
    final int code;
    final String text;

    public Error(long in_reply_to, int code, String text)
    {
        super(Type.error, SENTINEL_MSG_ID, in_reply_to);
        this.code = code;
        this.text = text;
    }

    @Override
    void writeBody(JsonWriter out) throws IOException
    {
        super.writeBody(out);
        out.name("code");
        out.value(code);
        out.name("text");
        out.value(text);
    }

    @Override
    public MessageType type()
    {
        throw new UnsupportedOperationException();
    }
}
