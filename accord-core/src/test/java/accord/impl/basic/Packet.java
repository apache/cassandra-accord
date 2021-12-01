package accord.impl.basic;

import accord.impl.mock.Network;
import accord.local.Node.Id;
import accord.messages.Message;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;

public class Packet implements Pending, ReplyContext
{
    static final int SENTINEL_MESSAGE_ID = Integer.MIN_VALUE;

    public final Id src;
    public final Id dst;
    public final long requestId; // if message is Reply, this is the id of the message we are replying to
    public final long replyId; // if message is Reply, this is the id of the message we are replying to
    public final Message message;

    public Packet(Id src, Id dst, long requestId, Request request)
    {
        this.src = src;
        this.dst = dst;
        this.requestId = requestId;
        this.replyId = SENTINEL_MESSAGE_ID;
        this.message = request;
    }

    public Packet(Id src, Id dst, long replyId, Reply reply)
    {
        this.src = src;
        this.dst = dst;
        this.requestId = SENTINEL_MESSAGE_ID;
        this.replyId = replyId;
        this.message = reply;
    }

    @Override
    public String toString()
    {
        return "{from:" + src + ", "
        + "to:" + dst + ", "
        + (requestId != SENTINEL_MESSAGE_ID ? "id:" + requestId + ", " : "")
        + (replyId != SENTINEL_MESSAGE_ID ? "replyTo:" + replyId + ", " : "")
        + "body:" + message + "}";
    }

    public static long getMessageId(ReplyContext context)
    {
        if (context instanceof Network.MessageId)
            return ((Network.MessageId) context).msgId;
        return ((Packet) context).requestId;
    }
}
