package accord.maelstrom;

import accord.messages.ReplyContext;

public class MaelstromReplyContext implements ReplyContext
{
    public final long messageId;

    public MaelstromReplyContext(long messageId)
    {
        this.messageId = messageId;
    }

    public static ReplyContext contextFor(long messageId)
    {
        return new MaelstromReplyContext(messageId);
    }

    public static long messageIdFor(ReplyContext replyContext)
    {
        if (replyContext instanceof Packet)
            return ((Packet) replyContext).body.msg_id;
        return ((MaelstromReplyContext) replyContext).messageId;
    }
}
