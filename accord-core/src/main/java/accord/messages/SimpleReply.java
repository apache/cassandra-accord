package accord.messages;

public enum SimpleReply implements Reply
{
    Ok, Nack;

    @Override
    public MessageType type()
    {
        return MessageType.SIMPLE_RSP;
    }
}
