package accord.messages;

public enum SimpleReply implements Reply
{
    Ok, InProgress, Nack;

    @Override
    public boolean isFinal()
    {
        return this != InProgress;
    }

    @Override
    public MessageType type()
    {
        return MessageType.SIMPLE_RSP;
    }

}
