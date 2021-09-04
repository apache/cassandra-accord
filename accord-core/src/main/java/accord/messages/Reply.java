package accord.messages;

public interface Reply extends Message
{
    default boolean isFinal() { return true; }
}
