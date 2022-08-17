package accord.messages;

import accord.local.Node.Id;

/**
 * Represents some execution for handling responses from messages a node has sent.
 * TODO: associate a Callback with a CommandShard or other context for execution (for coordination, usually its home shard)
 */
public interface Callback<T>
{
    void onSuccess(Id from, T response);
    default void onSlowResponse(Id from) {}
    void onFailure(Id from, Throwable failure);
    void onCallbackFailure(Id from, Throwable failure);
}
