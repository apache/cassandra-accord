package accord.api;

import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Request;

public interface MessageSink
{
    void send(Id to, Request request);
    void send(Id to, Request request, Callback callback);
    void reply(Id replyingToNode, long replyingToMessage, Reply reply);
}
