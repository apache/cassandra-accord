package accord.impl.mock;

import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Request;

public interface Network
{
    void send(Id from, Id to, Request request, Callback callback);
    void reply(Id from, Id replyingToNode, long replyingToMessage, Reply reply);

    Network BLACK_HOLE = new Network()
    {
        @Override
        public void send(Id from, Id to, Request request, Callback callback)
        {
            // TODO: log
        }

        @Override
        public void reply(Id from, Id replyingToNode, long replyingToMessage, Reply reply)
        {
            // TODO: log
        }
    };
}
