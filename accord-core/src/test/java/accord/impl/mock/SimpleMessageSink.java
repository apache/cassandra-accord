package accord.impl.mock;

import accord.local.Node;
import accord.api.MessageSink;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;

public class SimpleMessageSink implements MessageSink
{
    public final Node.Id node;
    public final Network network;

    public SimpleMessageSink(Node.Id node, Network network)
    {
        this.node = node;
        this.network = network;
    }

    @Override
    public void send(Node.Id to, Request request)
    {
        network.send(node, to, request, null);
    }

    @Override
    public void send(Node.Id to, Request request, Callback callback)
    {
        network.send(node, to, request, callback);
    }

    @Override
    public void reply(Node.Id replyingToNode, ReplyContext replyContext, Reply reply)
    {
        network.reply(node, replyingToNode, Network.getMessageId(replyContext), reply);
    }
}
