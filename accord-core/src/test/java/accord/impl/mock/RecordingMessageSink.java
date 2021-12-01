package accord.impl.mock;

import accord.local.Node;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;

import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

public class RecordingMessageSink extends SimpleMessageSink
{
    public static class Envelope<T>
    {
        public final Node.Id to;
        public final T payload;
        public final Callback callback;

        public Envelope(Node.Id to, T payload, Callback callback)
        {
            this.to = to;
            this.payload = payload;
            this.callback = callback;
        }
    }

    public final List<Envelope<Request>> requests = new ArrayList<>();
    public final List<Envelope<Reply>> responses = new ArrayList<>();

    public RecordingMessageSink(Node.Id node, Network network)
    {
        super(node, network);
    }

    @Override
    public void send(Node.Id to, Request request)
    {
        requests.add(new Envelope<>(to, request, null));
        super.send(to, request);
    }

    @Override
    public void send(Node.Id to, Request request, Callback callback)
    {
        requests.add(new Envelope<>(to, request, callback));
        super.send(to, request, callback);
    }

    @Override
    public void reply(Node.Id replyingToNode, ReplyContext replyContext, Reply reply)
    {
        responses.add(new Envelope<>(replyingToNode, reply, null));
        super.reply(replyingToNode, replyContext, reply);
    }

    public void assertHistorySizes(int requests, int responses)
    {
        Assertions.assertEquals(requests, this.requests.size());
        Assertions.assertEquals(responses, this.responses.size());
    }

    public void clearHistory()
    {
        requests.clear();
        responses.clear();
    }
}
