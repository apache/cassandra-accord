package accord.impl.basic;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import accord.coordinate.Timeout;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.MessageSink;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;

import static accord.impl.basic.Packet.SENTINEL_MESSAGE_ID;

public class NodeSink implements MessageSink
{
    final Id self;
    final Function<Id, Node> lookup;
    final Cluster parent;
    final Random random;

    int nextMessageId = 0;
    Map<Long, Callback> callbacks = new LinkedHashMap<>();

    public NodeSink(Id self, Function<Id, Node> lookup, Cluster parent, Random random)
    {
        this.self = self;
        this.lookup = lookup;
        this.parent = parent;
        this.random = random;
    }

    @Override
    public synchronized void send(Id to, Request send)
    {
        parent.add(self, to, SENTINEL_MESSAGE_ID, send);
    }

    @Override
    public void send(Id to, Request send, Callback callback)
    {
        long messageId = nextMessageId++;
        callbacks.put(messageId, callback);
        parent.add(self, to, messageId, send);
        parent.pending.add((PendingRunnable) () -> {
            if (callback == callbacks.get(messageId))
            {
                try
                {
                    callback.onSlowResponse(to);
                }
                catch (Throwable t)
                {
                    callback.onCallbackFailure(t);
                    lookup.apply(self).agent().onUncaughtException(t);
                }

            }
        }, 100 + random.nextInt(200), TimeUnit.MILLISECONDS);
        parent.pending.add((PendingRunnable) () -> {
            if (callback == callbacks.remove(messageId))
            {
                try
                {
                    callback.onFailure(to, new Timeout(null, null));
                }
                catch (Throwable t)
                {
                    callback.onCallbackFailure(t);
                    lookup.apply(self).agent().onUncaughtException(t);
                }
            }
        }, 1000 + random.nextInt(10000), TimeUnit.MILLISECONDS);
    }

    @Override
    public void reply(Id replyToNode, ReplyContext replyContext, Reply reply)
    {
        parent.add(self, replyToNode, Packet.getMessageId(replyContext), reply);
    }
}
