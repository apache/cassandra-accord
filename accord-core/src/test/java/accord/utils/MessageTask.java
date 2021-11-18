package accord.utils;

import accord.local.Node;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Request;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Message task that will continue sending messages to a set of nodes until all
 * nodes ack the message.
 */
public class MessageTask extends CompletableFuture<Void> implements Runnable
{
    public interface NodeProcess
    {
        boolean process(Node node, Node.Id from);
    }

    private static final Reply SUCCESS = new Reply() {};
    private static final Reply FAILURE = new Reply() {};

    private final Node originator;
    private final List<Node.Id> recipients;
    private final Request request;
    private final RetryingCallback callback;

    private class RetryingCallback implements Callback<Reply>
    {
        private final Set<Node.Id> outstanding;

        public RetryingCallback(Collection<Node.Id> outstanding)
        {
            this.outstanding = new HashSet<>(outstanding);
        }

        @Override
        public void onSuccess(Node.Id from, Reply response)
        {
            Preconditions.checkArgument(response == SUCCESS || response == FAILURE);
            if (response == FAILURE)
            {
                originator.send(from, request, this);
                return;
            }

            synchronized (this)
            {
                this.outstanding.remove(from);
                if (outstanding.isEmpty())
                    complete(null);
            }
        }

        @Override
        public void onFailure(Node.Id from, Throwable throwable)
        {
            originator.send(from, request, this);
        }
    }

    private MessageTask(Node originator,
                       List<Node.Id> recipients,
                       NodeProcess process)
    {
        this.originator = originator;
        this.recipients = recipients.stream().filter(id -> !originator.id().equals(id)).collect(Collectors.toList());
        this.request = (on, from, messageId) ->
                on.reply(from, messageId, process.process(on, from) ? SUCCESS : FAILURE);
        this.callback = new RetryingCallback(recipients);
    }

    public static MessageTask of(Node originator, Collection<Node.Id> recipients, NodeProcess process)
    {
        return new MessageTask(originator, new ArrayList<>(recipients), process);
    }

    public static MessageTask begin(Node originator, Collection<Node.Id> recipients, NodeProcess process)
    {
        MessageTask task = of(originator, recipients, process);
        task.run();
        return task;
    }

    public static MessageTask of(Node originator, Collection<Node.Id> recipients, Consumer<Node> consumer)
    {
        NodeProcess process = (node, from) -> {
            consumer.accept(node);
            return true;
        };
        return of(originator, recipients, process);
    }

    public static MessageTask of(Node originator, Collection<Node.Id> recipients, BiConsumer<Node, Node.Id> consumer)
    {
        NodeProcess process = (node, from) -> {
            consumer.accept(node, from);
            return true;
        };
        return of(originator, recipients, process);
    }

    public static MessageTask apply(Node originator, Collection<Node.Id> recipients, BiConsumer<Node, Node.Id> consumer)
    {
        NodeProcess process = (node, from) -> {
            consumer.accept(node, from);
            return true;
        };
        MessageTask task = of(originator, recipients, process);
        task.run();
        return task;
    }


    @Override
    public void run()
    {
        originator.send(recipients, request, callback);
    }
}
