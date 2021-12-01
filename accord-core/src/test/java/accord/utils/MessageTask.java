package accord.utils;

import accord.local.Node;
import accord.messages.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Message task that will continue sending messages to a set of nodes until all
 * nodes ack the message.
 */
public class MessageTask extends AsyncPromise<Void> implements Runnable
{
    public interface NodeProcess
    {
        boolean process(Node node, Node.Id from);
    }

    private static final Reply SUCCESS = new Reply() {
        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "SUCCESS";
        }
    };

    private static final Reply FAILURE = new Reply() {
        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "FAILURE";
        }
    };

    private final Node originator;
    private final List<Node.Id> recipients;
    private final String desc;
    private final Request request;
    private final RetryingCallback callback;

    private class TaskRequest implements Request
    {
        private final NodeProcess process;
        private final String desc;

        public TaskRequest(NodeProcess process, String desc)
        {
            this.process = process;
            this.desc = desc;
        }

        @Override
        public void process(Node on, Node.Id from, ReplyContext replyContext)
        {
            on.reply(from, replyContext, process.process(on, from) ? SUCCESS : FAILURE);
        }

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "TaskRequest{" + desc + '}';
        }
    }

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
                    setSuccess(null);
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
                        String desc, NodeProcess process)
    {
        this.originator = originator;
        this.recipients = ImmutableList.copyOf(recipients);
        this.desc = desc;
        this.request = new TaskRequest(process, desc);
        this.callback = new RetryingCallback(recipients);
    }

    public static MessageTask of(Node originator, Collection<Node.Id> recipients, String desc, NodeProcess process)
    {
        return new MessageTask(originator, new ArrayList<>(recipients), desc, process);
    }

    public static MessageTask begin(Node originator, Collection<Node.Id> recipients, String desc, NodeProcess process)
    {
        MessageTask task = of(originator, recipients, desc, process);
        task.run();
        return task;
    }

    public static MessageTask of(Node originator, Collection<Node.Id> recipients, String desc, Consumer<Node> consumer)
    {
        NodeProcess process = (node, from) -> {
            consumer.accept(node);
            return true;
        };
        return of(originator, recipients, desc, process);
    }

    public static MessageTask apply(Node originator, Collection<Node.Id> recipients, String desc, BiConsumer<Node, Node.Id> consumer)
    {
        NodeProcess process = (node, from) -> {
            consumer.accept(node, from);
            return true;
        };
        MessageTask task = of(originator, recipients, desc, process);
        task.run();
        return task;
    }

    @Override
    public void run()
    {
        originator.send(recipients, request, callback);
    }

    @Override
    public String toString()
    {
        return "MessageTask{" + desc + '}';
    }
}
