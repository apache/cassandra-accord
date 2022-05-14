package accord.messages;

import java.util.IdentityHashMap;
import java.util.function.Function;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Listener;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;

import static accord.messages.Defer.Ready.Expired;
import static accord.messages.Defer.Ready.No;
import static accord.messages.Defer.Ready.Yes;

// TODO: use something more efficient? could probably assign each CommandStore a unique ascending integer and use an int[]
class Defer extends IdentityHashMap<CommandStore, Boolean> implements Listener
{
    public enum Ready { No, Yes, Expired }

    final Function<Command, Ready> waitUntil;
    final Request request;
    final Node node;
    final Node.Id replyToNode;
    final ReplyContext replyContext;
    boolean isDone;

    Defer(Status waitUntil, Request request, Node node, Id replyToNode, ReplyContext replyContext)
    {
        this(command -> {
            int c = command.status().logicalCompareTo(waitUntil);
            if (c < 0) return No;
            if (c > 0) return Expired;
            return Yes;
        }, request, node, replyToNode, replyContext);
    }

    Defer(Function<Command, Ready> waitUntil, Request request, Node node, Id replyToNode, ReplyContext replyContext)
    {
        this.waitUntil = waitUntil;
        this.request = request;
        this.node = node;
        this.replyToNode = replyToNode;
        this.replyContext = replyContext;
    }

    void add(Command command, CommandStore commandStore)
    {
        if (isDone)
            throw new IllegalStateException("Recurrent retry of " + request);

        put(commandStore, Boolean.TRUE);
        command.addListener(this);
    }

    @Override
    public void onChange(Command command)
    {
        Ready ready = waitUntil.apply(command);
        if (ready == No) return;
        command.removeListener(this);
        if (ready == Expired) return;

        remove(command.commandStore);
        if (isEmpty())
        {
            isDone = true;
            request.process(node, replyToNode, replyContext);
        }
    }
}

