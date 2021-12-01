package accord.impl.list;

import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.MessageType;
import accord.messages.ReplyContext;
import accord.txn.Txn;
import accord.messages.Request;

public class ListRequest implements Request
{
    public final Txn txn;

    public ListRequest(Txn txn)
    {
        this.txn = txn;
    }

    public void process(Node node, Id client, ReplyContext replyContext)
    {
        // TODO (now): error handling
        node.coordinate(txn).addCallback((success, fail) -> {
            if (success != null)
                node.reply(client, replyContext, (ListResult) success);
        });
    }

    @Override
    public MessageType type()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return txn.toString();
    }
}
