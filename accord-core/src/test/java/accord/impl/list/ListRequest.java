package accord.impl.list;

import java.util.function.BiConsumer;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.CheckOnCommitted;
import accord.coordinate.CoordinateFailed;
import accord.impl.basic.Cluster;
import accord.impl.basic.Packet;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.MessageType;
import accord.messages.ReplyContext;
import accord.txn.Txn;
import accord.messages.Request;
import accord.primitives.TxnId;

public class ListRequest implements Request
{
    static class ResultCallback implements BiConsumer<Result, Throwable>
    {
        final Node node;
        final Id client;
        final ReplyContext replyContext;
        final Txn txn;

        ResultCallback(Node node, Id client, ReplyContext replyContext, Txn txn)
        {
            this.node = node;
            this.client = client;
            this.replyContext = replyContext;
            this.txn = txn;
        }

        @Override
        public void accept(Result success, Throwable fail)
        {
            // TODO: error handling
            if (success != null)
            {
                node.reply(client, replyContext, (ListResult) success);
            }
            else if (fail instanceof CoordinateFailed)
            {
                ((Cluster)node.scheduler()).onDone(() -> {
                    Key homeKey = ((CoordinateFailed) fail).homeKey;
                    TxnId txnId = ((CoordinateFailed) fail).txnId;
                    CheckOnCommitted.checkOnCommitted(node, txnId, homeKey, node.topology().forEpoch(homeKey, txnId.epoch), txnId.epoch)
                                    .addCallback((s, f) -> {
                                        if (s.status == Status.Invalidated)
                                            node.reply(client, replyContext, new ListResult(client, ((Packet)replyContext).requestId, null, null, null));
                                    });
                });
            }
        }
    }

    public final Txn txn;

    public ListRequest(Txn txn)
    {
        this.txn = txn;
    }

    public void process(Node node, Id client, ReplyContext replyContext)
    {
        node.coordinate(txn).addCallback(new ResultCallback(node, client, replyContext, txn));
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
