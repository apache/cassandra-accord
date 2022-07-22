package accord.impl.list;

import java.util.function.BiConsumer;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.CheckShards;
import accord.coordinate.CoordinateFailed;
import accord.coordinate.Invalidate;
import accord.impl.basic.Cluster;
import accord.impl.basic.Packet;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.messages.MessageType;
import accord.messages.ReplyContext;
import accord.primitives.RoutingKeys;
import accord.primitives.Txn;
import accord.messages.Request;
import accord.primitives.TxnId;

import static accord.local.Status.Executed;

public class ListRequest implements Request
{
    enum Outcome { Invalidated, Lost, Neither }

    static class CheckOnResult extends CheckShards
    {
        final BiConsumer<Outcome, Throwable> callback;
        int count = 0;
        protected CheckOnResult(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Outcome, Throwable> callback)
        {
            super(node, txnId, RoutingKeys.of(homeKey), txnId.epoch, IncludeInfo.All);
            this.callback = callback;
        }

        static void checkOnResult(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Outcome, Throwable> callback)
        {
            CheckOnResult result = new CheckOnResult(node, txnId, homeKey, callback);
            result.start();
        }

        @Override
        protected Action check(Id from, CheckStatusOk ok)
        {
            ++count;
            return ok.status.compareTo(Executed) >= 0 ? Action.Accept : Action.Reject;
        }

        @Override
        protected void onDone(Done done, Throwable failure)
        {
            super.onDone(done, failure);
            if (failure != null) callback.accept(null, failure);
            else if (merged.status.hasBeen(Status.Invalidated)) callback.accept(Outcome.Invalidated, null);
            else if (count == nodes().size()) callback.accept(Outcome.Lost, null);
            else callback.accept(Outcome.Neither, null);
        }

        @Override
        protected boolean isSufficient(Id from, CheckStatusOk ok)
        {
            throw new UnsupportedOperationException();
        }
    }

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
                    RoutingKey homeKey = ((CoordinateFailed) fail).homeKey;
                    TxnId txnId = ((CoordinateFailed) fail).txnId;
                    CheckOnResult.checkOnResult(node, txnId, homeKey, (s, f) -> {
                        switch (s)
                        {
                            case Invalidated:
                                node.reply(client, replyContext, new ListResult(client, ((Packet)replyContext).requestId, null, null, null));
                                break;
                            case Lost:
                                node.reply(client, replyContext, new ListResult(client, ((Packet)replyContext).requestId, null, new int[0][], null));
                                break;
                            case Neither:
                                // currently caught elsewhere in response tracking, but might help to throw an exception here
                        }
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
