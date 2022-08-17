package accord.impl.list;

import java.util.function.Consumer;

import accord.impl.mock.Network;
import accord.local.Node;
import accord.api.Agent;
import accord.api.Result;
import accord.local.Command;
import accord.primitives.Timestamp;
import accord.txn.Txn;

public class ListAgent implements Agent
{
    final Consumer<Throwable> onFailure;
    public ListAgent(Consumer<Throwable> onFailure)
    {
        this.onFailure = onFailure;
    }

    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        if (success != null)
        {
            ListResult result = (ListResult) success;
            node.reply(result.client, Network.replyCtxFor(result.requestId), result);
        }
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        throw new AssertionError("Inconsistent execution timestamp detected for txnId " + command.txnId() + ": " + prev + " != " + next);
    }

    @Override
    public void onUncaughtException(Throwable t)
    {
        // TODO: ensure reported to runner
        onFailure.accept(t);
    }
}
