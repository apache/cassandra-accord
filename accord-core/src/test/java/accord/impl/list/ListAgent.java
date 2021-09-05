package accord.impl.list;

import accord.local.Node;
import accord.api.Agent;
import accord.api.Result;
import accord.local.Command;
import accord.txn.Timestamp;

public class ListAgent implements Agent
{
    public static final ListAgent INSTANCE = new ListAgent();

    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        if (success != null)
        {
            ListResult result = (ListResult) success;
            node.reply(result.client, result.requestId, result);
        }
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        throw new AssertionError();
    }
}
