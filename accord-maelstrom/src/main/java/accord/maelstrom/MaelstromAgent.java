package accord.maelstrom;

import accord.local.Node;
import accord.api.Agent;
import accord.api.Result;
import accord.local.Command;
import accord.txn.Timestamp;

public class MaelstromAgent implements Agent
{
    static final MaelstromAgent INSTANCE = new MaelstromAgent();

    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        if (success != null)
        {
            MaelstromResult result = (MaelstromResult) success;
            node.reply(result.client, MaelstromReplyContext.contextFor(result.requestId), new MaelstromReply(result.requestId, result));
        }
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        throw new AssertionError();
    }
}
