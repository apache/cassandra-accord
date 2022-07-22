package accord.impl;

import accord.local.Node;
import accord.api.Agent;
import accord.api.Result;
import accord.local.Command;
import accord.primitives.Timestamp;

public class TestAgent implements Agent
{
    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        // do nothing, intended for use by implementations to decide what to do about recovered transactions
        // specifically if and how they should inform clients of the result
        // e.g. in Maelstrom we send the full result directly, in other impls we may simply acknowledge success via the coordinator
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        throw new AssertionError();
    }

    @Override
    public void onUncaughtException(Throwable t)
    {
    }

    @Override
    public void onHandledException(Throwable t)
    {
    }
}
