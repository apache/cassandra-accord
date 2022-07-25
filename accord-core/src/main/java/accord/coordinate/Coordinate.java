package accord.coordinate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.tracking.FastPathTracker;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.txn.Ballot;
import accord.messages.Callback;
import accord.local.Node;
import accord.txn.Dependencies;
import accord.local.Node.Id;
import accord.txn.Timestamp;
import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptOk;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.messages.PreAccept.PreAcceptReply;
import com.google.common.collect.Sets;

import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 */
public class Coordinate extends AsyncFuture<Result> implements BiConsumer<Result, Throwable>
{
    public static Future<Result> coordinate(Node node, TxnId txnId, Txn txn, Key homeKey)
    {
        Coordinate coordinate = new Coordinate();
        Initiate initiate = new Initiate(node, txnId, txn, homeKey, coordinate);
        initiate.start();
        return coordinate;
    }

    @Override
    public void accept(Result success, Throwable failure)
    {
        if (success != null) trySuccess(success);
        else tryFailure(failure);
    }
}
