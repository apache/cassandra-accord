package accord.coordinate;

import java.util.Set;

import accord.api.Data;
import accord.api.Key;
import accord.coordinate.tracking.ReadTracker;
import accord.api.Result;
import accord.messages.Callback;
import accord.local.Node;
import accord.topology.Topologies;
import accord.txn.*;
import accord.messages.ReadData.ReadReply;
import accord.txn.Dependencies;
import accord.local.Node.Id;
import accord.messages.Commit;
import accord.messages.ReadData;
import accord.messages.ReadData.ReadOk;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

class Execute extends AsyncPromise<Result> implements Callback<ReadReply>
{
    final Node node;
    final TxnId txnId;
    final Txn txn;
    final Key homeKey;
    final Timestamp executeAt;
    final Topologies topologies;
    final Keys keys;
    final Dependencies deps;
    final ReadTracker readTracker;
    private Data data;

    private Execute(Node node, Agreed agreed)
    {
        this.node = node;
        this.txnId = agreed.txnId;
        this.txn = agreed.txn;
        this.homeKey = agreed.homeKey;
        this.keys = txn.keys();
        this.deps = agreed.deps;
        this.executeAt = agreed.executeAt;
        // TODO (now): why do we removeEpochsBefore rather than do forTxn(agreed.txn, agreed.executeAt.epoch)?
        Topologies coordinationTopologies = node.topology().unsyncForTxn(agreed.txn, agreed.executeAt.epoch);
        Topologies readTopologies = node.topology().unsyncForKeys(agreed.txn.read.keys(), agreed.executeAt.epoch);
        this.readTracker = new ReadTracker(readTopologies);
        this.topologies = coordinationTopologies;

        // TODO: perhaps compose these different behaviours differently?
        if (agreed.applied != null)
        {
            setSuccess(agreed.result);
            Persist.persist(node, topologies, txnId, homeKey, txn, executeAt, deps, agreed.applied, agreed.result);
        }
        else
        {
            Set<Id> readSet = readTracker.computeMinimalReadSetAndMarkInflight();
            for (Node.Id to : coordinationTopologies.nodes())
            {
                boolean read = readSet.contains(to);
                Commit send = new Commit(to, topologies, txnId, txn, homeKey, executeAt, agreed.deps, read);
                if (read)
                {
                    node.send(to, send, this);
                }
                else
                {
                    node.send(to, send);
                }
            }
        }
    }

    @Override
    public void onSuccess(Id from, ReadReply reply)
    {
        if (isDone())
            return;

        if (!reply.isFinal())
            return;

        if (!reply.isOK())
        {
            tryFailure(new Preempted());
            return;
        }

        data = data == null ? ((ReadOk) reply).data
                            : data.merge(((ReadOk) reply).data);

        readTracker.recordReadSuccess(from);

        if (readTracker.hasCompletedRead())
        {
            Result result = txn.result(data);
            setSuccess(result);
            Persist.persist(node, topologies, txnId, homeKey, txn, executeAt, deps, txn.execute(executeAt, data), result);
        }
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        // try again with another random node
        // TODO: API hooks
        if (!(throwable instanceof Timeout))
            throwable.printStackTrace();

        // TODO: introduce two tiers of timeout, one to trigger a retry, and another to mark the original as failed
        // TODO: if we fail, nominate another coordinator from the homeKey shard to try
        readTracker.recordReadFailure(from);
        Set<Id> readFrom = readTracker.computeMinimalReadSetAndMarkInflight();
        if (readFrom != null)
        {
            node.send(readFrom, to -> new ReadData(to, readTracker.topologies(), txnId, txn, homeKey, executeAt), this);
        }
        else if (readTracker.hasFailed())
        {
            tryFailure(throwable);
        }
    }

    static Future<Result> execute(Node instance, Agreed agreed)
    {
        return new Execute(instance, agreed);
    }
}
