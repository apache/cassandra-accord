package accord.coordinate;

import java.util.Set;

import accord.api.Data;
import accord.coordinate.tracking.ReadTracker;
import accord.api.Result;
import accord.messages.Callback;
import accord.local.Node;
import accord.topology.Topologies;
import accord.txn.*;
import accord.messages.Apply;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadData.ReadWaiting;
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
        this.keys = txn.keys();
        this.deps = agreed.deps;
        this.executeAt = agreed.executeAt;
        this.topologies = node.topology().forTxn(agreed.txn).removeEpochsBefore(agreed.executeAt.epoch);
        this.readTracker = new ReadTracker(topologies.removeEpochsBefore(topologies.currentEpoch()));

        // TODO: perhaps compose these different behaviours differently?
        if (agreed.applied != null)
        {
            node.send(topologies.nodes(),
                      to -> new Apply(to, topologies, txnId, txn, executeAt, agreed.deps, agreed.applied, agreed.result));
            setSuccess(agreed.result);
        }
        else
        {
            Set<Id> readSet = readTracker.computeMinimalReadSetAndMarkInflight();
            for (Node.Id to : readTracker.nodes())
            {
                boolean read = readSet.contains(to);
                Commit send = new Commit(to, topologies, txnId, txn, executeAt, agreed.deps, read);
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
        {
            ReadWaiting waiting = (ReadWaiting) reply;
            // TODO first see if we can collect newer information (from ourselves or others), and if so send it
            //  otherwise, try to complete the transaction
            node.recover(waiting.txnId, waiting.txn);
            return;
        }

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
            Writes writes = txn.execute(executeAt, data);
            node.send(topologies.nodes(), to -> new Apply(to, topologies, txnId, txn, executeAt, deps, writes, result));
            setSuccess(result);
        }
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        // try again with another random node
        // TODO: API hooks
        if (!(throwable instanceof Timeout))
            throwable.printStackTrace();

        readTracker.recordReadFailure(from);
        Set<Id> readFrom = readTracker.computeMinimalReadSetAndMarkInflight();
        if (readFrom != null)
        {
            node.send(readFrom, to -> new ReadData(to, topologies, txnId, txn, executeAt), this);
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
