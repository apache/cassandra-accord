package accord.coordinate;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import accord.api.Data;
import accord.coordinate.tracking.ReadTracker;
import accord.api.Result;
import accord.messages.Callback;
import accord.local.Node;
import accord.topology.Topologies;
import accord.txn.Dependencies;
import accord.messages.Apply;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadData.ReadWaiting;
import accord.local.Node.Id;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Keys;
import accord.messages.Commit;
import accord.messages.ReadData;
import accord.messages.ReadData.ReadOk;

class Execute extends CompletableFuture<Result> implements Callback<ReadReply>
{
    final Node node;
    final TxnId txnId;
    final Txn txn;
    final Timestamp executeAt;
    final Topologies topologies;
    final Keys keys;
    final Dependencies deps;
    final ReadTracker tracker;
    private Data data;

    private Execute(Node node, Agreed agreed)
    {
        this.node = node;
        this.txnId = agreed.txnId;
        this.txn = agreed.txn;
        this.keys = txn.keys();
        this.deps = agreed.deps;
        this.executeAt = agreed.executeAt;
        this.topologies = agreed.topologies;
        this.tracker = new ReadTracker(topologies);

        // TODO: perhaps compose these different behaviours differently?
        if (agreed.applied != null)
        {
            node.send(topologies.nodes(),
                      to -> new Apply(to, topologies, txnId, txn, executeAt, agreed.deps, agreed.applied, agreed.result));
            complete(agreed.result);
        }
        else
        {
            Set<Id> readSet = tracker.computeMinimalReadSetAndMarkInflight();
            for (Node.Id to : tracker.nodes())
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
            completeExceptionally(new Preempted());
            return;
        }

        data = data == null ? ((ReadOk) reply).data
                            : data.merge(((ReadOk) reply).data);

        tracker.recordReadSuccess(from);

        if (tracker.hasCompletedRead())
        {
            Result result = txn.result(data);
            // TODO (review): we definitely *dont* want to send this to topologies that are from before the execution epoch,
            //                as they'll be dangling unappliable operations
            node.send(topologies.nodes(), to -> new Apply(to, topologies, txnId, txn, executeAt, deps, txn.execute(executeAt, data), result));
            complete(result);
        }
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        // try again with another random node
        // TODO: API hooks
        if (!(throwable instanceof Timeout))
            throwable.printStackTrace();

        tracker.recordReadFailure(from);
        Set<Id> readFrom = tracker.computeMinimalReadSetAndMarkInflight();
        if (readFrom != null)
        {
            node.send(readFrom, to -> new ReadData(to, topologies, txnId, txn, executeAt), this);
        }
        else if (tracker.hasFailed())
        {
            completeExceptionally(throwable);
        }
    }

    static CompletionStage<Result> execute(Node instance, Agreed agreed)
    {
        return new Execute(instance, agreed);
    }
}
