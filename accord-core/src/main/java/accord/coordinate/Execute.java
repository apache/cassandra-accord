package accord.coordinate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import accord.api.Data;
import accord.messages.Preempted;
import accord.api.Result;
import accord.messages.Callback;
import accord.local.Node;
import accord.txn.Dependencies;
import accord.messages.Apply;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadData.ReadWaiting;
import accord.topology.Shard;
import accord.topology.Shards;
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
    final Shards shards;
    final Keys keys;
    final Dependencies deps;
    final int[] attempts;
    final int[] inFlight;
    final boolean[] hasData;
    private Data data;
    final int replicaIndex;
    int count = 0;

    private Execute(Node node, Agreed agreed)
    {
        this.node = node;
        this.txnId = agreed.txnId;
        this.txn = agreed.txn;
        this.keys = txn.keys();
        this.deps = agreed.deps;
        this.executeAt = agreed.executeAt;
        this.shards = agreed.shards;
        this.attempts = new int[shards.size()];
        this.inFlight = new int[shards.size()];
        this.hasData = new boolean[shards.size()];
        this.replicaIndex = node.random().nextInt(shards.get(0).nodes.size());

        // TODO: perhaps compose these different behaviours differently?
        if (agreed.applied != null)
        {
            Apply send = new Apply(txnId, txn, executeAt, agreed.deps, agreed.applied, agreed.result);
            node.send(shards, send);
            complete(agreed.result);
        }
        else
        {
            // TODO: we're sending duplicate commits
            shards.forEach((i, shard) -> {
                for (int n = 0 ; n < shard.nodes.size() ; ++n)
                {
                    Id to = shard.nodes.get(n);
                    // TODO: Topology needs concept of locality/distance
                    boolean read = n == replicaIndex % shard.nodes.size();
                    Commit send = new Commit(txnId, txn, executeAt, agreed.deps, read);
                    if (read)
                    {
                        node.send(to, send, this);
                        shards.forEachOn(to, (j, s) -> ++inFlight[j]);
                    }
                    else
                    {
                        node.send(to, send);
                    }
                }
            });
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
            // otherwise, try to complete the transaction
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

        shards.forEachOn(from, (i, shard) -> {
            --inFlight[i];
            if (!hasData[i]) ++count;
            hasData[i] = true;
        });

        if (count == shards.size())
        {
            Result result = txn.result(data);
            node.send(shards, new Apply(txnId, txn, executeAt, deps, txn.execute(executeAt, data), result));
            complete(result);
        }
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        // try again with another random node
        // TODO: API hooks
        if (!(throwable instanceof accord.messages.Timeout))
            throwable.printStackTrace();

        shards.forEachOn(from, (i, shard) -> {
            // TODO: less naive selection of replica to consult
            if (--inFlight[i] == 0 && !hasData[i])
                read(i);
            if (inFlight[i] == 0 && !hasData[i])
                completeExceptionally(throwable);
        });
    }

    private void read(int shardIndex)
    {
        Shard shard = shards.get(shardIndex);
        if (attempts[shardIndex] == shard.nodes.size())
            return;

        int nodeIndex = (replicaIndex + attempts[shardIndex]++) % shard.nodes.size();
        Node.Id to = shard.nodes.get(nodeIndex);
        shards.forEachOn(to, (i, s) -> ++inFlight[i]);
        node.send(to, new ReadData(txnId, txn), this);
    }

    static CompletionStage<Result> execute(Node instance, Agreed agreed)
    {
        return new Execute(instance, agreed);
    }
}
