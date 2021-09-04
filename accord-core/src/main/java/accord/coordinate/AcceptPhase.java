package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import accord.messages.Preempted;
import accord.txn.Ballot;
import accord.messages.Callback;
import accord.local.Node;
import accord.local.Node.Id;
import accord.topology.Shards;
import accord.txn.Timestamp;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.messages.Accept;
import accord.messages.Accept.AcceptOk;
import accord.messages.Accept.AcceptReply;

class AcceptPhase extends CompletableFuture<Agreed>
{
    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final Txn txn;
    final Shards shards;

    private List<AcceptOk> acceptOks;
    private Timestamp proposed;
    private int[] accepts;
    private int[] failures;
    private int acceptQuorums;

    AcceptPhase(Node node, Ballot ballot, TxnId txnId, Txn txn, Shards shards)
    {
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.shards = shards;
    }

    protected void startAccept(Timestamp executeAt, Dependencies deps)
    {
        this.proposed = executeAt;
        this.acceptOks = new ArrayList<>();
        this.accepts = new int[shards.size()];
        this.failures = new int[shards.size()];
        node.send(shards, new Accept(ballot, txnId, txn, executeAt, deps), new Callback<AcceptReply>()
        {
            @Override
            public void onSuccess(Id from, AcceptReply response)
            {
                onAccept(from, response);
            }

            @Override
            public void onFailure(Id from, Throwable throwable)
            {
                shards.forEachOn(from, (i, shard) -> {
                    if (++failures[i] >= shard.slowPathQuorumSize)
                        completeExceptionally(new accord.messages.Timeout());
                });
            }
        });
    }

    private void onAccept(Id from, AcceptReply reply)
    {
        if (isDone())
            return;

        if (!reply.isOK())
        {
            completeExceptionally(new Preempted());
            return;
        }

        AcceptOk ok = (AcceptOk) reply;
        acceptOks.add(ok);
        shards.forEachOn(from, txn.keys(), (i, shard) -> {
            if (++accepts[i] == shard.slowPathQuorumSize)
                ++acceptQuorums;
        });

        if (acceptQuorums == shards.size())
            onAccepted();
    }

    private void onAccepted()
    {
        Dependencies deps = new Dependencies();
        for (AcceptOk acceptOk : acceptOks)
            deps.addAll(acceptOk.deps);
        agreed(proposed, deps);
    }

    protected void agreed(Timestamp executeAt, Dependencies deps)
    {
        complete(new Agreed(txnId, txn, executeAt, deps, shards, null, null));
    }
}
