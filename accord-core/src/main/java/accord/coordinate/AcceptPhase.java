package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import accord.coordinate.tracking.QuorumTracker;
import accord.topology.Topologies;
import accord.txn.Ballot;
import accord.messages.Callback;
import accord.local.Node;
import accord.local.Node.Id;
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
    final Topologies topologies; // TODO: remove, hide in participants

    private List<AcceptOk> acceptOks;
    private Timestamp proposed;
    private QuorumTracker acceptTracker;

    AcceptPhase(Node node, Ballot ballot, TxnId txnId, Txn txn, Topologies topologies)
    {
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.topologies = topologies;
    }

    protected void startAccept(Timestamp executeAt, Dependencies deps)
    {
        this.proposed = executeAt;
        this.acceptOks = new ArrayList<>();
        this.acceptTracker = new QuorumTracker(topologies);
        node.send(acceptTracker.nodes(), to -> new Accept(to, topologies, ballot, txnId, txn, executeAt, deps), new Callback<AcceptReply>()
        {
            @Override
            public void onSuccess(Id from, AcceptReply response)
            {
                onAccept(from, response);
            }

            @Override
            public void onFailure(Id from, Throwable throwable)
            {
                acceptTracker.recordFailure(from);
                if (acceptTracker.hasFailed())
                    completeExceptionally(new Timeout());
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
        acceptTracker.recordSuccess(from);

        if (acceptTracker.hasReachedQuorum())
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
        complete(new Agreed(txnId, txn, executeAt, deps, topologies, null, null));
    }
}
