package accord.coordinate;

import java.util.ArrayList;
import java.util.List;

import accord.api.Key;
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
import org.apache.cassandra.utils.concurrent.AsyncPromise;

class Propose extends AsyncPromise<Agreed>
{
    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final Txn txn;
    final Key homeKey;

    private List<AcceptOk> acceptOks;
    private Timestamp proposed;
    private QuorumTracker acceptTracker;

    Propose(Node node, Ballot ballot, TxnId txnId, Txn txn, Key homeKey)
    {
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
    }

    protected void startAccept(Timestamp executeAt, Dependencies deps, Topologies topologies)
    {
        this.proposed = executeAt;
        this.acceptOks = new ArrayList<>();
        this.acceptTracker = new QuorumTracker(topologies);
        // TODO: acceptTracker should be a callback itself, with a reference to us for propagating failure
        node.send(acceptTracker.nodes(), to -> new Accept(to, topologies, ballot, txnId, txn, homeKey, executeAt, deps), new Callback<AcceptReply>()
        {
            @Override
            public void onSuccess(Id from, AcceptReply response)
            {
                onAccept(from, response);
            }

            @Override
            public void onFailure(Id from, Throwable throwable)
            {
                if (acceptTracker.failure(from))
                    tryFailure(new Timeout());
            }
        });
    }

    private void onAccept(Id from, AcceptReply reply)
    {
        if (isDone())
            return;

        if (!reply.isOK())
        {
            tryFailure(new Preempted());
            return;
        }

        AcceptOk ok = (AcceptOk) reply;
        acceptOks.add(ok);
        if (acceptTracker.success(from))
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
        setSuccess(new Agreed(txnId, txn, homeKey, executeAt, deps, null, null));
    }
}
