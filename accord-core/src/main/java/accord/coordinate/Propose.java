package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.topology.Shard;
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
import org.apache.cassandra.utils.concurrent.AsyncFuture;

class Propose implements Callback<AcceptReply>
{
    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final Txn txn;
    final Key homeKey;

    private final List<AcceptOk> acceptOks;
    private final Timestamp executeAt;
    private final QuorumTracker acceptTracker;
    private final BiConsumer<Result, Throwable> callback;
    private boolean isDone;

    Propose(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, BiConsumer<Result, Throwable> callback)
    {
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
        this.callback = callback;
        this.acceptOks = new ArrayList<>();
        this.acceptTracker = new QuorumTracker(topologies);
    }

    public static void propose(Node node, Ballot ballot, TxnId txnId, Txn txn, Key homeKey,
                               Timestamp executeAt, Dependencies dependencies, BiConsumer<Result, Throwable> callback)
    {
        Topologies topologies = node.topology().withUnsyncEpochs(txn, txnId, executeAt);
        propose(node, topologies, ballot, txnId, txn, homeKey, executeAt, dependencies, callback);
    }

    public static void propose(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, Key homeKey,
                               Timestamp executeAt, Dependencies dependencies, BiConsumer<Result, Throwable> callback)
    {
        Propose propose = new Propose(node, topologies, ballot, txnId, txn, homeKey, executeAt, callback);
        node.send(propose.acceptTracker.nodes(), to -> new Accept(to, topologies, ballot, txnId, homeKey, txn, executeAt, dependencies), propose);
    }

    @Override
    public void onSuccess(Id from, AcceptReply reply)
    {
        if (isDone)
            return;

        if (!reply.isOK())
        {
            isDone = true;
            callback.accept(null, new Preempted(txnId, homeKey));
            return;
        }

        AcceptOk ok = (AcceptOk) reply;
        acceptOks.add(ok);
        if (acceptTracker.success(from))
            onAccepted();
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (acceptTracker.failure(from))
        {
            isDone = true;
            callback.accept(null, new Timeout(txnId, homeKey));
        }
    }

    @Override
    public void onCallbackFailure(Throwable failure)
    {
        isDone = true;
        callback.accept(null, failure);
    }

    private void onAccepted()
    {
        isDone = true;
        Dependencies deps = new Dependencies();
        for (AcceptOk acceptOk : acceptOks)
            deps.addAll(acceptOk.deps);

        Execute.execute(node, txnId, txn, homeKey, executeAt, deps, callback);
    }

    // A special version for proposing the invalidation of a transaction; only needs to succeed on one shard
    static class Invalidate extends AsyncFuture<Void> implements Callback<AcceptReply>
    {
        final Node node;
        final Ballot ballot;
        final TxnId txnId;
        final Key someKey;

        private final QuorumShardTracker acceptTracker;

        Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, Key someKey)
        {
            this.node = node;
            this.acceptTracker = new QuorumShardTracker(shard);
            this.ballot = ballot;
            this.txnId = txnId;
            this.someKey = someKey;
        }

        public static Invalidate proposeInvalidate(Node node, Ballot ballot, TxnId txnId, Key someKey)
        {
            Shard shard = node.topology().forEpochIfKnown(someKey, txnId.epoch);
            Invalidate invalidate = new Invalidate(node, shard, ballot, txnId, someKey);
            node.send(shard.nodes, to -> new Accept.Invalidate(ballot, txnId, someKey), invalidate);
            return invalidate;
        }

        @Override
        public void onSuccess(Id from, AcceptReply reply)
        {
            if (isDone())
                return;

            if (!reply.isOK())
            {
                tryFailure(new Preempted(txnId, null));
                return;
            }

            if (acceptTracker.success(from))
                trySuccess(null);
        }

        @Override
        public void onFailure(Id from, Throwable failure)
        {
            if (acceptTracker.failure(from))
                tryFailure(new Timeout(txnId, null));
        }

        @Override
        public void onCallbackFailure(Throwable failure)
        {
            tryFailure(failure);
        }
    }
}
