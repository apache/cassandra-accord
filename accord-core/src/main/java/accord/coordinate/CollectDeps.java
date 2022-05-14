package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.GetDeps;
import accord.messages.GetDeps.GetDepsOk;
import accord.primitives.Deps;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

class CollectDeps implements Callback<GetDepsOk>
{
    final Node node;
    final TxnId txnId;
    final Route route;
    final Txn txn;

    final Timestamp executeAt;

    private final List<GetDepsOk> oks;
    private final QuorumTracker tracker;
    private final BiConsumer<Deps, Throwable> callback;
    private boolean isDone;

    CollectDeps(Node node, Topologies topologies, TxnId txnId, Route route, Txn txn, Timestamp executeAt, BiConsumer<Deps, Throwable> callback)
    {
        this.node = node;
        this.txnId = txnId;
        this.route = route;
        this.txn = txn;
        this.executeAt = executeAt;
        this.callback = callback;
        this.oks = new ArrayList<>();
        this.tracker = new QuorumTracker(topologies);
    }

    public static void withDeps(Node node, TxnId txnId, Route route, Txn txn, Timestamp executeAt, BiConsumer<Deps, Throwable> callback)
    {
        Topologies topologies = node.topology().withUnsyncedEpochs(route, txnId, executeAt);
        CollectDeps collect = new CollectDeps(node, topologies, txnId, route, txn, executeAt, callback);
        node.send(collect.tracker.nodes(), to -> new GetDeps(to, topologies, route, txnId, txn, executeAt), collect);
    }

    @Override
    public void onSuccess(Id from, GetDepsOk ok)
    {
        if (isDone)
            return;

        oks.add(ok);
        if (tracker.success(from))
            onQuorum();
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (tracker.failure(from))
        {
            isDone = true;
            callback.accept(null, new Timeout(txnId, route.homeKey));
        }
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        isDone = true;
        callback.accept(null, failure);
    }

    private void onQuorum()
    {
        isDone = true;
        Deps deps = Deps.merge(oks, ok -> ok.deps);
        callback.accept(deps, null);
    }
}
