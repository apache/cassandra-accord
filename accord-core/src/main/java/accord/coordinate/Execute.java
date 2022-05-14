package accord.coordinate;

import java.util.Set;
import java.util.function.BiConsumer;

import accord.api.Data;
import accord.api.Result;
import accord.local.Node;
import accord.messages.ReadData.ReadNack;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.messages.ReadData.ReadReply;
import accord.primitives.Deps;
import accord.local.Node.Id;
import accord.messages.Commit;
import accord.messages.ReadData;
import accord.messages.ReadData.ReadOk;
import accord.topology.Topology;

import static accord.coordinate.AnyReadCoordinator.Action.Accept;
import static accord.messages.Commit.Kind.Maximal;

class Execute extends AnyReadCoordinator<ReadReply>
{
    final Txn txn;
    final Route route;
    final Timestamp executeAt;
    final Deps deps;
    final Topologies applyTo;
    final BiConsumer<Result, Throwable> callback;
    private Data data;

    private Execute(Node node, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps, BiConsumer<Result, Throwable> callback)
    {
        super(node, node.topology().forEpoch(txn.read.keys(), executeAt.epoch), txnId);
        this.txn = txn;
        this.route = route;
        this.executeAt = executeAt;
        this.deps = deps;
        this.applyTo = node.topology().forEpoch(route, executeAt.epoch);
        this.callback = callback;
    }

    public static void execute(Node node, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps, BiConsumer<Result, Throwable> callback)
    {
        Execute execute = new Execute(node, txnId, txn, route, executeAt, deps, callback);
        execute.start();
    }

    @Override
    void start(Set<Id> readSet)
    {
        Commit.commitMinimalAndRead(node, applyTo, txnId, txn, route, executeAt, deps, readSet, this);
    }

    @Override
    void contact(Set<Id> nodes)
    {
        node.send(nodes, to -> new ReadData(to, tracker.topologies(), txnId, route, executeAt), this);
    }

    @Override
    Action process(Id from, ReadReply reply)
    {
        if (reply.isOk())
        {
            data = data == null ? ((ReadOk) reply).data
                                : data.merge(((ReadOk) reply).data);
            return Accept;
        }

        ReadNack nack = (ReadNack) reply;
        switch (nack)
        {
            default: throw new IllegalStateException();
            case Redundant:
                callback.accept(null, new Preempted(txnId, route.homeKey));
                return Action.Abort;
            case NotCommitted:
                // we might be missing the original commit, or the additional commit, so send everything
                Topologies topology = node.topology().preciseEpochs(route, txnId.epoch, executeAt.epoch);
                Topology coordinateTopology = topology.forEpoch(txnId.epoch);
                node.send(from, new Commit(Maximal, from, coordinateTopology, topology, txnId, txn, route, executeAt, deps, false));
                // also try sending a read command to another replica, in case they're ready to serve a response
                return Action.TryAlternative;
            case Invalid:
                onFailure(from, new IllegalStateException("Submitted a read command to a replica that did not own the range"));
                return Action.Abort;
        }
    }

    @Override
    void onSuccess()
    {
        Result result = txn.result(data);
        callback.accept(result, null);
        Topologies sendTo = txnId.epoch == executeAt.epoch ? applyTo : node.topology().preciseEpochs(route, txnId.epoch, executeAt.epoch);
        Persist.persist(node, sendTo, applyTo, txnId, route, txn, executeAt, deps, txn.execute(executeAt, data), result);
    }

    @Override
    public void onFailure(Throwable failure)
    {
        callback.accept(null, failure);
    }
}
