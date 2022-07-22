package accord.coordinate;

import java.util.Set;
import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;

import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.KeyRanges;
import accord.primitives.PartialRoute;
import accord.primitives.Route;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.coordinate.Recover.Outcome.Executed;
import static accord.coordinate.Recover.Outcome.Invalidated;
import static accord.local.Status.PreAccepted;
import static accord.messages.Commit.Invalidate.commitInvalidate;

public class RecoverWithRoute extends CheckShards
{
    final Ballot ballot;
    final Route route;
    final BiConsumer<Recover.Outcome, Throwable> callback;

    private RecoverWithRoute(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Route route, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        super(node, txnId, route, txnId.epoch, IncludeInfo.All);
        this.ballot = ballot;
        this.route = route;
        this.callback = callback;
        assert topologies.oldestEpoch() == topologies.currentEpoch() && topologies.currentEpoch() == txnId.epoch;
    }

    public static RecoverWithRoute recover(Node node, TxnId txnId, Route route, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        return recover(node, node.topology().forEpoch(route, txnId.epoch), txnId, route, callback);
    }

    public static RecoverWithRoute recover(Node node, Topologies topologies, TxnId txnId, Route route, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        return recover(node, topologies, ballot, txnId, route, callback);
    }

    public static RecoverWithRoute recover(Node node, Ballot ballot, TxnId txnId, Route route, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        return recover(node, node.topology().forEpoch(route, txnId.epoch), ballot, txnId, route, callback);
    }

    public static RecoverWithRoute recover(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Route route, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        RecoverWithRoute recover = new RecoverWithRoute(node, topologies, ballot, txnId, route, callback);
        recover.start();
        return recover;
    }

    @Override
    protected void contact(Set<Id> nodes)
    {
        node.send(nodes, to -> new CheckStatus(to, tracker.topologies(), txnId, route, IncludeInfo.All), this);
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        CheckStatusOkFull full = (CheckStatusOkFull)ok;
        if (!full.fullStatus.hasBeen(PreAccepted))
            return false;

        if (full.fullStatus.hasBeen(Status.Invalidated))
            return true;

        KeyRanges rangesForNode = tracker.topologies().forEpoch(txnId.epoch).rangesForNode(from);
        PartialRoute route = this.route.slice(rangesForNode);
        Preconditions.checkState(full.partialTxn.covers(route));
        return true;
    }

    @Override
    protected void onDone(Done done, Throwable failure)
    {
        super.onDone(done, failure);

        if (failure != null)
        {
            callback.accept(null, failure);
            return;
        }

        CheckStatusOkFull merged = (CheckStatusOkFull) this.merged;
        switch (merged.fullStatus)
        {
            case NotWitnessed:
            {
                Invalidate.invalidate(node, txnId, route, route.homeKey, callback);
                break;
            }
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case Committed:
            case ReadyToExecute:
            {
                Txn txn = merged.partialTxn.reconstitute(route);
                Recover.recover(node, txnId, txn, route, callback);
                break;
            }
            case Executed:
            case Applied:
            {
                assert merged.executeAt != null;
                // TODO: we might not be able to reconstitute Txn if we have GC'd on some shards
                Txn txn = merged.partialTxn.reconstitute(route);
                if (merged.committedDeps.covers(route))
                {
                    Deps deps = merged.committedDeps.reconstitute(route);
                    node.withEpoch(merged.executeAt.epoch, () -> {
                        Persist.persistAndCommit(node, txnId, route, txn, merged.executeAt, deps, merged.writes, merged.result);
                    });
                    callback.accept(Executed, null);
                    break;
                }
                Recover.recover(node, txnId, txn, route, callback);
                break;
            }
            case Invalidated:
            {
                long untilEpoch = node.topology().epoch();
                commitInvalidate(node, txnId, route, untilEpoch);
                callback.accept(Invalidated, null);
            }
        }

    }
}
