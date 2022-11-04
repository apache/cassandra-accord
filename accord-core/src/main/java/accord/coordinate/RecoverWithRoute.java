package accord.coordinate;

import java.util.function.BiConsumer;

import accord.local.Status.Known;
import accord.primitives.*;
import com.google.common.base.Preconditions;

import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.topology.Topologies;

import static accord.local.Status.Known.Nothing;
import static accord.messages.Commit.Invalidate.commitInvalidate;
import static accord.primitives.ProgressToken.APPLIED;
import static accord.primitives.ProgressToken.INVALIDATED;

public class RecoverWithRoute extends CheckShards
{
    final Ballot ballot;
    final Route route;
    final BiConsumer<Outcome, Throwable> callback;

    private RecoverWithRoute(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Route route, BiConsumer<Outcome, Throwable> callback)
    {
        super(node, txnId, route, txnId.epoch, IncludeInfo.All);
        this.ballot = ballot;
        this.route = route;
        this.callback = callback;
        assert topologies.oldestEpoch() == topologies.currentEpoch() && topologies.currentEpoch() == txnId.epoch;
    }

    public static RecoverWithRoute recover(Node node, TxnId txnId, Route route, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, node.topology().forEpoch(route, txnId.epoch), txnId, route, callback);
    }

    public static RecoverWithRoute recover(Node node, Topologies topologies, TxnId txnId, Route route, BiConsumer<Outcome, Throwable> callback)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        return recover(node, topologies, ballot, txnId, route, callback);
    }

    public static RecoverWithRoute recover(Node node, Ballot ballot, TxnId txnId, Route route, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, node.topology().forEpoch(route, txnId.epoch), ballot, txnId, route, callback);
    }

    public static RecoverWithRoute recover(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Route route, BiConsumer<Outcome, Throwable> callback)
    {
        RecoverWithRoute recover = new RecoverWithRoute(node, topologies, ballot, txnId, route, callback);
        recover.start();
        return recover;
    }

    @Override
    public void contact(Id to)
    {
        node.send(to, new CheckStatus(to, topologies(), txnId, route, IncludeInfo.All), this);
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        KeyRanges rangesForNode = topologies().forEpoch(txnId.epoch).rangesForNode(from);
        PartialRoute route = this.route.slice(rangesForNode);
        return isSufficient(route, ok);
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return isSufficient(route, merged);
    }

    protected boolean isSufficient(AbstractRoute route, CheckStatusOk ok)
    {
        CheckStatusOkFull full = (CheckStatusOkFull)ok;
        Known sufficientTo = full.sufficientFor(route);
        if (sufficientTo == Nothing)
            return false;

        if (sufficientTo == Known.Invalidation)
            return true;

        Preconditions.checkState(full.partialTxn.covers(route));
        return true;
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure != null)
        {
            callback.accept(null, failure);
            return;
        }

        CheckStatusOkFull merged = (CheckStatusOkFull) this.merged;
        switch (merged.sufficientFor(route))
        {
            case Nothing:
            {
                Invalidate.invalidate(node, txnId, route, route.homeKey, callback);
                break;
            }
            case Definition:
            case ExecutionOrder:
            {
                Txn txn = merged.partialTxn.reconstitute(route);
                Recover.recover(node, txnId, txn, route, callback);
                break;
            }
            case Outcome:
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
                    callback.accept(APPLIED, null);
                    break;
                }
                Recover.recover(node, txnId, txn, route, callback);
                break;
            }
            case Invalidation:
            {
                long untilEpoch = node.topology().epoch();
                commitInvalidate(node, txnId, route, untilEpoch);
                callback.accept(INVALIDATED, null);
            }
        }
    }
}
