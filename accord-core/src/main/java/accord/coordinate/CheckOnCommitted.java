package accord.coordinate;

import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Command.ApplyOutcome;
import accord.local.Command.CommitOutcome;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRanges;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.local.Command.CommitOutcome.Insufficient;
import static accord.local.Status.Executed;

/**
 * Check on the status of a known-committed transaction. Returns early if any result indicates Executed, otherwise
 * waits only for a quorum and returns the maximum result. Updates local command stores based on the obtained information.
 *
 * If a command is durable (i.e. executed on a majority on all shards) this is sufficient to replicate the command locally.
 */
public class CheckOnCommitted extends CheckShards
{
    final BiConsumer<? super CheckStatusOkFull, Throwable> callback;
    /**
     * The epoch until which we want to persist any response for locally
     */
    final long untilLocalEpoch;
    final AbstractRoute route;

    CheckOnCommitted(Node node, TxnId txnId, AbstractRoute route, long untilRemoteEpoch, long untilLocalEpoch, BiConsumer<? super CheckStatusOkFull, Throwable> callback)
    {
        this(node, txnId, route, route, untilRemoteEpoch, untilLocalEpoch, callback);
    }

    CheckOnCommitted(Node node, TxnId txnId, AbstractRoute route, RoutingKeys someKeys, long untilRemoteEpoch, long untilLocalEpoch, BiConsumer<? super CheckStatusOkFull, Throwable> callback)
    {
        // TODO (now): restore behaviour of only collecting info if e.g. Committed or Executed
        super(node, txnId, someKeys, untilRemoteEpoch, IncludeInfo.All);
        this.route = route;
        this.callback = callback;
        this.untilLocalEpoch = untilLocalEpoch;
    }

    // TODO: many callers only need to consult precisely executeAt.epoch remotely
    public static CheckOnCommitted checkOnCommitted(Node node, TxnId txnId, AbstractRoute route, long untilRemoteEpoch, long untilLocalEpoch, BiConsumer<? super CheckStatusOkFull, Throwable> callback)
    {
        CheckOnCommitted checkOnCommitted = new CheckOnCommitted(node, txnId, route, untilRemoteEpoch, untilLocalEpoch, callback);
        checkOnCommitted.start();
        return checkOnCommitted;
    }

    protected AbstractRoute route()
    {
        return route;
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        return ((CheckStatusOkFull)ok).fullStatus.hasBeen(Executed);
    }

    @Override
    protected void onDone(Done done, Throwable failure)
    {
        if (failure != null)
        {
            callback.accept(null, failure);
        }
        else
        {
            super.onDone(done, null);
            onSuccessCriteriaOrExhaustion((CheckStatusOkFull) merged);
        }
    }

    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull full)
    {
        long epoch = full.executeAt == null ? untilLocalEpoch : Math.max(full.executeAt.epoch, untilLocalEpoch);
        node.withEpoch(epoch, () -> {
            persistLocally(full);
            callback.accept(full, null);
        });
    }

    void persistLocally(CheckStatusOkFull full)
    {
        switch (full.fullStatus)
        {
            case Invalidated:
                AbstractRoute route = AbstractRoute.merge(route(), full.route);
                node.forEachLocal(route, txnId.epoch, untilLocalEpoch, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.commitInvalidate();
                });
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
                return;
        }

        KeyRanges commitRanges = node.topology().localRangesForEpochs(txnId.epoch, untilLocalEpoch);
        if (!route().covers(commitRanges))
            return; // only try to persist locally if we requested enough data

        Timestamp executeAt = full.executeAt;
        KeyRanges executeRanges = node.topology().localRangesForEpochs(executeAt.epoch, Math.max(executeAt.epoch, untilLocalEpoch));
        KeyRanges allRanges = node.topology().localRangesForEpochs(txnId.epoch, untilLocalEpoch);

        PartialRoute requestedRoute = route().slice(allRanges);
        AbstractRoute maxRoute = route().union(full.route);
        RoutingKey progressKey = node.trySelectProgressKey(txnId, maxRoute);

        boolean canCommit = requestedRoute.covers(commitRanges);
        boolean canExecute = requestedRoute.covers(executeRanges);

        Preconditions.checkState(canCommit);
        Preconditions.checkState(untilRemoteEpoch < full.executeAt.epoch || canExecute);
        Preconditions.checkState(full.partialTxn.covers(requestedRoute));
        Preconditions.checkState(full.committedDeps.covers(requestedRoute));

        PartialTxn partialTxn = full.partialTxn.reconstitutePartial(requestedRoute).slice(allRanges, true);
        PartialDeps partialDeps = full.committedDeps.reconstitutePartial(requestedRoute).slice(allRanges);

        switch (full.fullStatus)
        {
            default: throw new IllegalStateException();
            case Executed:
            case Applied:
                if (canExecute)
                {
                    // TODO: assert that the outcome is Success or Redundant, but only for those we expect to succeed
                    //  (i.e. those covered by Route)
                    node.forEachLocal(maxRoute, txnId.epoch, untilLocalEpoch, commandStore -> {
                        Command command = commandStore.command(txnId);
                        confirm(command.commit(maxRoute, progressKey, partialTxn, executeAt, partialDeps));
                        confirm(command.apply(untilLocalEpoch, maxRoute, executeAt, partialDeps, full.writes, full.result));
                    });
                    break;
                }
            case Committed:
            case ReadyToExecute:
                node.forEachLocal(maxRoute, txnId.epoch, untilLocalEpoch, commandStore -> {
                    Command command = commandStore.command(txnId);
                    if (Insufficient == command.commit(maxRoute, progressKey, partialTxn, executeAt, partialDeps))
                        command.commit(maxRoute, progressKey, partialTxn, executeAt, partialDeps);
//                    confirm(command.commit(route, progressKey, partialTxn, executeAt, partialDeps));
                });
        }
    }

    private static void confirm(CommitOutcome outcome)
    {
        switch (outcome)
        {
            default: throw new IllegalStateException();
            case Redundant:
            case Success:
                return;
            case Insufficient: throw new IllegalStateException("Should have enough information");
        }
    }

    private static void confirm(ApplyOutcome outcome)
    {
        switch (outcome)
        {
            default: throw new IllegalStateException();
            case Redundant:
            case Success:
            case OutOfRange:
                return;
            case Insufficient: throw new IllegalStateException("Should have enough information");
        }

    }
}
