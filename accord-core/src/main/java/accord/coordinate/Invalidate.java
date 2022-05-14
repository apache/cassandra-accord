package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.coordinate.Recover.Outcome;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.BeginInvalidation;
import accord.messages.BeginInvalidation.InvalidateNack;
import accord.messages.BeginInvalidation.InvalidateOk;
import accord.messages.BeginInvalidation.InvalidateReply;
import accord.messages.Callback;
import accord.primitives.AbstractRoute;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.topology.Shard;
import accord.primitives.Ballot;
import accord.primitives.TxnId;

import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.local.Status.Accepted;
import static accord.local.Status.PreAccepted;
import static accord.messages.Commit.Invalidate.commitInvalidate;

public class Invalidate implements Callback<InvalidateReply>
{
    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final RoutingKeys someKeys;
    final RoutingKey someKey;
    final Status recoverIfAtLeast;
    final BiConsumer<Outcome, Throwable> callback;

    boolean isDone;
    final List<InvalidateOk> invalidateOks = new ArrayList<>();
    final QuorumShardTracker preacceptTracker;

    private Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, RoutingKeys someKeys, RoutingKey someKey, Status recoverIfAtLeast, BiConsumer<Outcome, Throwable> callback)
    {
        this.callback = callback;
        Preconditions.checkArgument(someKeys.contains(someKey));
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.someKeys = someKeys;
        this.someKey = someKey;
        this.recoverIfAtLeast = recoverIfAtLeast;
        this.preacceptTracker = new QuorumShardTracker(shard);
    }

    public static Invalidate invalidateIfNotWitnessed(Node node, TxnId txnId, RoutingKeys someKeys, RoutingKey someKey, BiConsumer<Outcome, Throwable> callback)
    {
        return invalidate(node, txnId, someKeys, someKey, PreAccepted, callback);
    }

    public static Invalidate invalidate(Node node, TxnId txnId, RoutingKeys someKeys, RoutingKey someKey, BiConsumer<Outcome, Throwable> callback)
    {
        return invalidate(node, txnId, someKeys, someKey, Accepted, callback);
    }

    private static Invalidate invalidate(Node node, TxnId txnId, RoutingKeys someKeys, RoutingKey someKey, Status recoverIfAtLeast, BiConsumer<Outcome, Throwable> callback)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        Shard shard = node.topology().forEpochIfKnown(someKey, txnId.epoch);
        Invalidate invalidate = new Invalidate(node, shard, ballot, txnId, someKeys, someKey, recoverIfAtLeast, callback);
        node.send(shard.nodes, to -> new BeginInvalidation(txnId, someKey, ballot), invalidate);
        return invalidate;
    }

    @Override
    public synchronized void onSuccess(Id from, InvalidateReply reply)
    {
        if (isDone || preacceptTracker.hasReachedQuorum())
            return;

        if (!reply.isOk())
        {
            InvalidateNack nack = (InvalidateNack) reply;
            if (nack.homeKey != null)
            {
                node.ifLocalSince(someKey, txnId, instance -> {
                    instance.command(txnId).updateHomeKey(nack.homeKey);
                    return null;
                });
            }

            isDone = true;
            callback.accept(null, new Preempted(txnId, null));
            return;
        }

        InvalidateOk ok = (InvalidateOk) reply;
        invalidateOks.add(ok);
        if (preacceptTracker.success(from))
            invalidate();
    }

    private void invalidate()
    {
        // first look to see if it has already been
        {
            Status maxStatus = invalidateOks.stream().map(ok -> ok.status).max(Comparable::compareTo).orElseThrow();
            Route route = InvalidateOk.findRoute(invalidateOks);
            RoutingKey homeKey = route != null ? route.homeKey : InvalidateOk.findHomeKey(invalidateOks);

            switch (maxStatus)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                    break;
                case PreAccepted:
                case Accepted:
                    if (recoverIfAtLeast.compareTo(maxStatus) > 0)
                        break;

                case Committed:
                case ReadyToExecute:
                case Executed:
                case Applied:
                    if (route != null)
                    {
                        RecoverWithRoute.recover(node, ballot, txnId, route, callback);
                    }
                    else if (homeKey != null && homeKey.equals(someKey))
                    {
                        throw new IllegalStateException("Received a reply from a node that must have known the route, but that did not include it");
                    }
                    else if (homeKey != null)
                    {
                        RecoverWithHomeKey.recover(node, txnId, homeKey, callback);
                    }
                    else
                    {
                        throw new IllegalStateException("Received a reply from a node that must have known the homeKey, but that did not include it");
                    }
                    return;

                case AcceptedInvalidate:
                    break; // latest accept also invalidating, so we're on the same page and should finish our invalidation

                case Invalidated:
                    node.forEachLocalSince(someKeys, txnId, instance -> {
                        instance.command(txnId).commitInvalidate();
                    });
                    isDone = true;
                    callback.accept(Outcome.Invalidated, null);
                    return;
            }
        }

        // if we have witnessed the transaction, but are able to invalidate, do we want to proceed?
        // Probably simplest to do so, but perhaps better for user if we don't.
        proposeInvalidate(node, ballot, txnId, someKey).addCallback((success, fail) -> {
            if (fail != null)
            {
                isDone = true;
                callback.accept(null, fail);
                return;
            }

            try
            {
                AbstractRoute route = InvalidateOk.mergeRoutes(invalidateOks);
                // TODO: commitInvalidate (and others) should skip the network for local applications,
                //  so we do not need to explicitly do so here before notifying the waiter
                commitInvalidate(node, txnId, route != null ? route : someKeys, txnId);
                // TODO: pick a reasonable upper bound, so we don't invalidate into an epoch/commandStore that no longer cares about this command
                node.forEachLocalSince(someKeys, txnId, instance -> {
                    instance.command(txnId).commitInvalidate();
                });
            }
            finally
            {
                isDone = true;
                callback.accept(Outcome.Invalidated, null);
            }
        });
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        if (preacceptTracker.failure(from))
        {
            isDone = true;
            callback.accept(null, new Timeout(txnId, null));
        }
    }

    @Override
    public void onCallbackFailure(Throwable failure)
    {
        if (isDone)
            return;

        isDone = true;
        callback.accept(null, failure);
    }
}
