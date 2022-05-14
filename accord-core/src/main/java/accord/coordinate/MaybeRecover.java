package accord.coordinate;

import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.coordinate.Recover.Outcome;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.AbstractRoute;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Ballot;
import accord.primitives.TxnId;

import static accord.local.Status.Accepted;
import static accord.utils.Functions.reduceNonNull;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class MaybeRecover extends CheckShards implements BiConsumer<Outcome, Throwable>
{
    @Nullable final AbstractRoute route;
    final RoutingKey homeKey;
    final Status knownStatus;
    final Ballot knownPromised;
    final boolean knownPromisedHasBeenAccepted;
    final BiConsumer<CheckStatusOk, Throwable> callback;

    MaybeRecover(Node node, TxnId txnId, RoutingKey homeKey, @Nullable AbstractRoute route, Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted, BiConsumer<CheckStatusOk, Throwable> callback)
    {
        // we only want to enquire with the home shard, but we prefer maximal route information for running Invalidation against, if necessary
        super(node, txnId, RoutingKeys.of(homeKey), txnId.epoch, IncludeInfo.Route);
        this.homeKey = homeKey;
        this.route = route;
        this.knownStatus = knownStatus;
        this.knownPromised = knownPromised;
        this.knownPromisedHasBeenAccepted = knownPromiseHasBeenAccepted;
        this.callback = callback;
    }

    public static void maybeRecover(Node node, TxnId txnId, RoutingKey homeKey, @Nullable AbstractRoute route,
                                    Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted,
                                    BiConsumer<CheckStatusOk, Throwable> callback)
    {
        MaybeRecover maybeRecover = new MaybeRecover(node, txnId, homeKey, route, knownStatus, knownPromised, knownPromiseHasBeenAccepted, callback);
        maybeRecover.start();
    }

    @Override
    public void accept(Outcome outcome, Throwable fail)
    {
        if (fail != null) callback.accept(null, fail);
        else switch (outcome)
        {
            default: throw new IllegalStateException();
            case Executed:
            case Invalidated:
                callback.accept(null, null);
                break;
        }
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        return hasMadeProgress(ok);
    }

    public boolean hasMadeProgress(CheckStatusOk ok)
    {
        return ok != null && (ok.isCoordinating
                              || ok.status.logicalCompareTo(knownStatus) > 0
                              || ok.promised.compareTo(knownPromised) > 0
                              || (!knownPromisedHasBeenAccepted && knownStatus.logicalCompareTo(Accepted) == 0 && ok.accepted.equals(knownPromised)));
    }

    @Override
    protected void onDone(Done done, Throwable fail)
    {
        super.onDone(done, fail);
        if (fail != null)
        {
            callback.accept(null, fail);
        }
        else if (merged == null)
        {
            callback.accept(null, new Timeout(txnId, homeKey));
        }
        else
        {
            switch (merged.status)
            {
                default: throw new AssertionError();
                case NotWitnessed:
                case AcceptedInvalidate:
                    if (!(merged.route instanceof Route))
                    {
                        // order important, as route could be a Route which does not implement RoutingKeys.union
                        RoutingKeys someKeys = reduceNonNull(RoutingKeys::union, this.someKeys, merged.route, route);
                        Invalidate.invalidateIfNotWitnessed(node, txnId, someKeys, homeKey, this);
                        break;
                    }
                case PreAccepted:
                case Accepted:
                case Committed:
                case ReadyToExecute:
                case Executed:
                case Applied:
                    Preconditions.checkState(merged.route instanceof Route);
                    if (hasMadeProgress(merged)) callback.accept(merged, null);
                    else node.recover(txnId, (Route) merged.route).addCallback(this);
                    break;

                case Invalidated:
                    RoutingKeys someKeys = reduceNonNull(RoutingKeys::union, this.someKeys, merged.route, route);
                    Invalidate.invalidate(node, txnId, someKeys, homeKey, this);
            }
        }
    }
}
