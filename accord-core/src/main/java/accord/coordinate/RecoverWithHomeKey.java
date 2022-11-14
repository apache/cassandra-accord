package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class RecoverWithHomeKey extends CheckShards implements BiConsumer<Object, Throwable>
{
    final RoutingKey homeKey;
    final BiConsumer<Outcome, Throwable> callback;
    final Status witnessedByInvalidation;

    RecoverWithHomeKey(Node node, TxnId txnId, RoutingKey homeKey, Status witnessedByInvalidation, BiConsumer<Outcome, Throwable> callback)
    {
        super(node, txnId, RoutingKeys.of(homeKey), txnId.epoch, IncludeInfo.Route);
        this.witnessedByInvalidation = witnessedByInvalidation;
        // if witnessedByInvalidation == AcceptedInvalidate then we cannot assume its definition was known, and our comparison with the status is invalid
        Preconditions.checkState(witnessedByInvalidation != Status.AcceptedInvalidate);
        // if witnessedByInvalidation == Invalidated we should anyway not be recovering
        Preconditions.checkState(witnessedByInvalidation != Status.Invalidated);
        this.homeKey = homeKey;
        this.callback = callback;
    }

    public static RecoverWithHomeKey recover(Node node, TxnId txnId, RoutingKey homeKey, Status witnessedByInvalidation, BiConsumer<Outcome, Throwable> callback)
    {
        RecoverWithHomeKey maybeRecover = new RecoverWithHomeKey(node, txnId, homeKey, witnessedByInvalidation, callback);
        maybeRecover.start();
        return maybeRecover;
    }

    @Override
    public void accept(Object unused, Throwable fail)
    {
        callback.accept(null, fail);
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return ok.route != null;
    }

    @Override
    protected void onDone(Success success, Throwable fail)
    {
        if (fail != null)
        {
            callback.accept(null, fail);
        }
        else if (merged == null || !(merged.route instanceof Route))
        {
            switch (success)
            {
                default: throw new IllegalStateException();
                case Success:
                    // home shard must know full Route. Our success criteria is that the response contained a route,
                    // so reaching here without a response or one without a full Route is a bug.
                    callback.accept(null, new IllegalStateException());
                    return;
                case Quorum:
                    if (witnessedByInvalidation != null && witnessedByInvalidation.compareTo(Status.PreAccepted) > 0)
                        throw new IllegalStateException("We previously invalidated, finding a status that should be recoverable");
                    Invalidate.invalidate(node, txnId, contactKeys.with(homeKey), true, callback);
            }
        }
        else
        {
            // start recovery
            RecoverWithRoute.recover(node, txnId, (Route)merged.route, witnessedByInvalidation, callback);
        }
    }
}
