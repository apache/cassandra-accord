package accord.messages;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.local.*;
import accord.primitives.AbstractRoute;
import accord.primitives.Ballot;
import accord.primitives.Route;
import accord.primitives.TxnId;

import static accord.utils.Functions.mapReduceNonNull;

public class BeginInvalidation extends AbstractEpochRequest<BeginInvalidation.InvalidateReply> implements EpochRequest, PreLoadContext
{
    public final Ballot ballot;
    public final RoutingKey someKey;

    public BeginInvalidation(TxnId txnId, RoutingKey someKey, Ballot ballot)
    {
        super(txnId);
        this.someKey = someKey;
        this.ballot = ballot;
    }

    public void process()
    {
        node.mapReduceConsumeLocal(this, someKey, txnId.epoch, this);
    }

    @Override
    public InvalidateReply apply(SafeCommandStore instance)
    {
        Command command = instance.command(txnId);

        if (!command.preacceptInvalidate(ballot))
            return new InvalidateNack(command.promised(), command.homeKey());

        return new InvalidateOk(command.status(), command.route(), command.homeKey());
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Iterable<Key> keys()
    {
        return Collections.emptyList();
    }

    @Override
    public long waitForEpoch()
    {
        return txnId.epoch;
    }

    @Override
    public MessageType type()
    {
        return MessageType.BEGIN_INVALIDATE_REQ;
    }

    @Override
    public String toString()
    {
        return "BeginInvalidate{" +
               "txnId:" + txnId +
               ", ballot:" + ballot +
               '}';
    }

    public interface InvalidateReply extends Reply
    {
        boolean isOk();
    }

    public static class InvalidateOk implements InvalidateReply
    {
        public final Status status;
        public final @Nullable AbstractRoute route;
        public final @Nullable RoutingKey homeKey;

        public InvalidateOk(Status status, @Nullable AbstractRoute route, @Nullable RoutingKey homeKey)
        {
            this.status = status;
            this.route = route;
            this.homeKey = homeKey;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "InvalidateOk{" + status + ',' + (route != null ? route: homeKey) + '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.BEGIN_INVALIDATE_RSP;
        }

        public static Route findRoute(List<InvalidateOk> invalidateOks)
        {
            for (InvalidateOk ok : invalidateOks)
            {
                if (ok.route instanceof Route)
                    return (Route)ok.route;
            }
            return null;
        }

        public static AbstractRoute mergeRoutes(List<InvalidateOk> invalidateOks)
        {
            return mapReduceNonNull(ok -> ok.route, AbstractRoute::union, invalidateOks);
        }

        public static RoutingKey findHomeKey(List<InvalidateOk> invalidateOks)
        {
            for (InvalidateOk ok : invalidateOks)
            {
                if (ok.homeKey != null)
                    return ok.homeKey;
            }
            return null;
        }
    }

    public static class InvalidateNack implements InvalidateReply
    {
        public final Ballot supersededBy;
        public final RoutingKey homeKey;
        public InvalidateNack(Ballot supersededBy, RoutingKey homeKey)
        {
            this.supersededBy = supersededBy;
            this.homeKey = homeKey;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "InvalidateNack{supersededBy:" + supersededBy + '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.BEGIN_INVALIDATE_RSP;
        }
    }
}
