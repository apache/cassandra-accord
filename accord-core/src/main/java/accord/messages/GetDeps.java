package accord.messages;

import accord.api.Key;
import accord.local.SafeCommandStore;
import com.google.common.base.Preconditions;

import accord.local.Node.Id;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import java.util.Collections;

import static accord.messages.PreAccept.calculatePartialDeps;

public class GetDeps extends TxnRequest.WithUnsynced<PartialDeps>
{
    public static final class SerializationSupport
    {
        public static GetDeps create(TxnId txnId, PartialRoute scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, Keys keys, Timestamp executeAt, Txn.Kind kind)
        {
            return new GetDeps(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey, keys, executeAt, kind);
        }
    }

    public final Keys keys;
    public final Timestamp executeAt;
    public final Txn.Kind kind;

    public GetDeps(Id to, Topologies topologies, Route route, TxnId txnId, Txn txn, Timestamp executeAt)
    {
        super(to, topologies, txnId, route);
        this.keys = txn.keys().slice(scope.covering);
        this.executeAt = executeAt;
        this.kind = txn.kind();
    }

    protected GetDeps(TxnId txnId, PartialRoute scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, Keys keys, Timestamp executeAt, Txn.Kind kind)
    {
        super(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey);
        this.keys = keys;
        this.executeAt = executeAt;
        this.kind = kind;
    }

    public void process()
    {
        node.mapReduceConsumeLocal(this, minEpoch, executeAt.epoch, this);
    }

    @Override
    public PartialDeps apply(SafeCommandStore instance)
    {
        // TODO: shrink ranges to those that intersect key
        KeyRanges ranges = instance.ranges().between(minEpoch, executeAt.epoch);
        return calculatePartialDeps(instance, txnId, keys, kind, executeAt, ranges);
    }

    @Override
    public PartialDeps reduce(PartialDeps deps1, PartialDeps deps2)
    {
        return deps1.with(deps2);
    }

    @Override
    public void accept(PartialDeps result, Throwable failure)
    {
        node.reply(replyTo, replyContext, new GetDepsOk(result));
    }

    @Override
    public MessageType type()
    {
        return MessageType.GET_DEPS_REQ;
    }

    @Override
    public String toString()
    {
        return "GetDeps{" +
               "txnId:" + txnId +
               ", keys:" + keys +
               ", executeAt:" + executeAt +
               '}';
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Iterable<Key> keys()
    {
        return keys;
    }

    public static class GetDepsOk implements Reply
    {
        public final PartialDeps deps;

        public GetDepsOk(PartialDeps deps)
        {
            Preconditions.checkNotNull(deps);
            this.deps = deps;
        }

        @Override
        public String toString()
        {
            return toString("GetDepsOk");
        }

        String toString(String kind)
        {
            return kind + "{" + deps + '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.GET_DEPS_RSP;
        }
    }

}
