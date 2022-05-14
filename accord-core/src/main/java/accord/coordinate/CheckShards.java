package accord.coordinate;

import java.util.Set;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.CheckStatusReply;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.local.Status.Committed;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public abstract class CheckShards extends QuorumReadCoordinator<CheckStatusReply>
{
    final RoutingKeys someKeys;

    /**
     * The epoch until which we want to fetch data from remotely
     * TODO: configure the epoch we want to start with
     */
    final long untilRemoteEpoch;
    final IncludeInfo includeInfo;

    protected CheckStatusOk merged;

    protected CheckShards(Node node, TxnId txnId, RoutingKeys someKeys, long untilRemoteEpoch, IncludeInfo includeInfo)
    {
        super(node, ensureSufficient(node, txnId, someKeys, untilRemoteEpoch), txnId);
        this.untilRemoteEpoch = untilRemoteEpoch;
        this.someKeys = someKeys;
        this.includeInfo = includeInfo;
    }

    private static Topologies ensureSufficient(Node node, TxnId txnId, RoutingKeys someKeys, long epoch)
    {
        return node.topology().preciseEpochs(someKeys, txnId.epoch, epoch);
    }

    @Override
    protected void contact(Set<Id> nodes)
    {
        node.send(nodes, new CheckStatus(txnId, someKeys, txnId.epoch, untilRemoteEpoch, includeInfo), this);
    }

    protected abstract boolean isSufficient(Id from, CheckStatusOk ok);

    protected Action check(Id from, CheckStatusOk ok)
    {
        if (isSufficient(from, ok))
            return Action.Accept;

        return Action.AcceptQuorum;
    }

    @Override
    protected void onDone(Done done, Throwable failure)
    {
        if (failure != null)
            return;

        if (merged == null)
            return;

        if (merged instanceof CheckStatusOkFull)
            merged = ((CheckStatusOkFull) merged).covering(someKeys);

        if (!merged.hasExecutedOnAllShards)
            return;

        RoutingKey homeKey = merged.homeKey;
        if (homeKey == null)
            return;

        node.ifLocal(merged.homeKey, txnId, store -> {
            Timestamp executeAt = merged.status.hasBeen(Committed) ? merged.executeAt : null;
            store.command(txnId).setGloballyPersistent(merged.homeKey, executeAt);
            store.progressLog().durable(txnId, merged.homeKey, null);
            return null;
        });
    }

    @Override
    protected Action process(Id from, CheckStatusReply reply)
    {
        debug.put(from, reply);
        if (reply.isOk())
        {
            CheckStatusOk ok = (CheckStatusOk) reply;
            if (merged == null) merged = ok;
            else merged = merged.merge(ok);

            return check(from, ok);
        }
        else
        {
            onFailure(from, new IllegalStateException("Submitted command to a replica that did not own the range"));
            return Action.Abort;
        }
    }
}
