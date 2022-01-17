package accord.coordinate;

import accord.api.Key;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.InformOfTxn;
import accord.messages.InformOfTxn.InformOfTxnReply;
import accord.topology.Shard;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

public class InformHomeOfTxn extends AsyncFuture<Void> implements Callback<InformOfTxnReply>
{
    final TxnId txnId;
    final Key homeKey;
    final QuorumShardTracker tracker;
    Throwable failure;

    InformHomeOfTxn(TxnId txnId, Key homeKey, Shard homeShard)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.tracker = new QuorumShardTracker(homeShard);
    }

    public static Future<Void> inform(Node node, TxnId txnId, Txn txn, Key homeKey)
    {
        // TODO: we should not need to send the Txn here, but to avoid that we need to support no-ops
        Shard homeShard = node.topology().forEpochIfKnown(homeKey, txnId.epoch);
        if (homeShard == null)
        {
            node.configService().fetchTopologyForEpoch(txnId.epoch);
            return node.topology().awaitEpoch(txnId.epoch).flatMap(ignore -> inform(node, txnId, txn, homeKey));
        }
        InformHomeOfTxn inform = new InformHomeOfTxn(txnId, homeKey, homeShard);
        node.send(homeShard.nodes, new InformOfTxn(txnId, homeKey, txn), inform);
        return inform;
    }

    @Override
    public void onSuccess(Id from, InformOfTxnReply response)
    {
        if (response.isOk())
        {
            if (tracker.success(from))
                trySuccess(null);
        }
        else
        {
            onFailure(from, new StaleTopology());
        }
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        if (failure == null) failure = throwable;
        else failure.addSuppressed(throwable);

        // TODO: if we fail and have an incorrect topology, trigger refresh
        if (tracker.failure(from))
            tryFailure(failure);
    }
}
