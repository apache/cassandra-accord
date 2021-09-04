package accord.coordinate;

import accord.api.Result;
import accord.txn.Writes;
import accord.topology.Shards;
import accord.txn.Timestamp;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;

class Agreed
{
    public final TxnId txnId;
    public final Txn txn;
    public final Timestamp executeAt;
    public final Dependencies deps;
    public final Shards shards;
    public final Writes applied;
    public final Result result;

    public Agreed(TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps, Shards shards, Writes applied, Result result)
    {
        this.txnId = txnId;
        this.txn = txn;
        this.executeAt = executeAt;
        this.deps = deps;
        this.shards = shards;
        this.applied = applied;
        this.result = result;
    }
}
