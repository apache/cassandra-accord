package accord.coordinate;

import accord.api.Result;
import accord.topology.Topology;
import accord.txn.Writes;
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
    public final Topology topology;
    public final Writes applied;
    public final Result result;

    public Agreed(TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps, Topology topology, Writes applied, Result result)
    {
        this.txnId = txnId;
        this.txn = txn;
        this.executeAt = executeAt;
        this.deps = deps;
        this.topology = topology;
        this.applied = applied;
        this.result = result;
    }
}
