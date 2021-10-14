package accord.coordinate;

import accord.api.Result;
import accord.topology.Topologies;
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
    public final Topologies topologies;
    public final Writes applied;
    public final Result result;

    public Agreed(TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps, Topologies topologies, Writes applied, Result result)
    {
        this.txnId = txnId;
        this.txn = txn;
        this.executeAt = executeAt;
        this.deps = deps;
        this.topologies = topologies;
        this.applied = applied;
        this.result = result;
    }
}
