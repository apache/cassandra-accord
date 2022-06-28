package accord.coordinate;

import accord.api.Key;
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
    public final Key homeKey;
    public final Timestamp executeAt;
    public final Dependencies deps;
    public final Writes applied;
    public final Result result;

    public Agreed(TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Dependencies deps, Writes applied, Result result)
    {
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
        this.deps = deps;
        this.applied = applied;
        this.result = result;
    }
}
