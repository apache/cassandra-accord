package accord.messages;

import accord.local.Node;
import accord.topology.Topologies;

public abstract class TxnRequest implements Request
{
    private final TxnRequestScope scope;

    public TxnRequest(TxnRequestScope scope)
    {
        this.scope = scope;
    }

    public TxnRequest(Node.Id to, Topologies topologies)
    {
        scope = TxnRequestScope.forTopologies(to, topologies);
    }

    public TxnRequestScope scope()
    {
        return scope;
    }
}
