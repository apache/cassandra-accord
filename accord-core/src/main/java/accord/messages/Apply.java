package accord.messages;

import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Result;
import accord.topology.Topologies;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Writes;
import accord.txn.Txn;
import accord.txn.TxnId;

public class Apply extends TxnRequest
{
    public final TxnId txnId;
    public final Txn txn;
    // TODO: these only need to be sent if we don't know if this node has witnessed a Commit
    public final Dependencies deps;
    public final Timestamp executeAt;
    public final Writes writes;
    public final Result result;

    public Apply(Scope scope, TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps, Writes writes, Result result)
    {
        super(scope);
        this.txnId = txnId;
        this.txn = txn;
        this.deps = deps;
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    public Apply(Node.Id to, Topologies topologies, TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps, Writes writes, Result result)
    {
        this(Scope.forTopologies(to, topologies, txn), txnId, txn, executeAt, deps, writes, result);
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        node.forEachLocal(scope(), instance -> instance.command(txnId).apply(txn, deps, executeAt, writes, result));
    }

    @Override
    public MessageType type()
    {
        return MessageType.APPLY_REQ;
    }

    @Override
    public String toString()
    {
        return "Apply{" +
               "txnId: " + txnId +
               ", txn: " + txn +
               ", deps: " + deps +
               ", executeAt: " + executeAt +
               ", writes: " + writes +
               ", result: " + result +
               '}';
    }
}
