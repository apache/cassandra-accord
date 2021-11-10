package accord.messages;

import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Request;
import accord.api.Result;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Writes;
import accord.txn.Txn;
import accord.txn.TxnId;

public class Apply implements Request
{
    final TxnId txnId;
    final Txn txn;
    // TODO: these only need to be sent if we don't know if this node has witnessed a Commit
    final Dependencies deps;
    final Timestamp executeAt;
    final Writes writes;
    final Result result;

    public Apply(TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps, Writes writes, Result result)
    {
        this.txnId = txnId;
        this.txn = txn;
        this.deps = deps;
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    public void process(Node node, Id replyToNode, long replyToMessage)
    {
        node.local(txn).forEach(instance -> instance.command(txnId).apply(txn, deps, executeAt, writes, result));
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
