package accord.messages;

import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Request;
import accord.txn.Timestamp;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;

// TODO: CommitOk responses, so we can send again if no reply received? Or leave to recovery?
public class Commit extends ReadData implements Request
{
    final Timestamp executeAt;
    final Dependencies deps;
    final boolean read;

    public Commit(TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps, boolean read)
    {
        super(txnId, txn);
        this.executeAt = executeAt;
        this.deps = deps;
        this.read = read;
    }

    public void process(Node node, Id from, long messageId)
    {
        node.local(txn).forEach(instance -> instance.command(txnId).commit(txn, deps, executeAt));
        if (read) super.process(node, from, messageId);
    }

    @Override
    public String toString()
    {
        return "Commit{" +
               "executeAt: " + executeAt +
               ", deps: " + deps +
               ", read: " + read +
               '}';
    }
}
