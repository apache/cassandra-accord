package accord.messages;

import accord.api.Key;
import accord.local.Node;
import accord.local.Node.Id;
import accord.topology.Topologies;
import accord.txn.Timestamp;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;

// TODO: CommitOk responses, so we can send again if no reply received? Or leave to recovery?
public class Commit extends ReadData
{
    public final Dependencies deps;
    public final boolean read;

    public Commit(Id to, Topologies topologies, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Dependencies deps, boolean read)
    {
        super(to, topologies, txnId, txn, homeKey, executeAt);
        this.deps = deps;
        this.read = read;
    }

    public void process(Node node, Id from, ReplyContext replyContext)
    {
        Key progressKey = node.trySelectProgressKey(txnId, txn.keys, homeKey);
        node.forEachLocal(scope(), txnId.epoch,
                          instance -> instance.command(txnId).commit(txn, homeKey, progressKey, executeAt, deps));

        if (read)
            super.process(node, from, replyContext);
    }

    @Override
    public MessageType type()
    {
        return MessageType.COMMIT_REQ;
    }

    @Override
    public String toString()
    {
        return "Commit{txnId: " + txnId +
               ", executeAt: " + executeAt +
               ", deps: " + deps +
               ", read: " + read +
               '}';
    }
}
