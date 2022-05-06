package accord.messages;

import java.util.Collections;
import java.util.Set;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.local.Node;
import accord.local.Node.Id;
import accord.topology.Topologies;
import accord.txn.Keys;
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

    // TODO (now): accept Topology not Topologies
    public static void commitAndRead(Node node, Topologies executeTopologies, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Dependencies deps, Set<Id> readSet, Callback<ReadReply> callback)
    {
        for (Node.Id to : executeTopologies.nodes())
        {
            boolean read = readSet.contains(to);
            Commit send = new Commit(to, executeTopologies, txnId, txn, homeKey, executeAt, deps, read);
            if (read) node.send(to, send, callback);
            else node.send(to, send);
        }
        if (txnId.epoch != executeAt.epoch)
        {
            Topologies earlierTopologies = node.topology().preciseEpochs(txn, txnId.epoch, executeAt.epoch - 1);
            Commit.commit(node, earlierTopologies, executeTopologies, txnId, txn, homeKey, executeAt, deps);
        }
    }

    public static void commit(Node node, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Dependencies deps)
    {
        Topologies commitTo = node.topology().preciseEpochs(txn, txnId.epoch, executeAt.epoch);
        for (Node.Id to : commitTo.nodes())
        {
            Commit send = new Commit(to, commitTo, txnId, txn, homeKey, executeAt, deps, false);
            node.send(to, send);
        }
    }

    public static void commit(Node node, Topologies commitTo, Set<Id> doNotCommitTo, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Dependencies deps)
    {
        for (Node.Id to : commitTo.nodes())
        {
            if (doNotCommitTo.contains(to))
                continue;

            Commit send = new Commit(to, commitTo, txnId, txn, homeKey, executeAt, deps, false);
            node.send(to, send);
        }
    }

    public static void commit(Node node, Topologies commitTo, Topologies appliedTo, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Dependencies deps)
    {
        // TODO (now): if we switch to Topology rather than Topologies we can avoid sending commits to nodes that Apply the same
        commit(node, commitTo, Collections.emptySet(), txnId, txn, homeKey, executeAt, deps);
    }

    public static void commitInvalidate(Node node, TxnId txnId, Keys someKeys, Timestamp until)
    {
        Topologies commitTo = node.topology().preciseEpochs(someKeys, txnId.epoch, until.epoch);
        commitInvalidate(node, commitTo, txnId, someKeys);
    }

    public static void commitInvalidate(Node node, Topologies commitTo, TxnId txnId, Keys someKeys)
    {
        for (Node.Id to : commitTo.nodes())
        {
            Invalidate send = new Invalidate(to, commitTo, txnId, someKeys);
            node.send(to, send);
        }
    }

    public void process(Node node, Id from, ReplyContext replyContext)
    {
        Key progressKey = node.trySelectProgressKey(txnId, txn.keys, homeKey);
        node.forEachLocal(scope(), txnId.epoch, executeAt.epoch,
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

    public static class Invalidate extends TxnRequest
    {
        final TxnId txnId;

        public Invalidate(Id to, Topologies topologies, TxnId txnId, Keys someKeys)
        {
            super(to, topologies, someKeys);
            this.txnId = txnId;
        }

        public void process(Node node, Id from, ReplyContext replyContext)
        {
            node.forEachLocal(scope(), txnId.epoch, instance -> instance.command(txnId).commitInvalidate());
        }

        @Override
        public MessageType type()
        {
            return MessageType.COMMIT_REQ;
        }

        @Override
        public String toString()
        {
            return "CommitInvalidate{txnId: " + txnId + '}';
        }
    }
}
