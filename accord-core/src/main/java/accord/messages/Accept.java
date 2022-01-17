package accord.messages;

import accord.topology.Topologies;
import accord.api.Key;
import accord.txn.Ballot;
import accord.local.Node;
import accord.txn.Timestamp;
import accord.local.Command;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;

import static accord.messages.PreAccept.calculateDeps;

public class Accept extends TxnRequest
{
    public final Ballot ballot;
    public final TxnId txnId;
    public final Txn txn;
    public final Key homeKey;
    public final long minEpoch;
    public final Timestamp executeAt;
    public final Dependencies deps;

    public Accept(Node.Id to, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Dependencies deps)
    {
        super(to, topologies, txn.keys);
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.minEpoch = topologies.oldestEpoch();
        this.executeAt = executeAt;
        this.deps = deps;
    }

    public void process(Node node, Node.Id replyToNode, ReplyContext replyContext)
    {
        Key progressKey = node.trySelectProgressKey(waitForEpoch(), txn.keys, homeKey);
        // TODO: when we begin expunging old epochs we need to ensure we handle the case where we do not fully handle the keys;
        //       since this will likely imply the transaction has been applied or aborted we can indicate the coordinator
        //       should enquire as to the result
        node.reply(replyToNode, replyContext, node.mapReduceLocal(scope(), minEpoch, executeAt.epoch, instance -> {
            Command command = instance.command(txnId);
            if (!command.accept(ballot, txn, homeKey, progressKey, executeAt, deps))
                return new AcceptNack(txnId, command.promised());
            return new AcceptOk(txnId, calculateDeps(instance, txnId, txn, executeAt));
        }, (r1, r2) -> {
            if (!r1.isOK()) return r1;
            if (!r2.isOK()) return r2;
            AcceptOk ok1 = (AcceptOk) r1;
            AcceptOk ok2 = (AcceptOk) r2;
            if (ok1.deps.isEmpty()) return ok2;
            if (ok2.deps.isEmpty()) return ok1;
            ok1.deps.addAll(ok2.deps);
            return ok1;
        }));
    }

    @Override
    public MessageType type()
    {
        return MessageType.ACCEPT_REQ;
    }

    public interface AcceptReply extends Reply
    {
        @Override
        default MessageType type()
        {
            return MessageType.ACCEPT_RSP;
        }

        boolean isOK();
    }

    public static class AcceptOk implements AcceptReply
    {
        public final TxnId txnId;
        public final Dependencies deps;

        public AcceptOk(TxnId txnId, Dependencies deps)
        {
            this.txnId = txnId;
            this.deps = deps;
        }

        @Override
        public boolean isOK()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "AcceptOk{" +
                    "txnId=" + txnId +
                    ", deps=" + deps +
                    '}';
        }
    }

    public static class AcceptNack implements AcceptReply
    {
        public final TxnId txnId;
        public final Timestamp reject;

        public AcceptNack(TxnId txnId, Timestamp reject)
        {
            this.txnId = txnId;
            this.reject = reject;
        }

        @Override
        public boolean isOK()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "AcceptNack{" +
                    "txnId=" + txnId +
                    ", reject=" + reject +
                    '}';
        }
    }

    @Override
    public String toString()
    {
        return "Accept{" +
               "ballot: " + ballot +
               ", txnId: " + txnId +
               ", txn: " + txn +
               ", executeAt: " + executeAt +
               ", deps: " + deps +
               '}';
    }
}
