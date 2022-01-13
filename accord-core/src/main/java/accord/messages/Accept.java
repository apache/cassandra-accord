package accord.messages;

import accord.topology.Topologies;
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
    public final Timestamp executeAt;
    public final Dependencies deps;

    public Accept(TxnRequestScope scope, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps)
    {
        super(scope);
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.executeAt = executeAt;
        this.deps = deps;
    }

    public Accept(Node.Id dst, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps)
    {
        this(TxnRequestScope.forTopologies(dst, topologies, txn), ballot, txnId, txn, executeAt, deps);
    }

    public void process(Node on, Node.Id replyToNode, long replyToMessage)
    {
        on.reply(replyToNode, replyToMessage, on.local(scope()).map(instance -> {
            Command command = instance.command(txnId);
            if (!command.accept(ballot, txn, executeAt, deps))
                return new AcceptNack(txnId, command.promised());
            return new AcceptOk(txnId, calculateDeps(instance, txnId, txn, executeAt));
        }).reduce((r1, r2) -> {
            if (!r1.isOK()) return r1;
            if (!r2.isOK()) return r2;
            AcceptOk ok1 = (AcceptOk) r1;
            AcceptOk ok2 = (AcceptOk) r2;
            if (ok1.deps.isEmpty()) return ok2;
            if (ok2.deps.isEmpty()) return ok1;
            ok1.deps.addAll(ok2.deps);
            return ok1;
        }).orElseThrow());
    }

    public interface AcceptReply extends Reply
    {
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
