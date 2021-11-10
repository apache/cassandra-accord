package accord.messages;

import accord.messages.Reply;
import accord.messages.Request;
import accord.txn.Ballot;
import accord.local.Node;
import accord.txn.Timestamp;
import accord.local.Command;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;

import static accord.messages.PreAccept.calculateDeps;

public class Accept implements Request
{
    public final Ballot ballot;
    public final TxnId txnId;
    public final Txn txn;
    public final Timestamp executeAt;
    public final Dependencies deps;

    public Accept(Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Dependencies deps)
    {
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.executeAt = executeAt;
        this.deps = deps;
    }

    public void process(Node on, Node.Id replyToNode, long replyToMessage)
    {
        on.reply(replyToNode, replyToMessage, on.local(txn).map(instance -> {
            Command command = instance.command(txnId);
            if (!command.accept(ballot, txn, executeAt, deps))
                return new AcceptNack(command.promised());
            return new AcceptOk(calculateDeps(instance, txnId, txn, executeAt));
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
        public final Dependencies deps;

        public AcceptOk(Dependencies deps)
        {
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
            return "AcceptOk{" + deps + '}';
        }
    }

    public static class AcceptNack implements AcceptReply
    {
        public final Timestamp reject;

        public AcceptNack(Timestamp reject)
        {
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
            return "AcceptNack{" + reject + '}';
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
