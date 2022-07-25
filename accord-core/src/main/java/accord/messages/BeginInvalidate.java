package accord.messages;

import accord.api.Key;
import accord.api.Result;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.BeginRecovery.RecoverNack;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;

public class BeginInvalidate implements EpochRequest
{
    final Ballot ballot;
    final TxnId txnId;
    final Key someKey;

    public BeginInvalidate(TxnId txnId, Key someKey, Ballot ballot)
    {
        this.txnId = txnId;
        this.someKey = someKey;
        this.ballot = ballot;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        RecoverReply reply = node.ifLocal(someKey, txnId, instance -> {
            Command command = instance.command(txnId);

            if (!command.preAcceptInvalidate(ballot))
                return new InvalidateNack(command.promised(), command.txn(), command.homeKey());

            return new InvalidateOk(txnId, command.status(), command.accepted(), command.executeAt(), command.savedDeps(),
                                    command.writes(), command.result(), command.txn(), command.homeKey());
        });

        node.reply(replyToNode, replyContext, reply);
    }

    @Override
    public long waitForEpoch()
    {
        return txnId.epoch;
    }

    @Override
    public MessageType type()
    {
        return MessageType.BEGIN_INVALIDATE_REQ;
    }

    @Override
    public String toString()
    {
        return "BeginInvalidate{" +
               "txnId:" + txnId +
               ", ballot:" + ballot +
               '}';
    }

    public static class InvalidateOk extends RecoverOk
    {
        public final Txn txn;
        public final Key homeKey;

        public InvalidateOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, Dependencies deps, Writes writes, Result result, Txn txn, Key homeKey)
        {
            super(txnId, status, accepted, executeAt, deps, null, null, false, writes, result);
            this.txn = txn;
            this.homeKey = homeKey;
        }

        @Override
        public boolean isOK()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toString("InvalidateOk");
        }
    }

    public static class InvalidateNack extends RecoverNack
    {
        public final Txn txn;
        public final Key homeKey;
        public InvalidateNack(Ballot supersededBy, Txn txn, Key homeKey)
        {
            super(supersededBy);
            this.txn = txn;
            this.homeKey = homeKey;
        }

        @Override
        public boolean isOK()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "InvalidateNack{supersededBy:" + supersededBy + '}';
        }
    }
}
