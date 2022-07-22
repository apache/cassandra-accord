package accord.messages;

import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Result;
import accord.primitives.AbstractRoute;
import accord.primitives.Deps;
import accord.primitives.KeyRanges;
import accord.primitives.PartialDeps;
import accord.topology.Topologies;
import accord.primitives.Timestamp;
import accord.primitives.Writes;
import accord.primitives.TxnId;

import static accord.messages.MessageType.APPLY_REQ;
import static accord.messages.MessageType.APPLY_RSP;

public class Apply extends TxnRequest
{
    public final long untilEpoch;
    public final TxnId txnId;
    public final Timestamp executeAt;
    public final PartialDeps deps;
    public final Writes writes;
    public final Result result;

    public Apply(Id to, Topologies sendTo, Topologies applyTo, long untilEpoch, TxnId txnId, AbstractRoute route, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(to, sendTo, route);
        this.untilEpoch = untilEpoch;
        this.txnId = txnId;
        // TODO: we shouldn't send deps unless we need to (but need to implement fetching them if they're not present)
        KeyRanges slice = applyTo == sendTo ? scope.covering : applyTo.computeRangesForNode(to);
        this.deps = deps.slice(slice);
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        // note, we do not also commit here if txnId.epoch != executeAt.epoch, as the scope() for a commit would be different
        ApplyReply reply = node.mapReduceLocal(scope(), txnId.epoch, untilEpoch, instance -> {
            Command command = instance.command(txnId);
            switch (command.apply(untilEpoch, scope, executeAt, deps, writes, result))
            {
                default:
                case Insufficient:
                    return ApplyReply.Insufficient;
                case Redundant:
                    return ApplyReply.Redundant;
                case Success:
                    return ApplyReply.Applied;
                case OutOfRange:
                    throw new IllegalStateException();
            }
        }, (r1, r2) -> r1.compareTo(r2) >= 0 ? r1 : r2);

        if (reply == ApplyReply.Applied)
        {
            node.ifLocal(scope.homeKey, txnId, instance -> { instance.progressLog().durableLocal(txnId); return null; });
        }

        node.reply(replyToNode, replyContext, reply);
    }

    @Override
    public MessageType type()
    {
        return APPLY_REQ;
    }

    public enum ApplyReply implements Reply
    {
        Applied, Redundant, Insufficient;

        @Override
        public MessageType type()
        {
            return APPLY_RSP;
        }

        @Override
        public String toString()
        {
            return "Apply" + name();
        }

        @Override
        public boolean isFinal()
        {
            return this != Insufficient;
        }
    }

    @Override
    public String toString()
    {
        return "Apply{" +
               "txnId:" + txnId +
               ", deps:" + deps +
               ", executeAt:" + executeAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }
}
