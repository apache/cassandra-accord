package accord.messages;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.local.Command.AcceptOutcome;
import accord.local.Node.Id;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Txn;
import accord.topology.Topologies;
import accord.primitives.Ballot;
import accord.local.Node;
import accord.primitives.Timestamp;
import accord.local.Command;
import accord.primitives.Deps;
import accord.primitives.TxnId;

import static accord.local.Command.AcceptOutcome.RejectedBallot;
import static accord.local.Command.AcceptOutcome.Success;
import static accord.local.Status.PreAccepted;
import static accord.messages.PreAccept.calculateDeps;

// TODO: use different objects for send and receive, so can be more efficient (e.g. serialize without slicing, and without unnecessary fields)
public class Accept extends TxnRequest.WithUnsynced
{
    public final Ballot ballot;
    public final Keys keys;
    public final @Nullable PartialTxn partialTxn;
    public final Timestamp executeAt;
    public final PartialDeps partialDeps;
    public final Txn.Kind kindOfTxn;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private transient Defer defer;

    public Accept(Id to, Topologies topologies, Ballot ballot, TxnId txnId, Route route, Txn txn, Timestamp executeAt, Deps deps, Txn.Kind kindOfTxn)
    {
        super(to, topologies, txnId, route);
        this.ballot = ballot;
        this.keys = txn.keys.slice(scope.covering);
        this.executeAt = executeAt;
        // TODO (now): send minimal
        this.partialTxn = txn.slice(scope.covering, topologies.oldest().contains(to));
        this.partialDeps = deps.slice(scope.covering);
        this.kindOfTxn = kindOfTxn;
    }

    public void process(Node node, Node.Id replyToNode, ReplyContext replyContext)
    {
        RoutingKey progressKey = progressKey(node, scope.homeKey);
        node.reply(replyToNode, replyContext, node.mapReduceLocal(scope(), minEpoch, executeAt.epoch, instance -> {
            Command command = instance.command(txnId);
            switch (command.accept(ballot, scope, progressKey, partialTxn, executeAt, partialDeps))
            {
                default: throw new IllegalStateException();
                case Redundant:
                    return AcceptNack.REDUNDANT;
                case Insufficient:
                    if (defer == null)
                        defer = new Defer(PreAccepted, this, node, replyToNode, replyContext);
                    defer.add(command, instance);
                    return AcceptNack.INSUFFICIENT;
                case RejectedBallot:
                    return new AcceptNack(RejectedBallot, command.promised());
                case Success:
                    return new AcceptOk(calculateDeps(instance, txnId, keys, kindOfTxn, executeAt, Deps.builder(keys)));
            }
        }, (r1, r2) -> {
            if (!r1.isOk() || !r2.isOk())
                return r1.outcome().compareTo(r2.outcome()) >= 0 ? r1 : r2;

            AcceptOk ok1 = (AcceptOk) r1;
            AcceptOk ok2 = (AcceptOk) r2;
            Deps deps = ok1.deps.with(ok2.deps);
            if (deps == ok1.deps) return ok1;
            if (deps == ok2.deps) return ok2;
            return new AcceptOk(deps);
        }));
    }

    @Override
    public String toString()
    {
        return "Accept{" +
               "ballot: " + ballot +
               ", txnId: " + txnId +
               ", executeAt: " + executeAt +
               ", deps: " + partialDeps +
               '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.ACCEPT_REQ;
    }

    public static class Invalidate implements EpochRequest
    {
        public final Ballot ballot;
        public final TxnId txnId;
        public final RoutingKey someKey;

        public Invalidate(Ballot ballot, TxnId txnId, RoutingKey someKey)
        {
            this.ballot = ballot;
            this.txnId = txnId;
            this.someKey = someKey;
        }

        public void process(Node node, Node.Id replyToNode, ReplyContext replyContext)
        {
            node.reply(replyToNode, replyContext, node.ifLocal(someKey, txnId.epoch, instance -> {
                Command command = instance.command(txnId);
                switch (command.acceptInvalidate(ballot))
                {
                    default:
                    case Insufficient:
                        throw new IllegalStateException();
                    case Redundant:
                        return AcceptNack.REDUNDANT;
                    case Success:
                        return new AcceptOk(null);
                    case RejectedBallot:
                       return new AcceptNack(RejectedBallot, command.promised());
                }
            }));
        }

        @Override
        public MessageType type()
        {
            return MessageType.ACCEPT_INVALIDATE_REQ;
        }

        @Override
        public String toString()
        {
            return "AcceptInvalidate{ballot:" + ballot + ", txnId:" + txnId + ", key:" + someKey + '}';
        }

        @Override
        public long waitForEpoch()
        {
            return txnId.epoch;
        }
    }

    public static abstract class AcceptReply implements Reply
    {
        @Override
        public MessageType type()
        {
            return MessageType.ACCEPT_RSP;
        }

        public abstract boolean isOk();
        public abstract AcceptOutcome outcome();
    }

    public static class AcceptOk extends AcceptReply
    {
        // TODO: migrate this to PartialDeps? Need to think carefully about semantics when ownership changes between txnId and executeAt
        public final Deps deps;

        public AcceptOk(Deps deps)
        {
            this.deps = deps;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public AcceptOutcome outcome()
        {
            return Success;
        }

        @Override
        public String toString()
        {
            return "AcceptOk{deps=" + deps + '}';
        }
    }

    public static class AcceptNack extends AcceptReply
    {
        public static final AcceptNack REDUNDANT = new AcceptNack(AcceptOutcome.Redundant, null);
        public static final AcceptNack INSUFFICIENT = new AcceptNack(AcceptOutcome.Insufficient, null);

        public final AcceptOutcome outcome;
        public final Timestamp supersededBy;

        public AcceptNack(AcceptOutcome outcome, Timestamp supersededBy)
        {
            this.outcome = outcome;
            this.supersededBy = supersededBy;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public AcceptOutcome outcome()
        {
            return outcome;
        }

        @Override
        public String toString()
        {
            return "AcceptNack{" + outcome + ",supersededBy=" + supersededBy + '}';
        }
    }
}
