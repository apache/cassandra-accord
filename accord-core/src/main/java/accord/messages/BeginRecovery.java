package accord.messages;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Txn;
import accord.topology.Topologies;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.primitives.Keys;
import accord.primitives.Writes;
import accord.primitives.Ballot;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Timestamp;
import accord.local.Command;
import accord.primitives.Deps;
import accord.local.Status;
import accord.primitives.TxnId;

import static accord.local.Status.Accepted;
import static accord.local.Status.AcceptedInvalidate;
import static accord.local.Status.Committed;
import static accord.local.Status.PreAccepted;
import static accord.messages.PreAccept.calculateDeps;

public class BeginRecovery extends TxnRequest
{
    final TxnId txnId;
    final PartialTxn txn;
    final Ballot ballot;
    final @Nullable Route route;

    public BeginRecovery(Id to, Topologies topologies, TxnId txnId, Txn txn, Route route, Ballot ballot)
    {
        super(to, topologies, route);
        this.txnId = txnId;
        this.txn = txn.slice(scope.covering, scope.contains(scope.homeKey));
        this.ballot = ballot;
        this.route = scope.contains(scope.homeKey) ? route : null;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        RoutingKey progressKey = node.selectProgressKey(txnId, scope);
        RecoverReply reply = node.mapReduceLocal(scope(), txnId.epoch, txnId.epoch, instance -> {
            Command command = instance.command(txnId);

            switch (command.recover(txn, route != null ? route : scope, progressKey, ballot))
            {
                default:
                    throw new IllegalStateException("Unhandled Outcome");

                case Insufficient:
                case Redundant:
                    throw new IllegalStateException("Invalid Outcome");

                case RejectedBallot:
                    return new RecoverNack(command.promised());

                case Success:
            }

            PartialDeps deps = command.savedPartialDeps();
            if (!command.hasBeen(Accepted))
            {
                deps = calculateDeps(instance, txnId, txn.keys, txn.kind, txnId,
                                     PartialDeps.builder(instance.ranges().at(txnId.epoch), txn.keys));
            }

            boolean rejectsFastPath;
            Deps earlierCommittedWitness, earlierAcceptedNoWitness;

            if (command.hasBeen(Committed))
            {
                rejectsFastPath = false;
                earlierCommittedWitness = earlierAcceptedNoWitness = Deps.NONE;
            }
            else
            {
                rejectsFastPath = uncommittedStartedAfter(instance, txnId, txn.keys)
                                             .filter(c -> c.hasBeen(Accepted))
                                             .anyMatch(c -> !c.savedPartialDeps().contains(txnId));
                if (!rejectsFastPath)
                    rejectsFastPath = committedExecutesAfter(instance, txnId, txn.keys)
                                         .anyMatch(c -> !c.savedPartialDeps().contains(txnId));

                // committed txns with an earlier txnid and have our txnid as a dependency
                earlierCommittedWitness = committedStartedBefore(instance, txnId, txn.keys)
                                          .filter(c -> c.savedPartialDeps().contains(txnId))
                                          .collect(() -> Deps.builder(txn.keys), Deps.Builder::add, (a, b) -> { throw new IllegalStateException(); })
                                          .build();

                // accepted txns with an earlier txnid that don't have our txnid as a dependency
                earlierAcceptedNoWitness = uncommittedStartedBefore(instance, txnId, txn.keys)
                                              .filter(c -> c.is(Accepted)
                                                           && !c.savedPartialDeps().contains(txnId)
                                                           && c.executeAt().compareTo(txnId) > 0)
                                              .collect(() -> Deps.builder(txn.keys), Deps.Builder::add, (a, b) -> { throw new IllegalStateException(); })
                                              .build();
            }
            return new RecoverOk(txnId, command.status(), command.accepted(), command.executeAt(), deps, earlierCommittedWitness, earlierAcceptedNoWitness, rejectsFastPath, command.writes(), command.result());
        }, (r1, r2) -> {

            // TODO: should not operate on dependencies directly here, as we only merge them;
            //       should have a cheaply mergeable variant (or should collect them before merging)

            if (!r1.isOk()) return r1;
            if (!r2.isOk()) return r2;
            RecoverOk ok1 = (RecoverOk) r1;
            RecoverOk ok2 = (RecoverOk) r2;

            // set ok1 to the most recent of the two
            if (ok1.status.compareTo(ok2.status) < 0 || (ok1.status == ok2.status && ok1.accepted.compareTo(ok2.accepted) < 0))
            {
                RecoverOk tmp = ok1;
                ok1 = ok2;
                ok2 = tmp;
            }
            if (!ok1.status.hasBeen(PreAccepted)) throw new IllegalStateException();

            PartialDeps deps = ok1.deps.with(ok2.deps);
            Deps earlierCommittedWitness = ok1.earlierCommittedWitness.with(ok2.earlierCommittedWitness);
            Deps earlierAcceptedNoWitness = ok1.earlierAcceptedNoWitness.with(ok2.earlierAcceptedNoWitness)
                                                                        .without(earlierCommittedWitness::contains);
            Timestamp timestamp = ok1.status == PreAccepted ? Timestamp.max(ok1.executeAt, ok2.executeAt) : ok1.executeAt;

            return new RecoverOk(
                    txnId, ok1.status, Ballot.max(ok1.accepted, ok2.accepted), timestamp, deps,
                    earlierCommittedWitness, earlierAcceptedNoWitness,
                    ok1.rejectsFastPath | ok2.rejectsFastPath,
                    ok1.writes, ok1.result);
        });

        node.reply(replyToNode, replyContext, reply);
    }

    @Override
    public MessageType type()
    {
        return MessageType.BEGIN_RECOVER_REQ;
    }

    @Override
    public String toString()
    {
        return "BeginRecovery{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               ", ballot:" + ballot +
               '}';
    }

    public interface RecoverReply extends Reply
    {
        @Override
        default MessageType type()
        {
            return MessageType.BEGIN_RECOVER_RSP;
        }

        boolean isOk();
    }

    public static class RecoverOk implements RecoverReply
    {
        public final TxnId txnId; // TODO for debugging?
        public final Status status;
        public final Ballot accepted;
        public final Timestamp executeAt;
        public final PartialDeps deps;
        public final Deps earlierCommittedWitness;  // counter-point to earlierAcceptedNoWitness
        public final Deps earlierAcceptedNoWitness; // wait for these to commit
        public final boolean rejectsFastPath;
        public final Writes writes;
        public final Result result;

        public RecoverOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, PartialDeps deps, Deps earlierCommittedWitness, Deps earlierAcceptedNoWitness, boolean rejectsFastPath, Writes writes, Result result)
        {
            Preconditions.checkNotNull(deps);
            this.txnId = txnId;
            this.accepted = accepted;
            this.executeAt = executeAt;
            this.status = status;
            this.deps = deps;
            this.earlierCommittedWitness = earlierCommittedWitness;
            this.earlierAcceptedNoWitness = earlierAcceptedNoWitness;
            this.rejectsFastPath = rejectsFastPath;
            this.writes = writes;
            this.result = result;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toString("RecoverOk");
        }

        String toString(String kind)
        {
            return kind + "{" +
                   "txnId:" + txnId +
                   ", status:" + status +
                   ", accepted:" + accepted +
                   ", executeAt:" + executeAt +
                   ", deps:" + deps +
                   ", earlierCommittedWitness:" + earlierCommittedWitness +
                   ", earlierAcceptedNoWitness:" + earlierAcceptedNoWitness +
                   ", rejectsFastPath:" + rejectsFastPath +
                   ", writes:" + writes +
                   ", result:" + result +
                   '}';
        }

        public static <T extends RecoverOk> T maxAcceptedOrLater(List<T> recoverOks)
        {
            T max = null;
            for (T ok : recoverOks)
            {
                if (!ok.status.hasBeen(AcceptedInvalidate))
                    continue;

                boolean update =     max == null
                                  || max.status.logicalCompareTo(ok.status) < 0
                                  || (ok.status.logicalCompareTo(Accepted) == 0 && max.accepted.compareTo(ok.accepted) < 0);
                if (update)
                    max = ok;
            }
            return max;

        }
    }

    public static class RecoverNack implements RecoverReply
    {
        public final Ballot supersededBy;
        public RecoverNack(Ballot supersededBy)
        {
            this.supersededBy = supersededBy;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "RecoverNack{" +
                   "supersededBy:" + supersededBy +
                   '}';
        }
    }

    private static Stream<Command> uncommittedStartedBefore(CommandStore commandStore, TxnId startedBefore, Keys keys)
    {
        return keys.stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
            if (forKey == null)
                return Stream.of();
            return forKey.uncommitted.headMap(startedBefore, false).values().stream();
        });
    }

    private static Stream<Command> committedStartedBefore(CommandStore commandStore, TxnId startedBefore, Keys keys)
    {
        return keys.stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
            if (forKey == null)
                return Stream.of();
            return forKey.committedById.headMap(startedBefore, false).values().stream();
        });
    }

    private static Stream<Command> uncommittedStartedAfter(CommandStore commandStore, TxnId startedAfter, Keys keys)
    {
        return keys.stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
            if (forKey == null)
                return Stream.of();
            return forKey.uncommitted.tailMap(startedAfter, false).values().stream();
        });
    }

    private static Stream<Command> committedExecutesAfter(CommandStore commandStore, TxnId startedAfter, Keys keys)
    {
        return keys.stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
            if (forKey == null)
                return Stream.of();
            return forKey.committedByExecuteAt.tailMap(startedAfter, false).values().stream();
        });
    }
}
