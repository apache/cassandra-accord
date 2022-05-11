package accord.messages;

import accord.api.Result;
import accord.coordinate.Persist;
import accord.topology.Topologies;

import java.util.List;
import java.util.stream.Stream;

import accord.api.Key;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.primitives.Keys;
import accord.txn.Writes;
import accord.primitives.Ballot;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Timestamp;
import accord.local.Command;
import accord.primitives.Deps;
import accord.local.Status;
import accord.txn.Txn;
import accord.primitives.TxnId;
import com.google.common.base.Preconditions;

import static accord.local.Status.Accepted;
import static accord.local.Status.Applied;
import static accord.local.Status.Committed;
import static accord.local.Status.NotWitnessed;
import static accord.local.Status.PreAccepted;
import static accord.messages.PreAccept.calculateDeps;

public class BeginRecovery extends TxnRequest
{
    final TxnId txnId;
    final Txn txn;
    final Key homeKey;
    final Ballot ballot;

    public BeginRecovery(Id to, Topologies topologies, TxnId txnId, Txn txn, Key homeKey, Ballot ballot)
    {
        super(to, topologies, txn.keys);
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.ballot = ballot;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Key progressKey = node.selectProgressKey(txnId, txn.keys, homeKey);
        RecoverReply reply = node.mapReduceLocal(scope(), txnId.epoch, txnId.epoch, instance -> {
            Command command = instance.command(txnId);

            if (!command.recover(txn, homeKey, progressKey, ballot))
                return new RecoverNack(command.promised());

            Deps deps = command.status() == PreAccepted ? calculateDeps(instance, txnId, txn, txnId)
                                                        : command.savedDeps();

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
                                             .anyMatch(c -> !c.savedDeps().contains(txnId));
                if (!rejectsFastPath)
                    rejectsFastPath = committedExecutesAfter(instance, txnId, txn.keys)
                                         .anyMatch(c -> !c.savedDeps().contains(txnId));

                // committed txns with an earlier txnid and have our txnid as a dependency
                earlierCommittedWitness = committedStartedBefore(instance, txnId, txn.keys)
                                          .filter(c -> c.savedDeps().contains(txnId))
                                          .collect(() -> Deps.builder(txn.keys), Deps.Builder::add, (a, b) -> { throw new IllegalStateException(); })
                                          .build();

                // accepted txns with an earlier txnid that don't have our txnid as a dependency
                earlierAcceptedNoWitness = uncommittedStartedBefore(instance, txnId, txn.keys)
                                              .filter(c -> c.is(Accepted)
                                                           && !c.savedDeps().contains(txnId)
                                                           && c.executeAt().compareTo(txnId) > 0)
                                              .collect(() -> Deps.builder(txn.keys), Deps.Builder::add, (a, b) -> { throw new IllegalStateException(); })
                                              .build();
            }
            return new RecoverOk(txnId, command.status(), command.accepted(), command.executeAt(), deps, earlierCommittedWitness, earlierAcceptedNoWitness, rejectsFastPath, command.writes(), command.result());
        }, (r1, r2) -> { // TODO: reduce function that constructs a list to reduce at the end
            if (!r1.isOK()) return r1;
            if (!r2.isOK()) return r2;
            RecoverOk ok1 = (RecoverOk) r1;
            RecoverOk ok2 = (RecoverOk) r2;

            // set ok1 to the most recent of the two
            if (ok1.status.compareTo(ok2.status) < 0)
            { RecoverOk tmp = ok1; ok1 = ok2; ok2 = tmp; }

            switch (ok1.status)
            {
                default: throw new IllegalStateException();
                case PreAccepted:
                    if (ok2.status == NotWitnessed)
                        throw new IllegalStateException();
                    break;

                case Accepted:
                case AcceptedInvalidate:
                    // we currently replicate all deps to every shard, so all Accepted should have the same information
                    // but we must pick the one with the newest ballot
                    if (ok2.status.logicalCompareTo(ok1.status) == 0)
                        return ok1.accepted.compareTo(ok2.accepted) >= 0 ? ok1 : ok2;

                case Committed:
                case ReadyToExecute:
                case Executed:
                case Applied:
                case Invalidated:
                    // we currently replicate all deps to every shard, so all Committed should have the same information
                    return ok1;
            }

            // ok1 and ok2 both PreAccepted
            Deps deps = ok1.deps.with(ok2.deps);
            Deps earlierCommittedWitness = ok1.earlierCommittedWitness.with(ok2.earlierCommittedWitness);
            Deps earlierAcceptedNoWitness = ok1.earlierAcceptedNoWitness.with(ok2.earlierAcceptedNoWitness)
                                                                        .without(earlierCommittedWitness::contains);
            return new RecoverOk(
                    txnId, ok1.status,
                    Ballot.max(ok1.accepted, ok2.accepted),
                    Timestamp.max(ok1.executeAt, ok2.executeAt),
                    deps,
                    earlierCommittedWitness,
                    earlierAcceptedNoWitness,
                    ok1.rejectsFastPath | ok2.rejectsFastPath,
                    ok1.writes, ok1.result);
        });

        node.reply(replyToNode, replyContext, reply);
        if (reply instanceof RecoverOk && ((RecoverOk) reply).status == Applied)
        {
            // TODO: this should be a call to the node's agent for the implementation to decide what to do;
            //       probably at most want to disseminate to local replicas, or notify the progress log
            RecoverOk ok = (RecoverOk) reply;
            Preconditions.checkArgument(ok.status == Applied);
            node.withEpoch(ok.executeAt.epoch, () -> {
                Persist.persistAndCommit(node, txnId, homeKey, txn, ok.executeAt, ok.deps, ok.writes, ok.result);
            });
        }
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

        boolean isOK();
    }

    public static class RecoverOk implements RecoverReply
    {
        public final TxnId txnId; // TODO for debugging?
        public final Status status;
        public final Ballot accepted;
        public final Timestamp executeAt;
        public final Deps deps;
        public final Deps earlierCommittedWitness;  // counter-point to earlierAcceptedNoWitness
        public final Deps earlierAcceptedNoWitness; // wait for these to commit
        public final boolean rejectsFastPath;
        public final Writes writes;
        public final Result result;

        public RecoverOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, Deps deps, Deps earlierCommittedWitness, Deps earlierAcceptedNoWitness, boolean rejectsFastPath, Writes writes, Result result)
        {
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
        public boolean isOK()
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
                if (ok.status.hasBeen(Accepted))
                {
                    if (max == null)
                    {
                        max = ok;
                    }
                    else
                    {
                        int c = max.status.logicalCompareTo(ok.status);
                        if (c < 0) max = ok;
                        else if (c == 0 && max.accepted.compareTo(ok.accepted) < 0) max = ok;
                    }
                }
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
        public boolean isOK()
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
