package accord.messages;

import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.RoutingKey;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.TxnRequest.WithUnsynced;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.topology.Topologies;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.local.Command;
import accord.primitives.Deps;
import accord.primitives.Txn;
import accord.primitives.TxnId;

public class PreAccept extends WithUnsynced
{
    public final PartialTxn txn;
    public final @Nullable Route route; // ordinarily only set on home shard
    public final long maxEpoch;

    public PreAccept(Id to, Topologies topologies, TxnId txnId, Txn txn, Route route)
    {
        super(to, topologies, txnId, route);
        this.txn = txn.slice(scope.covering, route.contains(route.homeKey));
        this.maxEpoch = topologies.currentEpoch();
        this.route = scope.contains(scope.homeKey) ? route : null;
    }

    @VisibleForTesting
    public PreAccept(PartialRoute scope, long epoch, TxnId txnId, PartialTxn txn, @Nullable Route route)
    {
        super(scope, epoch, txnId);
        this.txn = txn;
        this.maxEpoch = epoch;
        this.route = route;
    }

    public void process(Node node, Id from, ReplyContext replyContext)
    {
        // TODO: verify we handle all of the scope() keys
        RoutingKey progressKey = progressKey(node, scope.homeKey);
        node.reply(from, replyContext, node.mapReduceLocal(scope(), minEpoch, maxEpoch, instance -> {
            // note: this diverges from the paper, in that instead of waiting for JoinShard,
            //       we PreAccept to both old and new topologies and require quorums in both.
            //       This necessitates sending to ALL replicas of old topology, not only electorate (as fast path may be unreachable).
            Command command = instance.command(txnId);
            switch (command.preaccept(txn, route != null ? route : scope, progressKey))
            {
                default:
                case Insufficient:
                    throw new IllegalStateException();

                case Success:
                case Redundant:
                     return new PreAcceptOk(txnId, command.executeAt(), calculateDeps(instance, txnId, txn.keys, txn.kind, txnId,
                                                                                     PartialDeps.builder(instance.ranges().at(txnId.epoch), txn.keys)));

                case RejectedBallot:
                    return PreAcceptNack.INSTANCE;
            }
        }, (r1, r2) -> {
            if (!r1.isOk()) return r1;
            if (!r2.isOk()) return r2;
            PreAcceptOk ok1 = (PreAcceptOk) r1;
            PreAcceptOk ok2 = (PreAcceptOk) r2;
            PreAcceptOk okMax = ok1.witnessedAt.compareTo(ok2.witnessedAt) >= 0 ? ok1 : ok2;
            Deps deps = ok1.deps.with(ok2.deps);
            if (deps == okMax.deps)
                return okMax;
            return new PreAcceptOk(txnId, okMax.witnessedAt, deps);
        }));
    }

    @Override
    public MessageType type()
    {
        return MessageType.PREACCEPT_REQ;
    }

    public interface PreAcceptReply extends Reply
    {
        @Override
        default MessageType type()
        {
            return MessageType.PREACCEPT_RSP;
        }

        boolean isOk();
    }

    public static class PreAcceptOk implements PreAcceptReply
    {
        public final TxnId txnId;
        public final Timestamp witnessedAt;
        public final Deps deps;

        public PreAcceptOk(TxnId txnId, Timestamp witnessedAt, Deps deps)
        {
            this.txnId = txnId;
            this.witnessedAt = witnessedAt;
            this.deps = deps;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PreAcceptOk that = (PreAcceptOk) o;
            return witnessedAt.equals(that.witnessedAt) && deps.equals(that.deps);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(witnessedAt, deps);
        }

        @Override
        public String toString()
        {
            return "PreAcceptOk{" +
                    "txnId:" + txnId +
                    ", witnessedAt:" + witnessedAt +
                    ", deps:" + deps +
                    '}';
        }
    }

    public static class PreAcceptNack implements PreAcceptReply
    {
        public static final PreAcceptNack INSTANCE = new PreAcceptNack();

        private PreAcceptNack() {}

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "PreAcceptNack{}";
        }
    }

    static <T extends Deps> T calculateDeps(CommandStore commandStore, TxnId txnId, Keys keys, Txn.Kind kindOfTxn, Timestamp executeAt, Deps.AbstractBuilder<T> builder)
    {
        // TODO (now): we should only adopt dependency edges where one or the other modifies *a given key*
        forEachConflictThatMayExecuteBefore(commandStore, executeAt, keys, conflict -> {
            if (conflict.txnId().equals(txnId))
                return;

            if (kindOfTxn.isWrite() || conflict.partialTxn().isWrite())
                builder.add(conflict);
        });
        return builder.build();
    }

    @Override
    public String toString()
    {
        return "PreAccept{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               ", scope:" + scope +
               '}';
    }

    private static void forEachConflictThatMayExecuteBefore(CommandStore commandStore, Timestamp mayExecuteBefore, Keys keys, Consumer<Command> forEach)
    {
        keys.forEach(key -> {
            CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
            if (forKey == null)
                return;

            forKey.uncommitted.headMap(mayExecuteBefore, false).values().forEach(forEach);
            // TODO: only return latest of Committed?
            forKey.committedByExecuteAt.headMap(mayExecuteBefore, false).values().forEach(forEach);
        });
    }

}
