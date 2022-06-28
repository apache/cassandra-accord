package accord.messages;

import java.util.Objects;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Key;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.TxnRequest.WithUnsynced;
import accord.topology.Topologies;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.local.Command;
import accord.primitives.Deps;
import accord.txn.Txn;
import accord.primitives.TxnId;

public class PreAccept extends WithUnsynced
{
    public final Key homeKey;
    public final Txn txn;
    public final long maxEpoch;

    public PreAccept(Id to, Topologies topologies, TxnId txnId, Txn txn, Key homeKey)
    {
        super(to, topologies, txn.keys, txnId);
        this.homeKey = homeKey;
        this.txn = txn;
        this.maxEpoch = topologies.currentEpoch();
    }

    @VisibleForTesting
    public PreAccept(Keys scope, long epoch, TxnId txnId, Txn txn, Key homeKey)
    {
        super(scope, epoch, txnId);
        this.homeKey = homeKey;
        this.txn = txn;
        this.maxEpoch = epoch;
    }

    public void process(Node node, Id from, ReplyContext replyContext)
    {
        // TODO: verify we handle all of the scope() keys
        Key progressKey = progressKey(node, homeKey);
        node.reply(from, replyContext, node.mapReduceLocal(scope(), minEpoch, maxEpoch, instance -> {
            // note: this diverges from the paper, in that instead of waiting for JoinShard,
            //       we PreAccept to both old and new topologies and require quorums in both.
            //       This necessitates sending to ALL replicas of old topology, not only electorate (as fast path may be unreachable).
            Command command = instance.command(txnId);
            if (!command.preaccept(txn, homeKey, progressKey))
                return PreAcceptNack.INSTANCE;
            return new PreAcceptOk(txnId, command.executeAt(), calculateDeps(instance, txnId, txn, txnId));
        }, (r1, r2) -> {
            if (!r1.isOK()) return r1;
            if (!r2.isOK()) return r2;
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

        boolean isOK();
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
        public boolean isOK()
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
        public boolean isOK()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "PreAcceptNack{}";
        }
    }

//    static Dependencies calculateDeps(CommandStore commandStore, TxnId txnId, Txn txn, Timestamp executeAt)
//    {
//        return calculateDeps(commandStore, txnId, txn.keys, txn.update.keys(), executeAt);
//    }
//
    static Deps calculateDeps(CommandStore commandStore, TxnId txnId, Txn txn, Timestamp executeAt)
    {
        // TODO (now): do not use Txn
        Deps.Builder deps = Deps.builder(txn.keys);
        conflictsMayExecuteBefore(commandStore, executeAt, txn.keys).forEach(conflict -> {
            if (conflict.txnId().equals(txnId))
                return;

            if (txn.isWrite() || conflict.txn().isWrite())
                deps.add(conflict);
        });
        return deps.build();
    }

    @Override
    public String toString()
    {
        return "PreAccept{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               ", homeKey:" + homeKey +
               '}';
    }

    private static Stream<Command> conflictsMayExecuteBefore(CommandStore commandStore, Timestamp mayExecuteBefore, Keys keys)
    {
        return keys.stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
            if (forKey == null)
                return Stream.of();

            return Stream.concat(
                forKey.uncommitted.headMap(mayExecuteBefore, false).values().stream(),
                // TODO: only return latest of Committed?
                forKey.committedByExecuteAt.headMap(mayExecuteBefore, false).values().stream()
            );
        });
    }

}
