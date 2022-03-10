package accord.messages;

import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.topology.Topologies;
import accord.txn.Timestamp;
import accord.local.Command;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;

public class PreAccept extends TxnRequest
{
    public final TxnId txnId;
    public final Txn txn;

    public PreAccept(Scope scope, TxnId txnId, Txn txn)
    {
        super(scope);
        this.txnId = txnId;
        this.txn = txn;
    }

    public PreAccept(Id to, Topologies topologies, TxnId txnId, Txn txn)
    {
        this(Scope.forTopologies(to, topologies, txn), txnId, txn);
    }

    public void process(Node node, Id from, ReplyContext replyContext)
    {
        node.reply(from, replyContext, node.mapReduceLocal(scope(), instance -> {
            // note: this diverges from the paper, in that instead of waiting for JoinShard,
            //       we PreAccept to both old and new topologies and require quorums in both.
            //       This necessitates sending to ALL replicas of old topology, not only electorate (as fast path may be unreachable).
            Command command = instance.command(txnId);
            if (!command.witness(txn))
                return PreAcceptNack.INSTANCE;
            return new PreAcceptOk(txnId, command.executeAt(), calculateDeps(instance, txnId, txn, txnId));
        }, (r1, r2) -> {
            if (!r1.isOK()) return r1;
            if (!r2.isOK()) return r2;
            PreAcceptOk ok1 = (PreAcceptOk) r1;
            PreAcceptOk ok2 = (PreAcceptOk) r2;
            PreAcceptOk okMax = ok1.witnessedAt.compareTo(ok2.witnessedAt) >= 0 ? ok1 : ok2;
            if (ok1 != okMax && !ok1.deps.isEmpty()) okMax.deps.addAll(ok1.deps);
            if (ok2 != okMax && !ok2.deps.isEmpty()) okMax.deps.addAll(ok2.deps);
            return okMax;
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
        public final Dependencies deps;

        public PreAcceptOk(TxnId txnId, Timestamp witnessedAt, Dependencies deps)
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
                    "txnId=" + txnId +
                    ", witnessedAt=" + witnessedAt +
                    ", deps=" + deps +
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

    static Dependencies calculateDeps(CommandStore commandStore, TxnId txnId, Txn txn, Timestamp executeAt)
    {
        NavigableMap<TxnId, Txn> deps = new TreeMap<>();
        txn.conflictsMayExecuteBefore(commandStore, executeAt).forEach(conflict -> {
            if (conflict.txnId().equals(txnId))
                return;

            if (txn.isWrite() || conflict.txn().isWrite())
                deps.put(conflict.txnId(), conflict.txn());
        });
        return new Dependencies(deps);
    }

    @Override
    public String toString()
    {
        return "PreAccept{" +
               "txnId: " + txnId +
               ", txn: " + txn +
               '}';
    }
}
