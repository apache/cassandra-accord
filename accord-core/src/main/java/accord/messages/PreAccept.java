package accord.messages;

import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.txn.Timestamp;
import accord.local.Command;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;

public class PreAccept implements Request
{
    public final TxnId txnId;
    public final Txn txn;

    public PreAccept(TxnId txnId, Txn txn)
    {
        this.txnId = txnId;
        this.txn = txn;
    }

    public void process(Node node, Id from, long messageId)
    {
        node.reply(from, messageId, txn.local(node).map(instance -> {
            Command command = instance.command(txnId);
            if (!command.witness(txn))
                return PreAcceptNack.INSTANCE;
            // TODO: only lookup keys relevant to this instance
            // TODO: why don't we calculate deps from the executeAt timestamp??
            // TODO: take action if the epoch is stale??
            return new PreAcceptOk(command.executeAt(), calculateDeps(instance, txnId, txn, txnId));
        }).reduce((r1, r2) -> {
            if (!r1.isOK()) return r1;
            if (!r2.isOK()) return r2;
            PreAcceptOk ok1 = (PreAcceptOk) r1;
            PreAcceptOk ok2 = (PreAcceptOk) r2;
            PreAcceptOk okMax = ok1.witnessedAt.compareTo(ok2.witnessedAt) >= 0 ? ok1 : ok2;
            if (ok1 != okMax && !ok1.deps.isEmpty()) okMax.deps.addAll(ok1.deps);
            if (ok2 != okMax && !ok2.deps.isEmpty()) okMax.deps.addAll(ok2.deps);
            return okMax;
        }).orElseThrow());
    }

    public interface PreAcceptReply extends Reply
    {
        boolean isOK();
    }

    public static class PreAcceptOk implements PreAcceptReply
    {
        public final Timestamp witnessedAt;
        public final Dependencies deps;

        public PreAcceptOk(Timestamp witnessedAt, Dependencies deps)
        {
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
                    "witnessedAt=" + witnessedAt +
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
