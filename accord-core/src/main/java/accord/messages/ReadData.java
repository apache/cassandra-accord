package accord.messages;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import accord.local.*;
import accord.local.Node.Id;
import accord.api.Data;
import accord.topology.KeyRanges;
import accord.topology.Topologies;
import accord.txn.Keys;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Timestamp;
import accord.api.Scheduler.Scheduled;
import accord.utils.DeterministicIdentitySet;

public class ReadData extends TxnRequest
{
    static class LocalRead implements Listener
    {
        final TxnId txnId;
        final Node node;
        final Node.Id replyToNode;
        final Keys keyScope;
        final ReplyContext replyContext;

        Data data;
        boolean isObsolete; // TODO: respond with the Executed result we have stored?
        Set<CommandStore> waitingOn;
        Scheduled waitingOnReporter;

        LocalRead(TxnId txnId, Node node, Id replyToNode, Keys keyScope, ReplyContext replyContext)
        {
            this.txnId = txnId;
            this.node = node;
            this.replyToNode = replyToNode;
            this.keyScope = keyScope;
            this.replyContext = replyContext;
            // TODO: this is messy, we want a complete separate liveness mechanism that ensures progress for all transactions
            this.waitingOnReporter = node.scheduler().once(new ReportWaiting(), 1L, TimeUnit.SECONDS);
        }

        class ReportWaiting implements Listener, Runnable
        {
            @Override
            public void onChange(Command command)
            {
                command.removeListener(this);
                run();
            }

            @Override
            public void run()
            {
                Iterator<CommandStore> i = waitingOn.iterator();
                Command blockedBy = null;
                while (i.hasNext() && null == (blockedBy = i.next().command(txnId).blockedBy()));
                if (blockedBy == null) return;
                blockedBy.addListener(this);
                assert blockedBy.status().compareTo(Status.NotWitnessed) > 0;
                node.reply(replyToNode, replyContext, new ReadWaiting(txnId, blockedBy.txnId(), blockedBy.txn(), blockedBy.executeAt(), blockedBy.status()));
            }
        }

        @Override
        public synchronized void onChange(Command command)
        {
            switch (command.status())
            {
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case Committed:
                    return;

                case Executed:
                case Applied:
                    obsolete(command);
                case ReadyToExecute:
            }

            command.removeListener(this);
            if (!isObsolete)
                read(command);
        }

        private void read(Command command)
        {
            // TODO: threading/futures (don't want to perform expensive reads within this mutually exclusive context)
            Data next = command.txn().read(command, keyScope);
            data = data == null ? next : data.merge(next);

            waitingOn.remove(command.commandStore);
            if (waitingOn.isEmpty())
            {
                waitingOnReporter.cancel();
                node.reply(replyToNode, replyContext, new ReadOk(data));
            }
        }

        void obsolete(Command command)
        {
            if (!isObsolete)
            {
                isObsolete = true;
                waitingOnReporter.cancel();
                Set<Node.Id> nodes = new HashSet<>();
                Topologies topologies = node.topology().forKeys(command.txn().keys());
                KeyRanges ranges = command.commandStore.ranges();
                topologies.forEachShard(shard -> {
                    if (ranges.intersects(shard.range))
                        nodes.addAll(shard.nodes);
                });
                // FIXME: this may result in redundant messages being sent when a shard is split across several command shards
                node.send(nodes, to -> new Apply(to, topologies, command.txnId(), command.txn(), command.executeAt(), command.savedDeps(), command.writes(), command.result()));
                node.reply(replyToNode, replyContext, new ReadNack());
            }
        }

        synchronized void setup(TxnId txnId, Txn txn, Scope scope)
        {
            // TODO: simple hash set supporting concurrent modification, or else avoid concurrent modification
            waitingOn = node.collectLocal(scope, DeterministicIdentitySet::new);
            // FIXME: fix/check thread safety
            CommandStore.onEach(waitingOn, instance -> {
                Command command = instance.command(txnId);
                command.witness(txn);
                switch (command.status())
                {
                    case NotWitnessed:
                        throw new IllegalStateException();
                    case PreAccepted:
                    case Accepted:
                    case Committed:
                        command.addListener(this);
                        break;

                    case Executed:
                    case Applied:
                        obsolete(command);
                        break;

                    case ReadyToExecute:
                        if (!isObsolete)
                            read(command);
                }
            });
        }
    }

    public final TxnId txnId;
    public final Txn txn;
    public final Timestamp executeAt;

    public ReadData(Scope scope, TxnId txnId, Txn txn, Timestamp executeAt)
    {
        super(scope);
        this.txnId = txnId;
        this.txn = txn;
        this.executeAt = executeAt;
    }

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Txn txn, Timestamp executeAt)
    {
        this(Scope.forTopologies(to, topologies, txn), txnId, txn, executeAt);
    }

    public void process(Node node, Node.Id from, ReplyContext replyContext)
    {
        new LocalRead(txnId, node, from, scope().keys(), replyContext).setup(txnId, txn, scope());
    }

    @Override
    public MessageType type()
    {
        return MessageType.READ_REQ;
    }

    public static class ReadReply implements Reply
    {
        @Override
        public MessageType type()
        {
            return MessageType.READ_RSP;
        }

        public boolean isOK()
        {
            return true;
        }
    }

    public static class ReadNack extends ReadReply
    {
        @Override
        public boolean isOK()
        {
            return false;
        }
    }

    public static class ReadOk extends ReadReply
    {
        public final Data data;
        public ReadOk(Data data)
        {
            this.data = data;
        }

        @Override
        public String toString()
        {
            return "ReadOk{" + data + '}';
        }
    }

    public static class ReadWaiting extends ReadReply
    {
        public final TxnId forTxn;
        public final TxnId txnId;
        public final Txn txn;
        public final Timestamp executeAt;
        public final Status status;

        public ReadWaiting(TxnId forTxn, TxnId txnId, Txn txn, Timestamp executeAt, Status status)
        {
            this.forTxn = forTxn;
            this.txnId = txnId;
            this.txn = txn;
            this.executeAt = executeAt;
            this.status = status;
        }

        @Override
        public boolean isFinal()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "ReadWaiting{" +
                   "forTxn:" + forTxn +
                   ", txnId:" + txnId +
                   ", txn:" + txn +
                   ", executeAt:" + executeAt +
                   ", status:" + status +
                   '}';
        }
    }

    @Override
    public String toString()
    {
        return "ReadData{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               '}';
    }
}
