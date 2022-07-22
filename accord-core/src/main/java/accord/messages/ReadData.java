package accord.messages;

import java.util.Set;

import accord.local.*;
import accord.local.Node.Id;
import accord.api.Data;
import accord.primitives.PartialRoute;
import accord.primitives.Route;
import accord.topology.Topologies;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.DeterministicIdentitySet;

import static accord.local.Status.Executed;
import static accord.messages.MessageType.READ_RSP;
import static accord.messages.ReadData.ReadNack.NotCommitted;
import static accord.messages.ReadData.ReadNack.Redundant;

public class ReadData extends TxnRequest
{
    // TODO: dedup - can currently have infinite pending reads that will be executed independently
    static class LocalRead implements Listener
    {
        final TxnId txnId;
        final Node node;
        final Node.Id replyToNode;
        final ReplyContext replyContext;

        Data data;
        boolean isObsolete; // TODO: respond with the Executed result we have stored?
        Set<CommandStore> waitingOn;

        LocalRead(TxnId txnId, Node node, Id replyToNode, ReplyContext replyContext)
        {
            this.txnId = txnId;
            this.node = node;
            this.replyToNode = replyToNode;
            this.replyContext = replyContext;
        }

        @Override
        public synchronized void onChange(Command command)
        {
            switch (command.status())
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                case Committed:
                    return;

                case Executed:
                case Applied:
                case Invalidated:
                    obsolete();
                case ReadyToExecute:
            }

            command.removeListener(this);
            if (!isObsolete)
                read(command);
        }

        private void read(Command command)
        {
            // TODO: threading/futures (don't want to perform expensive reads within this mutually exclusive context)
            Data next = command.partialTxn().read(command);
            data = data == null ? next : data.merge(next);

            waitingOn.remove(command.commandStore);
            if (waitingOn.isEmpty())
                node.reply(replyToNode, replyContext, new ReadOk(data));
        }

        void obsolete()
        {
            if (!isObsolete)
            {
                isObsolete = true;
                node.reply(replyToNode, replyContext, Redundant);
            }
        }

        synchronized ReadNack setup(TxnId txnId, PartialRoute scope, Timestamp executeAt)
        {
            waitingOn = node.collectLocal(scope, executeAt, DeterministicIdentitySet::new);
            // FIXME: fix/check thread safety
            return CommandStore.mapReduce(waitingOn, instance -> {
                Command command = instance.command(txnId);
                switch (command.status())
                {
                    default:
                        throw new IllegalStateException();
                    case NotWitnessed:
                    case PreAccepted:
                    case Accepted:
                    case AcceptedInvalidate:
                        return NotCommitted;

                    case Committed:
                        instance.progressLog().waiting(txnId, Executed, scope);
                        command.addListener(this);
                        return null;

                    case Executed:
                    case Applied:
                    case Invalidated:
                        isObsolete = true;
                        return Redundant;

                    case ReadyToExecute:
                        if (!isObsolete)
                            read(command);
                        return null;
                }
            }, (r1, r2) -> r1 == null || r2 == null
                           ? r1 == null ? r1 : r2
                           : r1.compareTo(r2) >= 0 ? r1 : r2
            );
        }
    }

    public final TxnId txnId;
    public final Timestamp executeAt;

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Route route, Timestamp executeAt)
    {
        super(to, topologies, route);
        this.txnId = txnId;
        this.executeAt = executeAt;
    }

    public void process(Node node, Node.Id from, ReplyContext replyContext)
    {
        ReadNack nack = new LocalRead(txnId, node, from, replyContext)
                        .setup(txnId, scope(), executeAt);

        if (nack != null)
            node.reply(from, replyContext, nack);
    }

    @Override
    public MessageType type()
    {
        return MessageType.READ_REQ;
    }

    public interface ReadReply extends Reply
    {
        boolean isOk();
    }

    public enum ReadNack implements ReadReply
    {
        Invalid, NotCommitted, Redundant;

        @Override
        public String toString()
        {
            return "Read" + name();
        }

        @Override
        public MessageType type()
        {
            return READ_RSP;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public boolean isFinal()
        {
            return this != NotCommitted;
        }
    }

    public static class ReadOk implements ReadReply
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

        @Override
        public MessageType type()
        {
            return READ_RSP;
        }

        public boolean isOk()
        {
            return true;
        }
    }

    @Override
    public String toString()
    {
        return "ReadData{" +
               "txnId:" + txnId +
               '}';
    }
}
