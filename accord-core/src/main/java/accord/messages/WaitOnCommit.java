package accord.messages;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import accord.local.*;
import accord.local.Node.Id;
import accord.topology.Topologies;
import accord.txn.TxnId;
import accord.txn.Keys;

public class WaitOnCommit extends TxnRequest
{
    static class LocalWait implements Listener
    {
        final Node node;
        final Id replyToNode;
        final TxnId txnId;
        final ReplyContext replyContext;

        final AtomicInteger waitingOn = new AtomicInteger();

        LocalWait(Node node, Id replyToNode, TxnId txnId, ReplyContext replyContext)
        {
            this.node = node;
            this.replyToNode = replyToNode;
            this.txnId = txnId;
            this.replyContext = replyContext;
        }

        @Override
        public synchronized void onChange(Command command)
        {
            switch (command.status())
            {
                default:
                    throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                    return;

                case Committed:
                case Executed:
                case Applied:
                case ReadyToExecute:
            }

            command.removeListener(this);
            ack();
        }

        private void ack()
        {
            if (waitingOn.decrementAndGet() == 0)
                node.reply(replyToNode, replyContext, WaitOnCommitOk.INSTANCE);
        }

        void setup(CommandStore instance)
        {
            Command command = instance.command(txnId);
            switch (command.status())
            {
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                    command.addListener(this);
                    break;

                case Committed:
                case Executed:
                case Applied:
                case ReadyToExecute:
                    ack();
            }
        }

        synchronized void setup(Keys keys)
        {
            List<CommandStore> instances = node.collectLocal(keys, txnId, ArrayList::new);
            waitingOn.set(instances.size());
            instances.forEach(instance -> instance.processBlocking(this::setup));
        }
    }

    public final TxnId txnId;
    public final Keys keys; // TODO (now): redundant with TxnRequest.Scope

    public WaitOnCommit(Id to, Topologies topologies, TxnId txnId, Keys keys)
    {
        super(to, topologies, keys);
        this.txnId = txnId;
        this.keys = keys;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        new LocalWait(node, replyToNode, txnId, replyContext).setup(keys);
    }

    @Override
    public MessageType type()
    {
        return MessageType.WAIT_ON_COMMIT_REQ;
    }

    public static class WaitOnCommitOk implements Reply
    {
        public static final WaitOnCommitOk INSTANCE = new WaitOnCommitOk();

        private WaitOnCommitOk() {}

        @Override
        public MessageType type()
        {
            return MessageType.WAIT_ON_COMMIT_RSP;
        }
    }
}
