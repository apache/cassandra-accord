package accord.messages;

import java.util.Set;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

public class InformHomeDurable implements Request
{
    final TxnId txnId;
    final RoutingKey homeKey;
    final Timestamp executeAt;
    final Set<Id> persistedOn;

    public InformHomeDurable(TxnId txnId, RoutingKey homeKey, Timestamp executeAt, Set<Id> persistedOn)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
        this.persistedOn = persistedOn;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        node.ifLocal(homeKey, txnId, instance -> {
            instance.command(txnId).setGloballyPersistent(homeKey, executeAt);
            instance.progressLog().durable(txnId, homeKey, persistedOn);
            return null;
        });
    }

    @Override
    public String toString()
    {
        return "InformHomeDurable{txnId:" + txnId + '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.INFORM_HOME_DURABLE_REQ;
    }
}
