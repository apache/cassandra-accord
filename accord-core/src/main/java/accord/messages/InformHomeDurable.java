package accord.messages;

import java.util.Set;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.local.PreLoadContext.contextFor;

public class InformHomeDurable implements Request
{
    public final TxnId txnId;
    public final RoutingKey homeKey;
    public final Timestamp executeAt;
    public final Durability durability;
    public final Set<Id> persistedOn;

    public InformHomeDurable(TxnId txnId, RoutingKey homeKey, Timestamp executeAt, Durability durability, Set<Id> persistedOn)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
        this.durability = durability;
        this.persistedOn = persistedOn;
    }

    @Override
    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        // TODO (expected, efficiency): do not load txnId first
        node.ifLocal(contextFor(txnId), homeKey, txnId.epoch(), safeStore -> {
            Command command = safeStore.command(txnId);
            command.setDurability(safeStore, durability, homeKey, executeAt);
            safeStore.progressLog().durable(command, persistedOn);
        }).addCallback(node.agent());
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
