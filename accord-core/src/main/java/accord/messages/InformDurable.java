package accord.messages;

import accord.api.ProgressLog.ProgressShard;
import accord.local.Command;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.Status.Durability;
import accord.primitives.*;
import accord.topology.Topologies;

import java.util.Collections;

import static accord.api.ProgressLog.ProgressShard.Adhoc;
import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Local;
import static accord.local.PreLoadContext.contextFor;
import static accord.messages.SimpleReply.Ok;

public class InformDurable extends TxnRequest<Reply> implements PreLoadContext
{
    public static class SerializationSupport
    {
        public static InformDurable create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Timestamp executeAt, Durability durability)
        {
            return new InformDurable(txnId, scope, waitForEpoch, executeAt, durability);
        }
    }

    public final Timestamp executeAt;
    public final Durability durability;
    private transient ProgressShard shard;

    public InformDurable(Id to, Topologies topologies, Route<?> route, TxnId txnId, Timestamp executeAt, Durability durability)
    {
        super(to, topologies, route, txnId);
        this.executeAt = executeAt;
        this.durability = durability;
    }

    private InformDurable(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Timestamp executeAt, Durability durability)
    {
        super(txnId, scope, waitForEpoch);
        this.executeAt = executeAt;
        this.durability = durability;
    }

    public void process()
    {
        Timestamp at = txnId;
        if (progressKey == null)
        {
            // we need to pick a progress log, but this node might not have participated in the coordination epoch
            // in this rare circumstance we simply pick a key to select some progress log to coordinate this
            // TODO (now): We might not replicate either txnId.epoch OR executeAt.epoch, but some inbetween.
            //             Do we need to receive this message in that case? If so, we need to account for this when selecting a progress key
            at = executeAt;
            progressKey = node.selectProgressKey(executeAt.epoch, scope, scope.homeKey());
            shard = Adhoc;
        }
        else
        {
            shard = scope.homeKey().equals(progressKey) ? Home : Local;
        }

        // TODO (soon): do not load from disk to perform this update
        node.mapReduceConsumeLocal(contextFor(txnId), progressKey, at.epoch, this);
    }

    @Override
    public Reply apply(SafeCommandStore safeStore)
    {
        Command command = safeStore.command(txnId);
        command.setDurability(safeStore, durability, scope.homeKey(), executeAt);
        safeStore.progressLog().durable(txnId, scope, shard);
        return Ok;
    }

    @Override
    public Reply reduce(Reply o1, Reply o2)
    {
        throw new IllegalStateException();
    }

    @Override
    public void accept(Reply reply, Throwable failure)
    {
        if (reply == null) throw new IllegalStateException();
        node.reply(replyTo, replyContext, reply);
    }

    @Override
    public String toString()
    {
        return "InformOfPersistence{" +
               "txnId:" + txnId +
               '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.INFORM_DURABLE_REQ;
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return Keys.EMPTY;
    }
}
