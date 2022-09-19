package accord.messages;

import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.utils.MapReduceConsume;

import java.util.Collections;

public abstract class AbstractEpochRequest<R extends Reply> implements PreLoadContext, Request, MapReduceConsume<SafeCommandStore, R>
{
    public final TxnId txnId;
    protected transient Node node;
    protected transient Node.Id replyTo;
    protected transient ReplyContext replyContext;

    protected AbstractEpochRequest(TxnId txnId)
    {
        this.txnId = txnId;
    }

    @Override
    public void process(Node on, Node.Id replyTo, ReplyContext replyContext)
    {
        this.node = on;
        this.replyTo = replyTo;
        this.replyContext = replyContext;
        process();
    }

    protected abstract void process();

    @Override
    public R reduce(R o1, R o2)
    {
        throw new IllegalStateException();
    }

    @Override
    public void accept(R reply, Throwable failure)
    {
        node.reply(replyTo, replyContext, reply);
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
