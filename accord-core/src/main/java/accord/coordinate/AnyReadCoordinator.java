package accord.coordinate;

import java.util.Set;

import accord.coordinate.tracking.ReadTracker;
import accord.coordinate.tracking.ReadTracker.ReadShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.primitives.TxnId;
import accord.topology.Topologies;

abstract class AnyReadCoordinator<Reply> implements Callback<Reply>
{
    enum Action { Abort, TryAlternative, Accept, Success }

    final Node node;
    final TxnId txnId;
    final ReadTracker<ReadShardTracker> tracker;
    private boolean isDone;
    private Throwable failure;

    AnyReadCoordinator(Node node, Topologies topologies, TxnId txnId)
    {
        this.node = node;
        this.txnId = txnId;
        this.tracker = new ReadTracker<>(topologies, ReadShardTracker[]::new, ReadShardTracker::new);
    }

    void start()
    {
        start(tracker.computeMinimalReadSetAndMarkInflight());
    }

    void start(Set<Id> nodes)
    {
        contact(nodes);
    }

    abstract void contact(Set<Id> nodes);
    abstract Action process(Id from, Reply reply);
    abstract void onSuccess();
    abstract void onFailure(Throwable t);

    @Override
    public void onSuccess(Id from, Reply reply)
    {
        if (isDone)
            return;

        switch (process(from, reply))
        {
            default: throw new IllegalStateException();
            case Abort:
                isDone = true;
                break;

            case TryAlternative:
                onSlowResponse(from);
                break;

            case Accept:
                if (!(tracker.recordReadSuccess(from) && tracker.hasCompletedRead()))
                    break;

            case Success:
                isDone = true;
                onSuccess();
                if (failure != null)
                    node.agent().onHandledException(failure); // TODO: introduce dedicated Agent method for this case
        }
    }

    @Override
    public void onSlowResponse(Id from)
    {
        tracker.recordSlowRead(from);
        Set<Id> readFrom = tracker.computeMinimalReadSetAndMarkInflight();
        if (readFrom != null)
            contact(readFrom);
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        if (tracker.recordReadFailure(from))
        {
            if (this.failure == null) this.failure = failure;
            else this.failure.addSuppressed(failure);

            Set<Id> readFrom = tracker.computeMinimalReadSetAndMarkInflight();
            if (readFrom != null)
            {
                contact(readFrom);
            }
            else if (tracker.hasFailed())
            {
                isDone = true;
                onFailure(this.failure);
            }
        }
    }

    @Override
    public void onCallbackFailure(Throwable failure)
    {
        isDone = true;
        if (this.failure != null)
            failure.addSuppressed(this.failure);
        onFailure(failure);
    }
}
