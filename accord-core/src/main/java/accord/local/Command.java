package accord.local;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import accord.api.Result;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;

import static accord.local.Status.Accepted;
import static accord.local.Status.Applied;
import static accord.local.Status.Committed;
import static accord.local.Status.Executed;
import static accord.local.Status.NotWitnessed;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.ReadyToExecute;

public class Command implements Listener, Consumer<Listener>
{
    public final CommandStore commandStore;
    private final TxnId txnId;
    private Txn txn;
    private Ballot promised = Ballot.ZERO, accepted = Ballot.ZERO;
    private Timestamp executeAt;
    private Dependencies deps = new Dependencies();
    private Writes writes;
    private Result result;

    private Status status = NotWitnessed;

    private NavigableMap<TxnId, Command> waitingOnCommit;
    private NavigableMap<Timestamp, Command> waitingOnApply;

    private final Listeners listeners = new Listeners();

    public Command(CommandStore commandStore, TxnId id)
    {
        this.commandStore = commandStore;
        this.txnId = id;
    }

    public TxnId txnId()
    {
        return txnId;
    }

    public Txn txn()
    {
        return txn;
    }

    public Ballot promised()
    {
        return promised;
    }

    public Ballot accepted()
    {
        return accepted;
    }

    public Timestamp executeAt()
    {
        return executeAt;
    }

    public Dependencies savedDeps()
    {
        return deps;
    }

    public Writes writes()
    {
        return writes;
    }

    public Result result()
    {
        return result;
    }

    public Status status()
    {
        return status;
    }

    public boolean hasBeen(Status status)
    {
        return this.status.compareTo(status) >= 0;
    }

    public boolean is(Status status)
    {
        return this.status == status;
    }

    // requires that command != null
    // relies on mutual exclusion for each key
    public boolean witness(Txn txn)
    {
        if (promised.compareTo(Ballot.ZERO) > 0)
            return false;

        if (hasBeen(PreAccepted))
            return true;

        Timestamp max = txn.maxConflict(commandStore);
        // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
        //  - use a global logical clock to issue new timestamps; or
        //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
        // TODO: should the fast path be skipped on a lower epoch as well??
        Timestamp witnessed = txnId.compareTo(max) > 0 && txnId.epoch >= commandStore.epoch() ? txnId : commandStore.uniqueNow(max);

        this.txn = txn;
        this.executeAt = witnessed;
        this.status = PreAccepted;

        txn.register(commandStore, this);
        listeners.forEach(this);
        return true;
    }

    public boolean accept(Ballot ballot, Txn txn, Timestamp executeAt, Dependencies deps)
    {
        if (this.promised.compareTo(ballot) > 0)
            return false;

        if (hasBeen(Committed))
            return false;

        witness(txn);
        this.deps = deps;
        this.executeAt = executeAt;
        promised = accepted = ballot;
        status = Accepted;
        listeners.forEach(this);
        return true;
    }

    // relies on mutual exclusion for each key
    public boolean commit(Txn txn, Dependencies deps, Timestamp executeAt)
    {
        if (hasBeen(Committed))
        {
            if (executeAt.equals(this.executeAt))
                return false;

            commandStore.agent().onInconsistentTimestamp(this, this.executeAt, executeAt);
        }

        witness(txn);
        this.status = Committed;
        this.deps = deps;
        this.executeAt = executeAt;
        this.waitingOnCommit = new TreeMap<>();
        this.waitingOnApply = new TreeMap<>();

        for (TxnId id : savedDeps().on(commandStore))
        {
            Command command = commandStore.command(id);
            switch (command.status)
            {
                default:
                    throw new IllegalStateException();
                case NotWitnessed:
                    command.witness(deps.get(command.txnId));
                case PreAccepted:
                case Accepted:
                    // we don't know when these dependencies will execute, and cannot execute until we do
                    waitingOnCommit.put(id, command);
                    command.addListener(this);
                    break;
                case Committed:
                    // TODO: split into ReadyToRead and ReadyToWrite;
                    //       the distributed read can be performed as soon as those keys are ready, and in parallel with any other reads
                    //       the client can even ACK immediately after; only the write needs to be postponed until other in-progress reads complete
                case ReadyToExecute:
                case Executed:
                case Applied:
                    command.addListener(this);
                    updatePredecessor(command);
                    break;
            }
        }
        if (waitingOnCommit.isEmpty())
        {
            waitingOnCommit = null;
            if (waitingOnApply.isEmpty())
                waitingOnApply = null;
        }
        listeners.forEach(this);
        maybeExecute();
        return true;
    }

    public boolean apply(Txn txn, Dependencies deps, Timestamp executeAt, Writes writes, Result result)
    {
        if (hasBeen(Executed) && executeAt.equals(this.executeAt))
            return false;
        else if (!hasBeen(Committed))
            commit(txn, deps, executeAt);
        else if (!executeAt.equals(this.executeAt))
            commandStore.agent().onInconsistentTimestamp(this, this.executeAt, executeAt);

        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
        this.status = Executed;
        this.listeners.forEach(this);
        maybeExecute();
        return true;
    }

    public boolean recover(Txn txn, Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return false;

        witness(txn);
        this.promised = ballot;
        return true;
    }

    public Command addListener(Listener listener)
    {
        listeners.add(listener);
        return this;
    }

    public void removeListener(Listener listener)
    {
        listeners.remove(listener);
    }

    @Override
    public void onChange(Command command)
    {
        switch (command.status)
        {
            case Committed:
            case ReadyToExecute:
            case Executed:
            case Applied:
                if (waitingOnApply != null)
                {
                    updatePredecessor(command);
                    if (waitingOnCommit != null)
                    {
                        if (waitingOnCommit.remove(command.txnId) != null && waitingOnCommit.isEmpty())
                            waitingOnCommit = null;
                    }
                    if (waitingOnCommit == null && waitingOnApply.isEmpty())
                        waitingOnApply = null;
                }
                else
                {
                    command.removeListener(this);
                }
                maybeExecute();
                break;
        }
    }

    private void maybeExecute()
    {
        if (status != Committed && status != Executed)
            return;

        if (waitingOnApply != null)
            return;

        switch (status)
        {
            case Committed:
                // TODO: maintain distinct ReadyToRead and ReadyToWrite states
                status = ReadyToExecute;
                listeners.forEach(this);
                break;
            case Executed:
                writes.apply(commandStore);
                status = Applied;
                listeners.forEach(this);
        }
    }

    private void updatePredecessor(Command committed)
    {
        if (committed.executeAt.compareTo(executeAt) > 0)
        {
            // cannot be a predecessor if we execute later
            committed.removeListener(this);
        }
        else if (committed.hasBeen(Applied))
        {
            waitingOnApply.remove(committed.executeAt);
            committed.removeListener(this);
        }
        else
        {
            waitingOnApply.putIfAbsent(committed.executeAt, committed);
        }
    }

    public Command blockedBy()
    {
        Command cur = directlyBlockedBy();
        if (cur == null)
            return null;

        Command next;
        while (null != (next = cur.directlyBlockedBy()))
            cur = next;
        return cur;
    }

    private Command directlyBlockedBy()
    {
        // firstly we're waiting on every dep to commit
        while (waitingOnCommit != null)
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            Command waitingOn = waitingOnCommit.firstEntry().getValue();
            if (!waitingOn.hasBeen(Committed)) return waitingOn;
            onChange(waitingOn);
        }

        while (waitingOnApply != null)
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            Command waitingOn = waitingOnApply.firstEntry().getValue();
            if (!waitingOn.hasBeen(Applied)) return waitingOn;
            onChange(waitingOn);
        }

        return null;
    }

    @Override
    public void accept(Listener listener)
    {
        listener.onChange(this);
    }

    @Override
    public String toString()
    {
        return "Command{" +
               "txnId=" + txnId +
               ", txn=" + txn +
               ", executeAt=" + executeAt +
               ", deps=" + deps +
               ", status=" + status +
               '}';
    }
}
