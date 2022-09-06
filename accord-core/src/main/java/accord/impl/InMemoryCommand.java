package accord.impl;

import accord.api.Key;
import accord.api.Result;
import accord.local.*;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.txn.*;

import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import static accord.local.Status.NotWitnessed;

public class InMemoryCommand extends Command
{
    public final CommandStore commandStore;
    private final TxnId txnId;

    private Key homeKey, progressKey;
    private Txn txn;
    private Ballot promised = Ballot.ZERO, accepted = Ballot.ZERO;
    private Timestamp executeAt;
    private Deps deps = Deps.NONE;
    private Writes writes;
    private Result result;

    private Status status = NotWitnessed;

    private boolean isGloballyPersistent; // only set on home shard

    private NavigableMap<TxnId, Command> waitingOnCommit;
    private NavigableMap<TxnId, Command> waitingOnApply;

    private final Listeners listeners = new Listeners();

    public InMemoryCommand(CommandStore commandStore, TxnId txnId)
    {
        this.commandStore = commandStore;
        this.txnId = txnId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InMemoryCommand command = (InMemoryCommand) o;
        return commandStore == command.commandStore
                && txnId.equals(command.txnId)
                && Objects.equals(txn, command.txn)
                && promised.equals(command.promised)
                && accepted.equals(command.accepted)
                && Objects.equals(executeAt, command.executeAt)
                && deps.equals(command.deps)
                && Objects.equals(writes, command.writes)
                && Objects.equals(result, command.result)
                && status == command.status
                && Objects.equals(waitingOnCommit, command.waitingOnCommit)
                && Objects.equals(waitingOnApply, command.waitingOnApply)
                && Objects.equals(listeners, command.listeners);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(commandStore, txnId, txn, promised, accepted, executeAt, deps, writes, result, status, waitingOnCommit, waitingOnApply, listeners);
    }

    @Override
    public TxnId txnId()
    {
        return txnId;
    }

    @Override
    public CommandStore commandStore()
    {
        return commandStore;
    }

    @Override
    public Key homeKey()
    {
        return homeKey;
    }

    @Override
    protected void setHomeKey(Key key)
    {
        this.homeKey = key;
    }

    @Override
    public Key progressKey()
    {
        return progressKey;
    }

    @Override
    protected void setProgressKey(Key key)
    {
        this.progressKey = key;
    }

    @Override
    public Txn txn()
    {
        return txn;
    }

    @Override
    protected void setTxn(Txn txn)
    {
        this.txn = txn;
    }

    @Override
    public Ballot promised()
    {
        return promised;
    }

    @Override
    public void promised(Ballot ballot)
    {
        this.promised = ballot;
    }

    @Override
    public Ballot accepted()
    {
        return accepted;
    }

    @Override
    public void accepted(Ballot ballot)
    {
        this.accepted = ballot;
    }

    @Override
    public Timestamp executeAt()
    {
        return executeAt;
    }

    @Override
    public void executeAt(Timestamp timestamp)
    {
        this.executeAt = timestamp;
    }

    @Override
    public Dependencies savedDeps()
    {
        return deps;
    }

    @Override
    public void savedDeps(Dependencies deps)
    {
        this.deps = deps;
    }

    @Override
    public Writes writes()
    {
        return writes;
    }

    @Override
    public void writes(Writes writes)
    {
        this.writes = writes;
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public void result(Result result)
    {
        this.result = result;
    }

    @Override
    public Status status()
    {
        return status;
    }

    @Override
    public void status(Status status)
    {
        this.status = status;
    }

    @Override
    public boolean isGloballyPersistent()
    {
        return isGloballyPersistent;
    }

    @Override
    public void isGloballyPersistent(boolean v)
    {
        isGloballyPersistent = v;
    }

    @Override
    public Command addListener(Listener listener)
    {
        listeners.add(listener);
        return this;
    }

    @Override
    public void removeListener(Listener listener)
    {
        listeners.remove(listener);
    }

    @Override
    public void notifyListeners()
    {
        listeners.forEach(this);
    }

    @Override
    public void addWaitingOnCommit(Command command)
    {
        if (waitingOnCommit == null)
            waitingOnCommit = new TreeMap<>();

        waitingOnCommit.put(command.txnId(), command);
    }

    @Override
    public boolean isWaitingOnCommit()
    {
        return waitingOnCommit != null && !waitingOnCommit.isEmpty();
    }

    @Override
    public void removeWaitingOnCommit(Command command)
    {
        if (waitingOnCommit == null)
            return;
        waitingOnCommit.remove(command.txnId());
    }

    @Override
    public Command firstWaitingOnCommit()
    {
        return isWaitingOnCommit() ? waitingOnCommit.firstEntry().getValue() : null;
    }

    @Override
    public void addWaitingOnApplyIfAbsent(Command command)
    {
        if (waitingOnApply == null)
            waitingOnApply = new TreeMap<>();

        waitingOnApply.putIfAbsent(command.txnId(), command);
    }

    @Override
    public boolean isWaitingOnApply()
    {
        return waitingOnApply != null && !waitingOnApply.isEmpty();
    }

    @Override
    public void removeWaitingOnApply(Command command)
    {
        if (waitingOnApply == null)
            return;
        waitingOnApply.remove(command.txnId());
    }

    @Override
    public Command firstWaitingOnApply()
    {
        return isWaitingOnApply() ? waitingOnApply.firstEntry().getValue() : null;
    }
}
