package accord.local;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import accord.txn.Timestamp;
import com.google.common.collect.Iterators;

public class CommandsForKey implements Listener, Iterable<Command>
{
    // TODO: efficiency
    public final NavigableMap<Timestamp, Command> uncommitted = new TreeMap<>();
    public final NavigableMap<Timestamp, Command> committedById = new TreeMap<>();
    public final NavigableMap<Timestamp, Command> committedByExecuteAt = new TreeMap<>();

    private Timestamp max = Timestamp.NONE;

    public Timestamp max()
    {
        return max;
    }

    @Override
    public void onChange(Command command)
    {
        max = Timestamp.max(max, command.executeAt());
        switch (command.status())
        {
            case Applied:
            case Executed:
            case Committed:
                uncommitted.remove(command.txnId());
                committedById.put(command.txnId(), command);
                committedByExecuteAt.put(command.executeAt(), command);
                command.removeListener(this);
                break;
        }
    }

    public void register(Command command)
    {
        max = Timestamp.max(max, command.executeAt());
        uncommitted.put(command.txnId(), command);
        command.addListener(this);
    }

    public void forWitnessed(Timestamp minTs, Timestamp maxTs, Consumer<Command> consumer)
    {
        uncommitted.subMap(minTs, true, maxTs, true).values().stream()
                .filter(cmd -> cmd.hasBeen(Status.PreAccepted)).forEach(consumer);
        committedById.subMap(minTs, true, maxTs, true).values().forEach(consumer);
        committedByExecuteAt.subMap(minTs, true, maxTs, true).values().stream()
                // TODO (review): should this be cmd.txnId().compareTo(minTs) < 0 || cmd.txnId().compareTo(maxTs) > 0?
                .filter(cmd -> !cmd.txnId().equals(cmd.executeAt())).forEach(consumer);
    }

    @Override
    public Iterator<Command> iterator()
    {
        return Iterators.concat(uncommitted.values().iterator(), committedByExecuteAt.values().iterator());
    }
}
