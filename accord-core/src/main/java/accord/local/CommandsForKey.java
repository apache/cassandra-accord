package accord.local;

import java.util.NavigableMap;
import java.util.TreeMap;

import accord.txn.Timestamp;
import accord.txn.TxnId;

public class CommandsForKey implements Listener
{
    // TODO: efficiency
    public final NavigableMap<Timestamp, Command> uncommitted = new TreeMap<>();
    public final NavigableMap<TxnId, Command> committedById = new TreeMap<>();
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
}
