package accord.local;

import java.util.NavigableMap;
import java.util.TreeMap;

import accord.api.Key;
import accord.api.Store;
import accord.topology.Shard;
import accord.txn.TxnId;

/**
 * node-local accord metadata per shard
 */
public class Instance
{
    public final Shard shard;
    private final Node node;
    private final Store store;
    private final NavigableMap<TxnId, Command> commands = new TreeMap<>();
    private final NavigableMap<Key, CommandsForKey> commandsForKey = new TreeMap<>();

    public Instance(Shard shard, Node node, Store store)
    {
        this.shard = shard;
        this.node = node;
        this.store = store;
    }

    public Command command(TxnId txnId)
    {
        return commands.computeIfAbsent(txnId, id -> new Command(this, id));
    }

    public boolean hasCommand(TxnId txnId)
    {
        return commands.containsKey(txnId);
    }

    public CommandsForKey commandsForKey(Key key)
    {
        return commandsForKey.computeIfAbsent(key, ignore -> new CommandsForKey());
    }

    public boolean hasCommandsForKey(Key key)
    {
        return commandsForKey.containsKey(key);
    }

    public Store store()
    {
        return store;
    }

    public Node node()
    {
        return node;
    }
}
