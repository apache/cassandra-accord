package accord.txn;

import java.util.Comparator;
import java.util.stream.Stream;

import accord.api.Data;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Update;
import accord.api.Key;
import accord.api.Store;
import accord.local.Command;
import accord.local.CommandsForKey;
import accord.local.Instance;
import accord.local.Node;

public class Txn
{
    enum Kind { READ, WRITE, RECONFIGURE }

    final Kind kind;
    public final Keys keys;
    public final Read read;
    public final Query query;
    public final Update update;

    public Txn(Keys keys, Read read, Query query)
    {
        this.kind = Kind.READ;
        this.keys = keys;
        this.read = read;
        this.query = query;
        this.update = null;
    }

    public Txn(Keys keys, Read read, Query query, Update update)
    {
        this.kind = Kind.WRITE;
        this.keys = keys;
        this.read = read;
        this.update = update;
        this.query = query;
    }

    public boolean isWrite()
    {
        switch (kind)
        {
            default:
                throw new IllegalStateException();
            case READ:
                return false;
            case WRITE:
            case RECONFIGURE:
                return true;
        }
    }

    public Result result(Data data)
    {
        return query.compute(data);
    }

    public Writes execute(Timestamp executeAt, Data data)
    {
        if (update == null)
            return new Writes(executeAt, keys, null);

        return new Writes(executeAt, keys, update.apply(data));
    }

    public Keys keys()
    {
        return keys;
    }

    public String toString()
    {
        return "read:" + read.toString() + (update != null ? ", update:" + update : "");
    }

    public Data read(Key start, Key end, Store store)
    {
        return read.read(start, end, store);
    }

    public Data read(Command command)
    {
        Instance instance = command.instance;
        return read(instance.shard.start, instance.shard.end, instance.store());
    }

    // TODO: move these somewhere else?
    public Stream<Instance> local(Node node)
    {
        return node.local(keys());
    }

    public Timestamp maxConflict(Instance instance)
    {
        return maxConflict(instance, keys());
    }

    public Stream<Command> conflictsMayExecuteBefore(Instance instance, Timestamp mayExecuteBefore)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = instance.commandsForKey(key);
            return Stream.concat(
            forKey.uncommitted.headMap(mayExecuteBefore, false).values().stream(),
            // TODO: only return latest of Committed?
            forKey.committedByExecuteAt.headMap(mayExecuteBefore, false).values().stream()
            );
        });
    }

    public Stream<Command> uncommittedStartedBefore(Instance instance, TxnId startedBefore)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = instance.commandsForKey(key);
            return forKey.uncommitted.headMap(startedBefore, false).values().stream();
        });
    }

    public Stream<Command> committedStartedBefore(Instance instance, TxnId startedBefore)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = instance.commandsForKey(key);
            return forKey.committedById.headMap(startedBefore, false).values().stream();
        });
    }

    public Stream<Command> uncommittedStartedAfter(Instance instance, TxnId startedAfter)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = instance.commandsForKey(key);
            return forKey.uncommitted.tailMap(startedAfter, false).values().stream();
        });
    }

    public Stream<Command> committedExecutesAfter(Instance instance, TxnId startedAfter)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = instance.commandsForKey(key);
            return forKey.committedByExecuteAt.tailMap(startedAfter, false).values().stream();
        });
    }

    public void register(Instance instance, Command command)
    {
        assert instance == command.instance;
        keys().forEach(key -> instance.commandsForKey(key).register(command));
    }

    protected Timestamp maxConflict(Instance instance, Keys keys)
    {
        return keys.stream()
                   .map(instance::commandsForKey)
                   .map(CommandsForKey::max)
                   .max(Comparator.naturalOrder())
                   .orElse(Timestamp.NONE);
    }

}
