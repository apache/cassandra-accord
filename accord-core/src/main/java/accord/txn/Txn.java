package accord.txn;

import java.util.Comparator;
import java.util.stream.Stream;

import accord.api.*;
import accord.local.*;
import accord.topology.KeyRanges;

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

    public Data read(KeyRanges range, Store store)
    {
        return read.read(range, store);
    }

    public Data read(Command command)
    {
        CommandStore commandStore = command.commandStore;
        return read(commandStore.ranges(), commandStore.store());
    }

    public Timestamp maxConflict(CommandStore commandStore)
    {
        return maxConflict(commandStore, keys());
    }

    public Stream<Command> conflictsMayExecuteBefore(CommandStore commandStore, Timestamp mayExecuteBefore)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.commandsForKey(key);
            return Stream.concat(
            forKey.uncommitted.headMap(mayExecuteBefore, false).values().stream(),
            // TODO: only return latest of Committed?
            forKey.committedByExecuteAt.headMap(mayExecuteBefore, false).values().stream()
            );
        });
    }

    public Stream<Command> uncommittedStartedBefore(CommandStore commandStore, TxnId startedBefore)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.commandsForKey(key);
            return forKey.uncommitted.headMap(startedBefore, false).values().stream();
        });
    }

    public Stream<Command> committedStartedBefore(CommandStore commandStore, TxnId startedBefore)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.commandsForKey(key);
            return forKey.committedById.headMap(startedBefore, false).values().stream();
        });
    }

    public Stream<Command> uncommittedStartedAfter(CommandStore commandStore, TxnId startedAfter)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.commandsForKey(key);
            return forKey.uncommitted.tailMap(startedAfter, false).values().stream();
        });
    }

    public Stream<Command> committedExecutesAfter(CommandStore commandStore, TxnId startedAfter)
    {
        return keys().stream().flatMap(key -> {
            CommandsForKey forKey = commandStore.commandsForKey(key);
            return forKey.committedByExecuteAt.tailMap(startedAfter, false).values().stream();
        });
    }

    public void register(CommandStore commandStore, Command command)
    {
        assert commandStore == command.commandStore;
        keys().forEach(key -> commandStore.commandsForKey(key).register(command));
    }

    protected Timestamp maxConflict(CommandStore commandStore, Keys keys)
    {
        return keys.stream()
                   .map(commandStore::commandsForKey)
                   .map(CommandsForKey::max)
                   .max(Comparator.naturalOrder())
                   .orElse(Timestamp.NONE);
    }

}
