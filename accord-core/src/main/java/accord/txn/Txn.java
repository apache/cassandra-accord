package accord.txn;

import java.util.Objects;

import accord.api.*;
import accord.local.*;

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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Txn txn = (Txn) o;
        return kind == txn.kind && keys.equals(txn.keys) && read.equals(txn.read) && query.equals(txn.query) && Objects.equals(update, txn.update);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, keys, read, query, update);
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
        return "{read:" + read.toString() + (update != null ? ", update:" + update : "") + '}';
    }

    public Data read(Command command, Keys keys)
    {
        return keys.foldl(command.commandStore.ranges().at(command.executeAt().epoch), (key, accumulate) -> {
            CommandStore commandStore = command.commandStore;
            if (!commandStore.hashIntersects(key))
                return accumulate;

            Data result = read.read(key, command.executeAt(), commandStore.store());
            return accumulate != null ? accumulate.merge(result) : result;
        }, null);
    }
}
