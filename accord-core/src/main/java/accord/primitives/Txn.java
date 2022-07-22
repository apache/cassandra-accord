package accord.primitives;

import java.util.Objects;

import com.google.common.base.Preconditions;

import accord.api.*;
import accord.local.*;

public class Txn
{
    public enum Kind
    {
        READ, WRITE;

        public boolean isWrite()
        {
            return this == WRITE;
        }
    }

    public final Kind kind;
    public final Keys keys;
    public final Read read;
    public final Query query;
    public final Update update;

    public Txn(Keys keys, Read read, Query query)
    {
        Preconditions.checkArgument(getClass() != Txn.class || query != null);
        this.kind = Kind.READ;
        this.keys = keys;
        this.read = read;
        this.query = query;
        this.update = null;
    }

    public Txn(Keys keys, Read read, Query query, Update update)
    {
        Preconditions.checkArgument(getClass() != Txn.class || query != null);
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
        return kind.isWrite();
    }

    public Result result(Data data)
    {
        return query.compute(data, read, update);
    }

    public Writes execute(Timestamp executeAt, Data data)
    {
        if (update == null)
            return new Writes(executeAt, Keys.EMPTY, null);

        return new Writes(executeAt, update.keys(), update.apply(data));
    }

    public Keys keys()
    {
        return keys;
    }

    public PartialTxn slice(KeyRanges ranges, boolean includeQuery)
    {
        return new PartialTxn(ranges, kind, keys.slice(ranges), read.slice(ranges), includeQuery ? query : null,
                              update == null ? null : update.slice(ranges));
    }

    public String toString()
    {
        return "{read:" + read.toString() + (update != null ? ", update:" + update : "") + '}';
    }

    public Data read(Command command)
    {
        return keys.foldl(command.commandStore.ranges().at(command.executeAt().epoch), (index, key, accumulate) -> {
            CommandStore commandStore = command.commandStore;
            if (!commandStore.hashIntersects(key))
                return accumulate;

            Data result = read.read(key, command.executeAt(), commandStore.store());
            return accumulate != null ? accumulate.merge(result) : result;
        }, null);
    }
}
