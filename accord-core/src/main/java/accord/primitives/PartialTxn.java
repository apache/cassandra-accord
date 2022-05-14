package accord.primitives;

import javax.annotation.Nullable;

import accord.api.Query;
import accord.api.Read;
import accord.api.Update;

public class PartialTxn extends Txn
{
    public final KeyRanges covering;
    public final Kind kind; // TODO: we do not need to take a write-edge dependency on every key

    public PartialTxn(KeyRanges covering, Kind kind, Keys keys, Read read, Query query, Update update)
    {
        super(keys, read, query, update);
        this.covering = covering;
        this.kind = kind;
    }

    public boolean covers(KeyRanges ranges)
    {
        return covering.contains(ranges);
    }

    public boolean covers(AbstractKeys<?, ?> keys)
    {
        // TODO: this distinction seems brittle
        if (keys instanceof AbstractRoute)
            return covers((AbstractRoute)keys);

        return covering.containsAll(keys);
    }

    public boolean covers(AbstractRoute route)
    {
        if (query == null && route.contains(route.homeKey))
            return false;
        return covering.containsAll(route);
    }

    // TODO: merge efficient merge when more than one input
    public PartialTxn with(PartialTxn add)
    {
        if (!add.kind.equals(kind))
            throw new IllegalArgumentException();

        KeyRanges covering = this.covering.union(add.covering);
        Keys keys = this.keys.union(add.keys);
        Read read = this.read.merge(add.read);
        Query query = this.query == null ? add.query : this.query;
        Update update = this.update == null ? null : this.update.merge(add.update);
        if (keys == this.keys)
        {
            if (covering == this.covering && read == this.read && query == this.query && update == this.update)
                return this;
        }
        else if (keys == add.keys)
        {
            if (covering == add.covering && read == add.read && query == add.query && update == add.update)
                return add;
        }
        return new PartialTxn(covering, kind, keys, read, query, update);
    }

    // TODO: override toString

    public Txn reconstitute(Route route)
    {
        if (!covers(route) || query == null)
            throw new IllegalStateException("Incomplete PartialTxn: " + this + ", route: " + route);

        return new Txn(keys, read, query, update);
    }

    public PartialTxn reconstitutePartial(PartialRoute route)
    {
        if (!covers(route))
            throw new IllegalStateException("Incomplete PartialTxn: " + this + ", route: " + route);

        if (covering.contains(route.covering))
            return this;

        return new PartialTxn(route.covering, kind, keys, read, query, update);
    }

    public static PartialTxn merge(@Nullable PartialTxn a, @Nullable PartialTxn b)
    {
        return a == null ? b : b == null ? a : a.with(b);
    }
}
