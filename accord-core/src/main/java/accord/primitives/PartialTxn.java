package accord.primitives;

import javax.annotation.Nullable;

import accord.api.Query;
import accord.api.Read;
import accord.api.Update;

public interface PartialTxn extends Txn
{
    Ranges covering();
    // TODO: merge efficient merge when more than one input
    PartialTxn with(PartialTxn add);
    Txn reconstitute(FullRoute<?> route);
    PartialTxn reconstitutePartial(PartialRoute<?> route);

    default boolean covers(Unseekables<?, ?> unseekables)
    {
        if (query() == null)
        {
            // The home shard is expected to store the query contents
            // So if the query is null, and we are being asked if we
            // cover a range that includes a home shard, we should say no
            Route<?> asRoute = Route.tryCastToRoute(unseekables);
            if (asRoute != null && asRoute.contains(asRoute.homeKey()))
                return false;
        }
        return covering().containsAll(unseekables);
    }

    // TODO: override toString
    static PartialTxn merge(@Nullable PartialTxn a, @Nullable PartialTxn b)
    {
        return a == null ? b : b == null ? a : a.with(b);
    }

    class InMemory extends Txn.InMemory implements PartialTxn
    {
        public final Ranges covering;

        public InMemory(Ranges covering, Kind kind, Seekables<?, ?> keys, Read read, Query query, Update update)
        {
            super(kind, keys, read, query, update);
            this.covering = covering;
        }

        @Override
        public Ranges covering()
        {
            return covering;
        }

        // TODO: merge efficient merge when more than one input
        public PartialTxn with(PartialTxn add)
        {
            if (!add.kind().equals(kind()))
                throw new IllegalArgumentException();

            Ranges covering = this.covering.union(add.covering());
            Seekables<?, ?> keys = ((Seekables)this.keys()).union(add.keys());
            Read read = this.read().merge(add.read());
            Query query = this.query() == null ? add.query() : this.query();
            Update update = this.update() == null ? null : this.update().merge(add.update());
            if (keys == this.keys())
            {
                if (covering == this.covering && read == this.read() && query == this.query() && update == this.update())
                    return this;
            }
            else if (keys == add.keys())
            {
                if (covering == add.covering() && read == add.read() && query == add.query() && update == add.update())
                    return add;
            }
            return new PartialTxn.InMemory(covering, kind(), keys, read, query, update);
        }

        // TODO: override toString

        public boolean covers(Ranges ranges)
        {
            return covering.containsAll(ranges);
        }

        public Txn reconstitute(FullRoute<?> route)
        {
            if (!covers(route) || query() == null)
                throw new IllegalStateException("Incomplete PartialTxn: " + this + ", route: " + route);

            return new Txn.InMemory(kind(), keys(), read(), query(), update());
        }

        public PartialTxn reconstitutePartial(PartialRoute<?> route)
        {
            if (!covers(route))
                throw new IllegalStateException("Incomplete PartialTxn: " + this + ", route: " + route);

            if (covering.containsAll(route.covering()))
                return this;

            return new PartialTxn.InMemory(route.covering(), kind(), keys(), read(), query(), update());
        }
    }

}
