package accord.primitives;

import accord.api.RoutingKey;
import accord.utils.Invariants;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_OVERLAPPING;

/**
 * A slice of a Route that covers
 */
public class PartialRangeRoute extends RangeRoute implements PartialRoute<Range>
{
    public static class SerializationSupport
    {
        public static PartialRangeRoute create(Ranges covering, RoutingKey homeKey, Range[] ranges)
        {
            return new PartialRangeRoute(covering, homeKey, ranges);
        }
    }

    public final Ranges covering;

    public PartialRangeRoute(Ranges covering, RoutingKey homeKey, Range[] ranges)
    {
        super(homeKey, ranges);
        this.covering = covering;
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.PartialRangeRoute;
    }

    @Override
    public Ranges covering()
    {
        return covering;
    }

    @Override
    public boolean covers(Ranges ranges)
    {
        return covering.containsAll(ranges);
    }

    @Override
    public boolean intersects(AbstractRanges<?> ranges)
    {
        return ranges.intersects(covering);
    }

    public PartialRangeRoute sliceStrict(Ranges newRange)
    {
        if (!covering.containsAll(newRange))
            throw new IllegalArgumentException("Not covered");

        return slice(newRange, this, homeKey, PartialRangeRoute::new);
    }

    @Override
    public Unseekables<Range, ?> toMaximalUnseekables()
    {
        throw new UnsupportedOperationException();
    }

    public PartialRangeRoute slice(Ranges newRanges)
    {
        if (newRanges.containsAll(covering))
            return this;

        return slice(newRanges, this, homeKey, PartialRangeRoute::new);
    }

    public Unseekables<Range, ?> with(RoutingKey withKey)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Route<Range> union(Route<Range> that)
    {
        if (Route.isFullRoute(that)) return that;
        return union((PartialRangeRoute) that);
    }

    public PartialRangeRoute union(PartialRoute<Range> with)
    {
        if (!(with instanceof PartialRangeRoute))
            throw new IllegalArgumentException();

        PartialRangeRoute that = (PartialRangeRoute) with;
        Invariants.checkState(homeKey.equals(that.homeKey));
        Ranges covering = this.covering.union(that.covering);
        if (covering == this.covering) return this;
        else if (covering == that.covering) return that;

        return union(MERGE_OVERLAPPING, this, that, covering, homeKey, PartialRangeRoute::new);
    }

    @Override
    public boolean equals(Object that)
    {
        return super.equals(that) && covering.equals(((PartialRangeRoute)that).covering);
    }
}
