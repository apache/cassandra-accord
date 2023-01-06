package accord.primitives;

import accord.api.RoutingKey;
import accord.utils.Invariants;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_OVERLAPPING;
import static accord.primitives.Routables.Slice.Overlapping;

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

    public static PartialRangeRoute empty(RoutingKey homeKey)
    {
        return new PartialRangeRoute(Ranges.EMPTY, homeKey, NO_RANGES);
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
    public Unseekables<Range, ?> toMaximalUnseekables()
    {
        return with(homeKey);
    }

    @Override
    public PartialRangeRoute sliceStrict(Ranges newRanges)
    {
        if (!covering.containsAll(newRanges))
            throw new IllegalArgumentException("Not covered");

        return slice(newRanges);
    }

    @Override
    public Route<Range> union(Route<Range> that)
    {
        if (Route.isFullRoute(that)) return that;
        return union((PartialRangeRoute) that);
    }

    @Override
    public PartialRangeRoute union(PartialRoute<Range> with)
    {
        if (!(with instanceof PartialRangeRoute))
            throw new IllegalArgumentException();

        PartialRangeRoute that = (PartialRangeRoute) with;
        Invariants.checkState(homeKey.equals(that.homeKey));
        Ranges covering = this.covering.with(that.covering);
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
