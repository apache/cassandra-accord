package accord.primitives;

import accord.api.RoutingKey;
import accord.utils.Invariants;

import javax.annotation.Nonnull;

public abstract class RangeRoute extends AbstractRanges<Route<Range>> implements Route<Range>
{
    public final RoutingKey homeKey;

    RangeRoute(@Nonnull RoutingKey homeKey, Range[] ranges)
    {
        super(ranges);
        this.homeKey = Invariants.nonNull(homeKey);
    }

    @Override
    public PartialRangeRoute slice(Ranges ranges)
    {
        return slice(ranges, this, homeKey, PartialRangeRoute::new);
    }

    @Override
    public RoutingKey homeKey()
    {
        return homeKey;
    }

    @Override
    public boolean equals(Object that)
    {
        return super.equals(that) && homeKey.equals(((RangeRoute)that).homeKey);
    }
}
