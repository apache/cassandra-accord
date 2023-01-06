package accord.primitives;

import accord.api.RoutingKey;
import accord.utils.Invariants;

import javax.annotation.Nonnull;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_OVERLAPPING;
import static accord.primitives.Routables.Slice.Overlapping;

public abstract class RangeRoute extends AbstractRanges<Route<Range>> implements Route<Range>
{
    public final RoutingKey homeKey;

    RangeRoute(@Nonnull RoutingKey homeKey, Range[] ranges)
    {
        super(ranges);
        this.homeKey = Invariants.nonNull(homeKey);
    }

    @Override
    public Unseekables<Range, ?> with(Unseekables<Range, ?> with)
    {
        if (isEmpty())
            return with;

        return union(MERGE_OVERLAPPING, this, (AbstractRanges<?>) with, null, null,
                (left, right, rs) -> Ranges.ofSortedAndDeoverlapped(rs));
    }

    public Unseekables<Range, ?> with(RoutingKey withKey)
    {
        if (contains(withKey))
            return this;

        return with(Ranges.of(withKey.asRange()));
    }

    @Override
    public PartialRangeRoute slice(Ranges ranges)
    {
        return slice(ranges, Overlapping, this, homeKey, PartialRangeRoute::new);
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
