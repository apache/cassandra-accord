package accord.primitives;

import accord.api.RoutingKey;

public class FullRangeRoute extends RangeRoute implements FullRoute<Range>
{
    public static class SerializationSupport
    {
        public static FullRangeRoute create(RoutingKey homeKey, Range[] ranges)
        {
            return new FullRangeRoute(homeKey, ranges);
        }
    }

    public FullRangeRoute(RoutingKey homeKey, Range[] ranges)
    {
        super(homeKey, ranges);
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.FullRangeRoute;
    }

    @Override
    public boolean covers(Ranges ranges)
    {
        return true;
    }

    @Override
    public PartialRangeRoute sliceStrict(Ranges ranges)
    {
        return slice(ranges);
    }

    @Override
    public FullRangeRoute toMaximalUnseekables()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return "{homeKey:" + homeKey + ',' + super.toString() + '}';
    }

}
