package accord.primitives;

import accord.api.RoutingKey;
import com.google.common.base.Preconditions;

public class Route extends AbstractRoute
{
    public static class SerializationSupport
    {
        public static Route create(RoutingKey homeKey, RoutingKey[] keys)
        {
            return new Route(homeKey, keys);
        }
    }

    public Route(RoutingKey homeKey, RoutingKey[] keys)
    {
        super(keys, homeKey);
    }

    @Override
    public boolean covers(KeyRanges ranges)
    {
        Preconditions.checkNotNull(ranges);
        return true;
    }

    @Override
    public AbstractRoute union(AbstractRoute that)
    {
        Preconditions.checkNotNull(that);
        return this;
    }

    @Override
    public PartialRoute slice(KeyRanges ranges)
    {
        return new PartialRoute(ranges, homeKey, slice(ranges, RoutingKey[]::new));
    }

    @Override
    public PartialRoute sliceStrict(KeyRanges ranges)
    {
        return slice(ranges);
    }

    @Override
    public String toString()
    {
        return "{homeKey:" + homeKey + ',' + super.toString() + '}';
    }
}
