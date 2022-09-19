package accord.primitives;

import accord.api.RoutingKey;
import accord.utils.Invariants;

public class FullKeyRoute extends KeyRoute implements FullRoute<RoutingKey>
{
    public static class SerializationSupport
    {
        public static FullKeyRoute create(RoutingKey homeKey, RoutingKey[] keys)
        {
            return new FullKeyRoute(homeKey, keys);
        }
    }

    public FullKeyRoute(RoutingKey homeKey, RoutingKey[] keys)
    {
        super(homeKey, keys);
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.FullKeyRoute;
    }

    @Override
    public boolean covers(Ranges ranges)
    {
        return true;
    }

    @Override
    public boolean intersects(AbstractRanges<?> ranges)
    {
        // TODO (now): remove this in favour of parent implementation - ambiguous at present
        return true;
    }

    @Override
    public FullKeyRoute with(RoutingKey withKey)
    {
        Invariants.checkArgument(contains(withKey));
        // TODO (now): remove this in favour of parent implementation - ambiguous at present
        return this;
    }

    @Override
    public PartialKeyRoute slice(Ranges ranges)
    {
        return new PartialKeyRoute(ranges, homeKey, slice(ranges, RoutingKey[]::new));
    }

    @Override
    public PartialKeyRoute sliceStrict(Ranges ranges)
    {
        return slice(ranges);
    }

    public FullKeyRoute toMaximalUnseekables()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return "{homeKey:" + homeKey + ',' + super.toString() + '}';
    }

}
