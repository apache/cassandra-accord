package accord.primitives;

import java.util.function.IntFunction;

import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.utils.SortedArrays;

/**
 * A slice of a Route that covers
 */
public class PartialRoute extends AbstractRoute
{
    public final KeyRanges covering;

    public PartialRoute(KeyRanges covering, RoutingKey homeKey, RoutingKey[] keys)
    {
        super(keys, homeKey);
        this.covering = covering;
    }

    public PartialRoute sliceStrict(KeyRanges newRange)
    {
        if (!covering.contains(newRange))
            throw new IllegalArgumentException("Not covered");

        RoutingKey[] keys = slice(newRange, RoutingKey[]::new);
        return new PartialRoute(newRange, homeKey, keys);
    }

    @Override
    public AbstractRoute union(AbstractRoute that)
    {
        if (that instanceof Route) return that;
        return union((PartialRoute) that);
    }

    @Override
    public boolean covers(KeyRanges ranges)
    {
        return covering.contains(ranges);
    }

    public PartialRoute slice(KeyRanges newRange)
    {
        if (newRange.contains(covering))
            return this;

        RoutingKey[] keys = slice(covering, RoutingKey[]::new);
        return new PartialRoute(covering, homeKey, keys);
    }

    public PartialRoute union(PartialRoute that)
    {
        Preconditions.checkState(homeKey.equals(that.homeKey));
        RoutingKey[] keys = SortedArrays.linearUnion(this.keys, that.keys, factory());
        KeyRanges covering = this.covering.union(that.covering);
        if (covering == this.covering && keys == this.keys)
            return this;
        if (covering == that.covering && keys == that.keys)
            return that;
        return new PartialRoute(covering, homeKey, keys);
    }

    private static IntFunction<RoutingKey[]> factory()
    {
        return RoutingKey[]::new;
    }
}
