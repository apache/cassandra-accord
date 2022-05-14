package accord.primitives;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.RoutingKey;

public abstract class AbstractRoute extends RoutingKeys
{
    public final RoutingKey homeKey;

    public AbstractRoute(RoutingKey[] keys, RoutingKey homeKey)
    {
        super(keys);
        Preconditions.checkNotNull(homeKey);
        this.homeKey = homeKey;
    }

    public abstract AbstractRoute union(AbstractRoute that);

    public abstract boolean covers(KeyRanges ranges);

    public abstract PartialRoute slice(KeyRanges ranges);

    /**
     * Requires that the ranges are fully covered by this collection
     */
    public abstract PartialRoute sliceStrict(KeyRanges ranges);

    public static AbstractRoute merge(@Nullable AbstractRoute prefer, @Nullable AbstractRoute defer)
    {
        if (defer == null) return prefer;
        if (prefer == null) return defer;
        return prefer.union(defer);
    }
}
