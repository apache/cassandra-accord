package accord.primitives;

import accord.api.RoutingKey;

import javax.annotation.Nullable;

public interface Route<K extends Unseekable> extends Unseekables<K, Route<K>>
{
    RoutingKey homeKey();

    default boolean isRoute() { return true; }

    boolean covers(Ranges ranges);

    /**
     * Return an object containing any {@code K} present in either of the original collections,
     * and covering the union of the ranges.
     *
     * Differs from {@link Unseekables#with} in that the parameter must be a {@link Route}
     * and the result will be a {@link Route}.
     */
    Route<K> union(Route<K> route);

    @Override
    PartialRoute<K> slice(Ranges ranges);
    PartialRoute<K> sliceStrict(Ranges ranges);
    Ranges sliceCovering(Ranges ranges, Slice slice);

    /**
     * @return a PureRoutables that includes every shard we know of, not just those we contact
     * (i.e., includes the homeKey if not already included)
     */
    Unseekables<K, ?> toMaximalUnseekables();

    // this method exists solely to circumvent JDK bug with testing and casting interfaces
    static boolean isFullRoute(@Nullable Unseekables<?, ?> unseekables) { return unseekables != null && unseekables.kind().isFullRoute(); }

    // this method exists solely to circumvent JDK bug with testing and casting interfaces
    static boolean isRoute(@Nullable Unseekables<?, ?> unseekables) { return unseekables != null && unseekables.kind().isRoute(); }

    // this method exists solely to circumvent JDK bug with testing and casting interfaces
    static FullRoute<?> castToFullRoute(@Nullable Unseekables<?, ?> unseekables)
    {
        if (unseekables == null)
            return null;

        switch (unseekables.domain())
        {
            default: throw new AssertionError();
            case Key: return (FullKeyRoute) unseekables;
            case Range: return (FullRangeRoute) unseekables;
        }
    }

    static Route<?> castToRoute(@Nullable Unseekables<?, ?> unseekables)
    {
        if (unseekables == null)
            return null;

        switch (unseekables.domain())
        {
            default: throw new AssertionError();
            case Key: return (KeyRoute) unseekables;
            case Range: return (RangeRoute) unseekables;
        }
    }

    // this method exists solely to circumvent JDK bug with testing and casting interfaces
    static Route<?> tryCastToRoute(@Nullable Unseekables<?, ?> unseekables)
    {
        if (unseekables == null)
            return null;

        switch (unseekables.kind())
        {
            default: throw new AssertionError();
            case RoutingKeys:
            case RoutingRanges:
                return null;
            case PartialKeyRoute:
                return (PartialKeyRoute) unseekables;
            case PartialRangeRoute:
                return (PartialRangeRoute) unseekables;
            case FullKeyRoute:
                return (FullKeyRoute) unseekables;
            case FullRangeRoute:
                return (FullRangeRoute) unseekables;
        }
    }

    // this method exists solely to circumvent JDK bug with testing and casting interfaces
    static PartialRoute<?> castToPartialRoute(@Nullable Unseekables<?, ?> unseekables)
    {
        if (unseekables == null)
            return null;

        switch (unseekables.domain())
        {
            default: throw new AssertionError();
            case Key: return (PartialKeyRoute) unseekables;
            case Range: return (PartialRangeRoute) unseekables;
        }
    }

    static <T extends Unseekable> Route<T> merge(@Nullable Route<T> prefer, @Nullable Route<T> defer)
    {
        if (defer == null) return prefer;
        if (prefer == null) return defer;
        return prefer.union(defer);
    }
}
