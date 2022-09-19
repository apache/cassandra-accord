package accord.primitives;

import accord.api.RoutingKey;

import javax.annotation.Nonnull;

public interface RoutableKey extends Routable, Comparable<RoutableKey>
{
    /**
     * A special RoutingKey that sorts before or after everything, so that exclusive bounds may still cover
     * the full range of possible RoutingKey.
     *
     * All RoutingKey implementations must sort correctly with this type.
     *
     * TODO: need to partition range from/to -/+ infinity as otherwise we exclude at least one key
     */
    class InfiniteRoutableKey implements RoutableKey
    {
        public static final InfiniteRoutableKey POSITIVE_INFINITY = new InfiniteRoutableKey(1);
        public static final InfiniteRoutableKey NEGATIVE_INFINITY = new InfiniteRoutableKey(-1);

        final int compareTo;

        public InfiniteRoutableKey(int compareTo)
        {
            this.compareTo = compareTo;
        }

        @Override
        public int compareTo(@Nonnull RoutableKey ignore)
        {
            return compareTo;
        }

        @Override
        public int routingHash() { throw new UnsupportedOperationException(); }

        @Override
        public RoutingKey toUnseekable() { throw new UnsupportedOperationException(); }

        @Override
        public RoutingKey someIntersectingRoutingKey() { throw new UnsupportedOperationException(); }
    }

    /**
     * Implementations must be comparable with {@link InfiniteRoutableKey}
     * @param that the object to be compared.
     * @return
     */
    int compareTo(@Nonnull RoutableKey that);

    /**
     * Returns a hash code of a key to support accord internal sharding. Hash values for equal keys must be equal.
     *
     * TODO (now): can we remove this if we remove hashIntersects et al?
     */
    int routingHash();

    default Kind kind() { return Kind.Key; }

    RoutingKey toUnseekable();

    @Override default RoutingKey someIntersectingRoutingKey() { return toUnseekable(); }
}
