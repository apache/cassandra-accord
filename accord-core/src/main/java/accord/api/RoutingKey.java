package accord.api;

import accord.primitives.KeyRange;

import javax.annotation.Nonnull;

public interface RoutingKey extends Comparable<RoutingKey>
{
    /**
     * A special RoutingKey that sorts before or after everything, so that exclusive bounds may still cover
     * the full range of possible RoutingKey.
     *
     * All RoutingKey implementations must sort correctly with this type.
     *
     * TODO: need to partition range from/to -/+ infinity as otherwise we exclude at least one key
     */
    class InfiniteRoutingKey implements RoutingKey
    {
        public static final InfiniteRoutingKey POSITIVE_INFINITY = new InfiniteRoutingKey(1);
        public static final InfiniteRoutingKey NEGATIVE_INFINITY = new InfiniteRoutingKey(-1);

        final int compareTo;

        public InfiniteRoutingKey(int compareTo)
        {
            this.compareTo = compareTo;
        }

        @Override
        public int routingHash()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(@Nonnull RoutingKey ignore)
        {
            return compareTo;
        }
    }

    /**
     * Implementations must be comparable with {@link InfiniteRoutingKey}
     * @param that the object to be compared.
     * @return
     */
    int compareTo(@Nonnull RoutingKey that);

    /**
     * Returns a hash code of a key to support accord internal sharding. Hash values for equal keys must be equal.
     */
    int routingHash();

    default RoutingKey toRoutingKey() { return this; }
}
