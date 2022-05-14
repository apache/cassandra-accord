package accord.api;

public interface RoutingKey extends Comparable<RoutingKey>
{
    /**
     * Returns a hash code of a key to support accord internal sharding. Hash values for equal keys must be equal.
     */
    int routingHash();

    default RoutingKey toRoutingKey() { return this; }
}
