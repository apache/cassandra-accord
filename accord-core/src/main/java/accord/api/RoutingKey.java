package accord.api;

public interface RoutingKey<K extends Key<K>> extends Comparable<K>
{
    /**
     * Returns a hash code of a key to support accord internal sharding. Hash values for equal keys must be equal.
     */
    int routingHash();
}
