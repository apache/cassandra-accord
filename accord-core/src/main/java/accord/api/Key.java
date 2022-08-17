package accord.api;

/**
 * A routing key for determining which shards are involved in a transaction
 */
public interface Key extends Comparable<Key>
{
    /**
     * Returns a hash code of a key to support accord internal sharding. Hash values for equal keys must be equal.
     */
    int routingHash();
}
