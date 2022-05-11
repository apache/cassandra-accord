package accord.api;

/**
 * A routing key for determining which shards are involved in a transaction
 */
public interface Key<K extends Key<K>> extends RoutingKey<K>
{
    Key toRoutingKey();
}
