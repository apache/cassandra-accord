package accord.api;

/**
 * A routing key for determining which shards are involved in a transaction
 */
public interface Key extends RoutingKey
{
    RoutingKey toRoutingKey();
}
