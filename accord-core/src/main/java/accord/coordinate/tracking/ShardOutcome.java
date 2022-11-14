package accord.coordinate.tracking;

/**
 * Represents the logical result of a ShardFunction applied to a ShardTracker,
 * encapsulating also any modification it should make to the AbstractTracker
 * containing the shard.
 */
public interface ShardOutcome<T extends AbstractTracker<?, ?>>
{
    AbstractTracker.ShardOutcomes apply(T tracker, int shardIndex);
}
