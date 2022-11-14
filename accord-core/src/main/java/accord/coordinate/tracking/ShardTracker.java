package accord.coordinate.tracking;

import accord.topology.Shard;

public abstract class ShardTracker
{
    public final Shard shard;

    public ShardTracker(Shard shard)
    {
        this.shard = shard;
    }
}
