package accord.api;

import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.Timestamp;

/**
 * A read to be performed on potentially multiple shards, the inputs of which may be fed to a {@link Query}
 *
 * TODO: support splitting the read into per-shard portions
 */
public interface Read
{
    Keys keys();
    Data read(Key key, Timestamp executeAt, DataStore store);
    Read slice(KeyRanges ranges);
    Read merge(Read other);
}
