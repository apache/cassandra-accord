package accord.api;

import accord.txn.Keys;
import accord.txn.Timestamp;

/**
 * A read to be performed on potentially multiple shards, the inputs of which may be fed to a {@link Query}
 *
 * TODO: support splitting the read into per-shard portions
 */
public interface Read
{
    Keys keys();
    Data read(Key key, Timestamp executeAt, DataStore store);
}
