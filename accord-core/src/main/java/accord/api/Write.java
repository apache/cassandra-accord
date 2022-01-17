package accord.api;

import accord.txn.Timestamp;

/**
 * A collection of data to write to one or more stores
 *
 * TODO: support splitting so as to minimise duplication of data across shards
 */
public interface Write
{
    void apply(Key key, Timestamp executeAt, DataStore store);
}
