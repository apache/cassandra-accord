package accord.api;

import accord.topology.KeyRanges;
import accord.txn.Timestamp;

/**
 * A collection of data to write to one or more stores
 *
 * TODO: support splitting so as to minimise duplication of data across shards
 */
public interface Write
{
    void apply(KeyRanges range, Timestamp executeAt, Store store);
}
