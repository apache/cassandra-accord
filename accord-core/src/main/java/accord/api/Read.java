package accord.api;

/**
 * A read to be performed on potentially multiple shards, the inputs of which may be fed to a {@link Query}
 *
 * TODO: support splitting the read into per-shard portions
 */
public interface Read
{
    Data read(KeyRange range, Store store);
}
