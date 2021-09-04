package accord.api;

/**
 * A client-defined update operation (the write equivalent of a query).
 * Takes as input the data returned by {@code Read}, and returns a {@code Write}
 * representing new information to distributed to each shard's stores.
 */
public interface Update
{
    Write apply(Data data);
}
