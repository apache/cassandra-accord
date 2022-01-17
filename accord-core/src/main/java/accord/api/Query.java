package accord.api;

/**
 * The computational/transformation part of a client query
 */
public interface Query
{
    /**
     * Perform some transformation on the complete {@link Data} result of a {@link Read}
     * from some {@link DataStore}, to produce a {@link Result} to return to the client.
     */
    Result compute(Data data);
}
