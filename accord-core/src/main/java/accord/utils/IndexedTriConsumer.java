package accord.utils;

public interface IndexedTriConsumer<P1, P2, P3>
{
    /**
     * Consume some object parameters associated with an index in a collection.
     *
     * The first parameter is typically used to convey some container the index refer to,
     * with the others providing other configuration.
     */
    void accept(P1 p1, P2 p2, P3 p3, int index);
}
