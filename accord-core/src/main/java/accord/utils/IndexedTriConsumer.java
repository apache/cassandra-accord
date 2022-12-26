package accord.utils;

// TODO (now): migrate to utils, but must standardise on parameter order with index last
public interface IndexedTriConsumer<P1, P2, P3>
{
    void accept(P1 p1, P2 p2, P3 p3, int index);
}
