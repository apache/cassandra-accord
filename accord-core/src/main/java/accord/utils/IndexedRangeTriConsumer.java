package accord.utils;

public interface IndexedRangeTriConsumer<P1, P2, P3>
{
    void accept(P1 p1, P2 p2, P3 p3, int fromIndex, int toIndex);
}
