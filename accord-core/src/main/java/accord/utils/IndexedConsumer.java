package accord.utils;

public interface IndexedConsumer<V>
{
    void accept(int i, V v);
}
