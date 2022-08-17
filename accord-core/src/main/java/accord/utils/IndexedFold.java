package accord.utils;

public interface IndexedFold<K, V>
{
    V apply(int index, K key, V value);
}