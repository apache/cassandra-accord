package accord.utils;

public interface IndexedFoldToLong<K>
{
    long apply(int index, K key, long param, long prev);
}
