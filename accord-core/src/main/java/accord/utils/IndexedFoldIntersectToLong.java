package accord.utils;

public interface IndexedFoldIntersectToLong<K>
{
    long apply(int leftIndex, int rightIndex, K key, long param, long prev);
}
