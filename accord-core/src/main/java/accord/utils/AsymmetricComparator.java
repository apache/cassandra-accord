package accord.utils;

// similar to Cassandra's AsymmetricOrdering, only we can create a static lambda function of this
public interface AsymmetricComparator<T1, T2>
{
    int compare(T1 t1, T2 t2);
}
