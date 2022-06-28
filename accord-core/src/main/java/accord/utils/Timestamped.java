package accord.utils;

import accord.primitives.Timestamp;

public class Timestamped<T>
{
    public final Timestamp timestamp;
    public final T data;

    public Timestamped(Timestamp timestamp, T data)
    {
        this.timestamp = timestamp;
        this.data = data;
    }

    public static <T> Timestamped<T> merge(Timestamped<T> a, Timestamped<T> b)
    {
        return a.timestamp.compareTo(b.timestamp) >= 0 ? a : b;
    }

    @Override
    public String toString()
    {
        return data.toString();
    }
}
