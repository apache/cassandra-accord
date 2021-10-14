package accord.utils;

public interface EpochFunction<V>
{
    static <V> EpochFunction<V> noop()
    {
        return (e, v) -> {};
    }

    void apply(long epoch, V value);
}
