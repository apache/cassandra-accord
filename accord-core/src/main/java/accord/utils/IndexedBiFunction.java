package accord.utils;

public interface IndexedBiFunction<T, U, R>
{
    R apply(int i, T t, U u);
}
