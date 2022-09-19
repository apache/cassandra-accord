package accord.primitives;

public interface FullRoute<T extends Unseekable> extends Route<T>, Unseekables<T, Route<T>>
{
    @Override default FullRoute<T> union(Route<T> route) { return this; }
}
