package accord.primitives;

public interface PartialRoute<T extends Unseekable> extends Route<T>
{
    boolean isEmpty();
    Ranges covering();

    /**
     * Expected to be compatible PartialRoute type, i.e. both split from the same FullRoute
     */
    PartialRoute<T> union(PartialRoute<T> route);
}
