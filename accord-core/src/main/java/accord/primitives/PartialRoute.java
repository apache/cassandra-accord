package accord.primitives;

import accord.api.RoutingKey;
import accord.primitives.Routable.Domain;

public interface PartialRoute<T extends Unseekable> extends Route<T>
{
    @Override
    boolean isEmpty();
    Ranges covering();

    /**
     * Expected to be compatible PartialRoute type, i.e. both split from the same FullRoute
     */
    PartialRoute<T> union(PartialRoute<T> route);

    @Override
    default Ranges sliceCovering(Ranges newRanges, Slice slice)
    {
        return covering().slice(newRanges, slice);
    }

    static PartialRoute<?> empty(Domain domain, RoutingKey homeKey)
    {
        return domain.isKey() ? PartialKeyRoute.empty(homeKey) : PartialRangeRoute.empty(homeKey);
    }
}
