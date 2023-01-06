package accord.utils;

import java.util.function.Function;

public interface MapReduce<I, O> extends Function<I, O>
{
    // TODO (desired, safety): ensure mutual exclusivity when calling each of these methods
    @Override
    O apply(I in);
    O reduce(O o1, O o2);
}
