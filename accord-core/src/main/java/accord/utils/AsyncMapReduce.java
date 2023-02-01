package accord.utils;

import org.apache.cassandra.utils.concurrent.Future;

import java.util.function.Function;

public interface AsyncMapReduce<I, O> extends Function<I, Future<O>>, Reduce<O>
{
    // TODO (desired, safety): ensure mutual exclusivity when calling each of these methods
    Future<O> apply(I in);
    O reduce(O o1, O o2);
}
