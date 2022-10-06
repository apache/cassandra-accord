package accord.utils;

import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiFunction;

public class ReducingFuture<V> extends AsyncPromise<V>
{
    private static final AtomicIntegerFieldUpdater<ReducingFuture> PENDING_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ReducingFuture.class, "pending");
    private final List<? extends Future<V>> futures;
    private final BiFunction<V, V, V> reducer;
    private volatile int pending;

    protected ReducingFuture(List<? extends Future<V>> futures, BiFunction<V, V, V> reducer)
    {
        this.futures = futures;
        this.reducer = reducer;
        this.pending = futures.size();
        if (futures.size() == 0)
            trySuccess(null);
        futures.forEach(f -> f.addListener(this::operationComplete));
    }

    private <F extends io.netty.util.concurrent.Future<?>> void operationComplete(F future) throws Exception
    {
        if (isDone())
            return;

        if (!future.isSuccess())
        {
            tryFailure(future.cause());
        }
        else if (PENDING_UPDATER.decrementAndGet(this) == 0)
        {
            V result = futures.get(0).getNow();
            for (int i=1, mi=futures.size(); i<mi; i++)
                result = reducer.apply(result, futures.get(i).getNow());

            trySuccess(result);
        }
    }

    public static <T> Future<T> reduce(List<? extends Future<T>> futures, BiFunction<T, T, T> reducer)
    {
        if (futures.isEmpty())
            return ImmediateFuture.success(null);

        if (futures.size() == 1)
            return futures.get(0);

        return new ReducingFuture<>(futures, reducer);
    }
}
