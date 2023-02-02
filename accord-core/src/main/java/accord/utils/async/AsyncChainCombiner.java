package accord.utils.async;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

abstract class AsyncChainCombiner<I, O> extends AsyncChains.Head<O>
{
    private static final AtomicIntegerFieldUpdater<AsyncChainCombiner> REMAINING = AtomicIntegerFieldUpdater.newUpdater(AsyncChainCombiner.class, "remaining");
    private volatile Object state;
    private volatile BiConsumer<? super O, Throwable> callback;
    private volatile int remaining;

    protected AsyncChainCombiner(List<AsyncChain<I>> inputs)
    {
        Preconditions.checkArgument(!inputs.isEmpty());
        this.state = inputs;
    }

    private List<AsyncChain<? extends I>> inputs()
    {
        Object current = state;
        Preconditions.checkState(current instanceof List);
        return (List<AsyncChain<? extends I>>) current;
    }

    private I[] results()
    {
        Object current = state;
        Preconditions.checkState(current instanceof Object[]);
        return (I[]) current;
    }

    void add(AsyncChain<I> chain)
    {
        inputs().add(chain);
    }

    void addAll(List<AsyncChain<I>> chains)
    {
        inputs().addAll(chains);
    }

    int size()
    {
        Object current = state;
        if (current instanceof List)
            return ((List) current).size();
        if (current instanceof Object[])
            return ((Object[]) current).length;
        throw new IllegalStateException();
    }

    abstract void complete(I[] results, BiConsumer<? super O, Throwable> callback);

    private void callback(int idx, I result, Throwable throwable)
    {
        int current = remaining;
        if (current == 0)
            return;

        if (throwable != null && REMAINING.compareAndSet(this, current, 0))
        {
            callback.accept(null, throwable);
            return;
        }

        results()[idx] = result;
        if (REMAINING.decrementAndGet(this) == 0)
        {
            try
            {
                complete(results(), callback);
            }
            catch (Throwable t)
            {
                callback.accept(null, t);
            }
        }
    }

    private BiConsumer<I, Throwable> callbackFor(int idx)
    {
        return (result, failure) -> callback(idx, result, failure);
    }

    @Override
    public void begin(BiConsumer<? super O, Throwable> callback)
    {
        List<? extends AsyncChain<? extends I>> chains = inputs();
        state = new Object[chains.size()];

        int size = chains.size();
        this.callback = callback;
        this.remaining = size;
        for (int i=0; i<size; i++)
            chains.get(i).begin(callbackFor(i));
    }

    static class All<V> extends AsyncChainCombiner<V, List<V>>
    {
        All(List<AsyncChain<V>> asyncChains)
        {
            super(asyncChains);
        }

        @Override
        void complete(V[] results, BiConsumer<? super List<V>, Throwable> callback)
        {
            List<V> result = Lists.newArrayList(results);
            callback.accept(result, null);
        }
    }

    static class Reduce<V> extends AsyncChainCombiner<V, V>
    {
        private final BiFunction<V, V, V> reducer;
        Reduce(List<AsyncChain<V>> asyncChains, BiFunction<V, V, V> reducer)
        {
            super(asyncChains);
            this.reducer = reducer;
        }

        @Override
        void complete(V[] results, BiConsumer<? super V, Throwable> callback)
        {
            V result = results[0];
            for (int i=1; i< results.length; i++)
                result = reducer.apply(result, results[i]);
            callback.accept(result, null);
        }

        /*
         * Determines if the given chain is a reduce instance with the same reducer, and can
         * therefore be added to, instead of creating another reduce instance
         */
        static <V> boolean canAppendTo(AsyncChain<? extends V> chain, BiFunction<V, V, V> reducer)
        {
            if (!(chain instanceof AsyncChainCombiner.Reduce))
                return false;

            AsyncChainCombiner.Reduce<? extends V> reduce = (AsyncChainCombiner.Reduce<? extends V>) chain;
            return reduce.reducer == reducer;
        }
    }
}
