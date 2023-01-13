package accord.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface MapReduceConsume<I, O> extends MapReduce<I, O>, ReduceConsume<O>
{
    @Override
    void accept(O result, Throwable failure);

    static <I> MapReduceConsume<I, Void> forEach(Consumer<I> forEach, BiConsumer<Object, Throwable> consume)
    {
        return new MapReduceConsume<I, Void>() {
            @Override public void accept(Void result, Throwable failure) { consume.accept(result, failure); }
            @Override public Void apply(I in) { forEach.accept(in); return null; }
            @Override public Void reduce(Void o1, Void o2) {return null; }
        };
    }
}
