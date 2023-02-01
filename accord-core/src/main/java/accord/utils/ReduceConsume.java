package accord.utils;

import java.util.function.BiConsumer;

public interface ReduceConsume<O> extends Reduce<O>, BiConsumer<O, Throwable>
{
    void accept(O result, Throwable failure);
}
