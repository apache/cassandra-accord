package accord.api;

import java.util.function.BiConsumer;

public interface UncaughtExceptionListener extends BiConsumer<Object, Throwable>
{
    void onUncaughtException(Throwable t);
    @Override
    default void accept(Object ignore, Throwable t)
    {
        if (t != null)
            onUncaughtException(t);
    }
}
