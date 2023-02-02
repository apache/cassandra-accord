package accord.utils.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

public class AsyncCallbacks
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncCallbacks.class);

    private static final BiConsumer<Object, Throwable> NOOP = (unused, failure) -> {
        if (failure != null)
            logger.error("Exception received by noop callback", failure);
    };

    public static <T> BiConsumer<? super T, Throwable> noop()
    {
        return NOOP;
    }

    public static <T> BiConsumer<? super T, Throwable> inExecutor(BiConsumer<? super T, Throwable> callback, Executor executor)
    {
        return (result, throwable) -> {
            try
            {
                executor.execute(() -> callback.accept(result, throwable));
            }
            catch (Throwable t)
            {
                callback.accept(null, t);
            }
        };
    }


    public static <T> BiConsumer<? super T, Throwable> inExecutor(Runnable runnable, Executor executor)
    {
        return (result, throwable) -> {
            if (throwable == null) executor.execute(runnable);
            else throw new RuntimeException(throwable);
        };
    }
}
