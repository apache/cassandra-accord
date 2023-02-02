package accord.utils.async;

import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

public class AsyncCallbacks
{
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
