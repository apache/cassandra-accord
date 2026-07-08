/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.utils.async;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class AsyncCallbacks
{
    // a runnable interface that may be directly failed
    public interface RunOrFail extends Runnable
    {
        /**
         * Does not throw exceptions ordinarily; failure should be reported to any callback it carries
         * and only a boolean indicating success/failure is returned to the caller.
         * Exceptions are thrown to the caller only if there was a problem reporting the outcome to any callback
         */
        boolean runMayThrow();

        /**
         * Notify any callback that the task will not be run due to the provided exception
         */
        void fail(Throwable fail);

        @Override
        default void run()
        {
            runMayThrow();
        }
    }

    public static class RunAndCallback implements RunOrFail
    {
        final Runnable run;
        final BiConsumer<? super Void, Throwable> callback;

        public RunAndCallback(Runnable run, BiConsumer<? super Void, Throwable> callback)
        {
            this.run = run;
            this.callback = callback;
        }

        @Override
        public boolean runMayThrow()
        {
            return runAndCallback(run, callback);
        }

        @Override
        public void fail(Throwable fail)
        {
            callback.accept(null, fail);
        }

        @Override
        public String toString()
        {
            return "[Run " + run.toString() + "; callback " + callback + ']';
        }
    }

    public static class CallAndCallback<V> implements RunOrFail
    {
        final Callable<? extends V> call;
        final BiConsumer<? super V, Throwable> callback;

        public CallAndCallback(Callable<? extends V> call, BiConsumer<? super V, Throwable> callback)
        {
            this.call = call;
            this.callback = callback;
        }

        @Override
        public boolean runMayThrow()
        {
            return callAndCallback(call, callback);
        }

        @Override
        public void fail(Throwable fail)
        {
            callback.accept(null, fail);
        }

        @Override
        public String toString()
        {
            return "[Call " + call.toString() + "; callback " + callback + ']';
        }
    }

    public static class FlatCallAndCallback<V> implements RunOrFail
    {
        final Callable<? extends AsyncChain<V>> call;
        final BiConsumer<? super V, Throwable> callback;

        public FlatCallAndCallback(Callable<? extends AsyncChain<V>> call, BiConsumer<? super V, Throwable> callback)
        {
            this.call = call;
            this.callback = callback;
        }

        @Override
        public boolean runMayThrow()
        {
            return flatCallAndCallback(call, callback);
        }

        @Override
        public void fail(Throwable fail)
        {
            callback.accept(null, fail);
        }

        @Override
        public String toString()
        {
            return "[FlatCall " + call.toString() + "; callback " + callback + ']';
        }
    }

    public static <T> BiConsumer<? super T, Throwable> inExecutor(BiConsumer<? super T, Throwable> callback, Executor executor)
    {
        return (success, fail) -> {
            try
            {
                executor.execute(() -> callback.accept(success, fail));
            }
            catch (Throwable t)
            {
                callback.accept(null, t);
            }
        };
    }

    public static <T> BiConsumer<T, Throwable> always(Runnable run)
    {
        return (success, fail) -> run.run();
    }

    public static <T> BiConsumer<T, Throwable> ifSuccess(Runnable run)
    {
        return (success, fail) -> {
            if (fail == null)
                run.run();
        };
    }

    public static <T> BiConsumer<T, Throwable> ifSuccess(Consumer<T> consumer)
    {
        return (success, fail) -> {
            if (fail == null)
                consumer.accept(success);
        };
    }

    public static boolean runAndCallback(Runnable run, BiConsumer<? super Void, Throwable> receiver)
    {
        try
        {
            run.run();
        }
        catch (Throwable t)
        {
            receiver.accept(null, t);
            return false;
        }
        receiver.accept(null, null);
        return true;
    }

    public static <V> boolean callAndCallback(Callable<V> call, BiConsumer<? super V, Throwable> receiver)
    {
        V v;
        try
        {
            v = call.call();
        }
        catch (Throwable t)
        {
            receiver.accept(null, t);
            return false;
        }
        receiver.accept(v, null);
        return true;
    }

    public static <V> boolean flatCallAndCallback(Callable<? extends AsyncChain<V>> call, BiConsumer<? super V, Throwable> receiver)
    {
        AsyncChain<V> v;
        try
        {
            v = call.call();
        }
        catch (Throwable t)
        {
            receiver.accept(null, t);
            return false;
        }
        v.begin(receiver);
        return true;
    }

    public static <V> BiConsumer<AsyncChain<V>, Throwable> flatCallback(BiConsumer<? super V, Throwable> callback)
    {
        return (success, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else success.begin(callback);
        };
    }

    public static Cancellable execute(Executor executor, RunOrFail runOrFail)
    {
        try
        {
            if (executor instanceof ExecutorService)
            {
                Future<?> future = ((ExecutorService) executor).submit(runOrFail);
                return () -> future.cancel(false);
            }
            else
            {
                executor.execute(runOrFail);
            }
        }
        catch (Throwable t)
        {
            runOrFail.fail(t);
        }
        return null;
    }
}
