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

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import static accord.utils.Invariants.checkArgument;

public class AsyncCallbacks
{
    public static <T> BiConsumer<? super T, Throwable> inExecutorService(BiConsumer<? super T, Throwable> callback, ExecutorService es)
    {
        // Checking for shutdown once here for the other `inExecutorService` as well as `AsyncChain.addCallback`
        // So we don't repeat the check
        checkArgument(!es.isShutdown(), "ExecutorService is shutdown");
        return (result, throwable) -> {
            try
            {
                es.execute(() -> callback.accept(result, throwable));
            }
            catch (Throwable t)
            {
                callback.accept(null, t);
            }
        };
    }


    public static <T> BiConsumer<? super T, Throwable> inExecutorService(Runnable runnable, ExecutorService es)
    {
        return inExecutorService(toCallback(runnable), es);
    }

    public static <T> BiConsumer<T, Throwable> toCallback(Runnable runnable) {
        return (unused, failure) -> {
            if (failure == null) runnable.run();
        };
    }
}
