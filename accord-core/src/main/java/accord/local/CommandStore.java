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

package accord.local;

import accord.api.*;
import accord.api.ProgressLog;
import accord.api.DataStore;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.utils.async.AsyncChain;

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public interface CommandStore extends AgentExecutor
{
    interface Factory
    {
        CommandStore create(int id,
                            NodeTimeService time,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            RangesForEpochHolder rangesForEpoch);
    }

    @VisibleForTesting
    class Unsafe
    {
        private static final ThreadLocal<CommandStore> CURRENT_STORE = new ThreadLocal<>();

        @Nullable
        public static CommandStore maybeCurrent()
        {
            return CURRENT_STORE.get();
        }

        private static void register(CommandStore store)
        {
            CURRENT_STORE.set(store);
        }

        private static void remove()
        {
            CURRENT_STORE.remove();
        }

        public static void runWith(CommandStore store, Runnable fn)
        {
            CommandStore prev = maybeCurrent();
            register(store);
            try
            {
                fn.run();
            }
            finally
            {
                if (prev == null) remove();
                else register(prev);
            }
        }

        public static <T> T runWith(CommandStore store, Callable<T> fn) throws Exception
        {
            CommandStore prev = maybeCurrent();
            register(store);
            try
            {
                return fn.call();
            }
            finally
            {
                if (prev == null) remove();
                else register(prev);
            }
        }
    }

    static CommandStore current()
    {
        CommandStore cs = Unsafe.CURRENT_STORE.get();
        if (cs == null)
            throw new IllegalStateException("Attempted to access current CommandStore, but not running in a CommandStore");
        return cs;
    }

    static void register(CommandStore store)
    {
        if (!store.inStore())
            throw new IllegalStateException("Unable to register a CommandStore when not running in it; store " + store);
        Unsafe.CURRENT_STORE.set(store);
    }

    static void checkInStore()
    {
        CommandStore store = Unsafe.maybeCurrent();
        if (store == null) throw new IllegalStateException("Expected to be running in a CommandStore but is not");
    }

    static void checkNotInStore()
    {
        CommandStore store = Unsafe.maybeCurrent();
        if (store != null)
            throw new IllegalStateException("Expected to not be running in a CommandStore, but running in " + store);
    }

    int id();
    boolean inStore();
    AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer);
    <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply);

    void shutdown();
    default void register()
    {
        execute(() -> CommandStore.register(this));
    }
}
