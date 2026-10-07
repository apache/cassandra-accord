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

package accord.utils;

import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.UncaughtExceptionListener;

public class ThrowableSafety
{
    private static final Logger logger = LoggerFactory.getLogger(ThrowableSafety.class);

    public static Throwable merge(@Nullable Throwable input, @Nullable Throwable with)
    {
        if (input == null)
            return with;
        if (with == null)
            return input;
        return mergeNonNull(input, with);
    }

    public static Throwable mergeNonNull(@Nonnull Throwable input, @Nonnull Throwable with)
    {
        if (input != with)
            input.addSuppressed(with);
        return input;
    }

    public static void addSuppressed(@Nonnull Throwable to, @Nonnull Throwable add)
    {
        if (to != add)
            to.addSuppressed(add);
    }

    public static <V> void safeSuccess(BiConsumer<V, Throwable> invoke, V success)
    {
        try { invoke.accept(success, null); }
        catch (Throwable t)
        {
            safeReport(t);
        }
    }

    public static <V> void safeSuccess(BiConsumer<V, Throwable> invoke, V success, UncaughtExceptionListener.Supplier uncaught)
    {
        try { invoke.accept(success, null); }
        catch (Throwable t)
        {
            safeReport(t, uncaught);
        }
    }

    public static <V> void safeFail(BiConsumer<V, Throwable> invoke, Throwable fail)
    {
        try { invoke.accept(null, fail); }
        catch (Throwable t)
        {
            safeReport(mergeNonNull(t, fail));
        }
    }

    public static <V> void safeFail(BiConsumer<V, Throwable> invoke, Throwable fail, UncaughtExceptionListener.Supplier uncaught)
    {
        try { invoke.accept(null, fail); }
        catch (Throwable t)
        {
            safeReport(mergeNonNull(t, fail), uncaught);
        }
    }

    public static <V> void safeInvoke(BiConsumer<V, Throwable> invoke, V success, Throwable fail)
    {
        try { invoke.accept(success, fail); }
        catch (Throwable t)
        {
            if (fail != null)
                addSuppressed(t, fail);

            safeReport(t);
        }
    }

    public static <V> void safeInvoke(BiConsumer<V, Throwable> invoke, V success, Throwable fail, UncaughtExceptionListener.Supplier uncaught)
    {
        try { invoke.accept(success, fail); }
        catch (Throwable t)
        {
            if (fail != null)
                addSuppressed(t, fail);

            safeReport(t, uncaught);
        }
    }

    public static void safeReport(Throwable report)
    {
        try
        {
            Thread thread = Thread.currentThread();
            thread.getUncaughtExceptionHandler().uncaughtException(thread, report);
        }
        catch (Throwable t)
        {
            addSuppressed(t, report);
            try { logger.error("Unexpected exception thrown by UncaughtExceptionHandler", t); }
            catch (Throwable ignore) { /*give up*/ }
        }
    }

    public static void safeReport(Throwable report, UncaughtExceptionListener.Supplier uncaught)
    {
        try
        {
            uncaught.uncaughtExceptions().onException(report);
        }
        catch (Throwable t)
        {
            addSuppressed(t, report);
            try { logger.error("Unexpected exception thrown by UncaughtExceptionListener", t); }
            catch (Throwable ignore) { /*give up*/ }
        }
    }

}
