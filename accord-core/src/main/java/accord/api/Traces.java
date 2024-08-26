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

package accord.api;

import java.lang.reflect.Field;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.TxnId;

import static accord.api.Propagatable.NO_OP_PROPAGATABLE;

public class Traces
{
    public static final String ACCORD_TRACE_CLASS_PROPERTY_NAME = "accord.api.trace_local_class";
    private static final Logger logger = LoggerFactory.getLogger(Traces.class);

    public static final TraceLocal instance;

    public interface TraceLocal
    {
        Tracer getTracer();
        // Propagatable for whatever the current thread local is set to
        @Nonnull Propagatable propagatable();
        // Propagate this specific tracer
        @Nullable Closeable propagate(Tracer tracer);
    }

    static
    {
        String customTracingClass = System.getProperty(ACCORD_TRACE_CLASS_PROPERTY_NAME);
        if (null != customTracingClass)
        {
            TraceLocal instanceToUse;
            try
            {
                Class<?> clazz = Class.forName(customTracingClass);
                Field instanceField = clazz.getField("instance");
                instanceToUse = (TraceLocal)instanceField.get(null);
            }
            catch (Exception e)
            {
                logger.error(String.format("Cannot use class %s for trace local, ignoring", customTracingClass), e);
                instanceToUse = new NoOpTraceLocal();
            }
            instance = instanceToUse;
        }
        else
        {
            instance = new NoOpTraceLocal();
        }
    }

    private static class NoOpTraceLocal implements TraceLocal
    {
        @Override
        public @Nullable Tracer getTracer()
        {
            return null;
        }

        @Override
        public @Nonnull Propagatable propagatable()
        {
            return NO_OP_PROPAGATABLE;
        }

        @Override
        public @Nullable Closeable propagate(@Nullable Tracer tracer)
        {
            return null;
        }
    }

    // The integration needs this to propagate the tracer
    @SuppressWarnings("unused")
    public static Tracer tracer()
    {
        return instance.getTracer();
    }

    public static void trace(String message)
    {
        Tracer tracer = instance.getTracer();
        if (tracer != null)
            tracer.trace(message);
    }

    public static void trace(String format, Object arg)
    {
        Tracer tracer = instance.getTracer();
        if (tracer != null)
            tracer.trace(format, arg);
    }

    public static void trace(String format, Object arg1, Object arg2)
    {
        Tracer tracer = instance.getTracer();
        if (tracer != null)
            tracer.trace(format, arg1, arg2);
    }

    public static void trace(String format, Object... args)
    {
        Tracer tracer = instance.getTracer();
        if (tracer != null)
            tracer.trace(format, args);
    }

    public static void trace(TxnId txnId, String message)
    {
        Tracer tracer = instance.getTracer();
        if (tracer != null)
            tracer.trace(txnId + ": " + message);
    }

    public static void trace(TxnId txnId, String format, Object arg)
    {
        Tracer tracer = instance.getTracer();
        if (tracer != null)
            tracer.trace(txnId + ": " + format, arg);
    }

    public static void trace(TxnId txnId, String format, Object arg1, Object arg2)
    {
        Tracer tracer = instance.getTracer();
        if (tracer != null)
            tracer.trace(txnId + ": " + format, arg1, arg2);
    }

    public static void trace(TxnId txnId, String format, Object arg1, Object arg2, Object arg3)
    {
        Tracer tracer = instance.getTracer();
        if (tracer != null)
            tracer.trace(txnId + ": " + format, arg1, arg2, arg3);
    }

    public static void trace(TxnId txnId, String format, Object... args)
    {
        Tracer tracer = instance.getTracer();
        if (tracer != null)
            tracer.trace(txnId + ": " + format, args);
    }

    /**
     * Propagatable for whatever the current thread local is set to
     */
    public static @Nonnull Propagatable propagate()
    {
        return instance.propagatable();
    }
}
