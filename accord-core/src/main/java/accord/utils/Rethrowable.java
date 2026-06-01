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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CancellationException;

public interface Rethrowable<T extends Throwable & Rethrowable<T>>
{
    /**
     * Create a new exception of the same type, keeping the original exception as a cause of the new exception,
     * so that we can track the current stack trace as well as the origin stack trace.
     */
    T rethrowable();

    static Throwable rethrowable(Throwable t)
    {
        if (t instanceof Rethrowable<?>)
        {
            Throwable wrapped = ((Rethrowable<?>)t).rethrowable();
            if (wrapped.getClass() != t.getClass())
            {
                IllegalStateException ise = new IllegalStateException("Wrapping should not change type");
                ise.addSuppressed(t);
                throw ise;
            }
            return wrapped;
        }
        else if (t instanceof CancellationException)
        {
            CancellationException wrapped = new CancellationException(t.getMessage());
            wrapped.addSuppressed(t);
            return wrapped;
        }
        else if (t instanceof AssertionError)
        {
            return new AssertionError(t);
        }
        else if (t instanceof OutOfMemoryError)
        {
            return t;
        }
        else if (t instanceof Error)
        {
            return new Error(t);
        }
        else
        {
            // Don't try to wrap in the same type if we have to wrap with RuntimeException anyways
            if (!(t instanceof RuntimeException))
                throw new RuntimeException(t);

            // Support adhoc wrapping using reflection since we can't access exception types external to Accord
            Class<? extends Throwable> clazz = t.getClass();
            try
            {
                Constructor<?> constructor = null;
                for (Constructor<?> candidate : clazz.getConstructors())
                {
                    Class<?>[] parameters = candidate.getParameterTypes();
                    if (parameters.length != 1)
                        continue;
                    if (parameters[0].isAssignableFrom(clazz))
                    {
                        constructor = candidate;
                        break;
                    }
                }
                if (constructor != null)
                    return (Throwable)constructor.newInstance(t);
            }
            // OK to ignore these as we can always throw a runtime exception
            catch (InstantiationException | IllegalAccessException | InvocationTargetException ignore) {}
            // Failed to wrap in the same type so use RuntimeException
            return new RuntimeException(t);
        }
    }
}
