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

import java.util.function.Function;

import accord.utils.Invariants;

public class NestedAsyncResult<V> extends AsyncResults.AbstractResult<V>
{
    private volatile AsyncResult<?> waitingOn;

    public <I> NestedAsyncResult(AsyncResult<I> whenDone, Function<I, AsyncResult<V>> apply)
    {
        this.waitingOn = whenDone;
        whenDone.invoke((success, fail) -> {
            if (fail != null)
            {
                tryFailure(fail);
                waitingOn = null;
            }
            else
            {
                try
                {
                    AsyncResult<V> next = apply.apply(success);
                    waitingOn = next;
                    next.invoke(this::callback);
                }
                catch (Throwable t)
                {
                    callback(null, t);
                }
            }
        });
    }

    private synchronized void callback(V success, Throwable fail)
    {
        if (fail != null) tryFailure(fail);
        else trySuccess(success);
        waitingOn = null;
    }

    public static <I, O> AsyncResult<O> flatMap(AsyncResult<I> whenDone, Function<I, AsyncResult<O>> report)
    {
        return new NestedAsyncResult<>(whenDone, report);
    }

    @Override
    public String toString()
    {
        boolean transitive = false;
        AsyncResult<?> waitingOn = this.waitingOn;
        while (waitingOn instanceof NestedAsyncResult<?>)
        {
            AsyncResult<?> next = ((NestedAsyncResult<?>) waitingOn).waitingOn;
            if (next == null)
                break;
            waitingOn = next;
            transitive = true;
        }

        if (waitingOn == null)
        {
            Invariants.expect(isDone());
            return "Done";
        }

        return (transitive ? "Transitively " : "") + "Waiting On: " + waitingOn;
    }
}
