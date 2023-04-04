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

package accord.impl.basic;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import accord.utils.async.AsyncResults;

public abstract class TaskExecutorService extends AbstractExecutorService
{
    public static class Task<T> extends AsyncResults.SettableResult<T> implements Pending, RunnableFuture<T>
    {
        private final Callable<T> fn;

        public Task(Callable<T> fn)
        {
            this.fn = fn;
        }

        @Override
        public void run()
        {
            try
            {
                setSuccess(fn.call());
            }
            catch (Throwable t)
            {
                setFailure(t);
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    protected <T> Task<T> newTaskFor(Runnable runnable, T value)
    {
        return newTaskFor(Executors.callable(runnable, value));
    }

    @Override
    protected <T> Task<T> newTaskFor(Callable<T> callable)
    {
        return new Task<>(callable);
    }

    private Task<?> newTaskFor(Runnable command)
    {
        return command instanceof Task ? (Task<?>) command : newTaskFor(command, null);
    }

    protected abstract void execute(Task<?> task);

    @Override
    public final void execute(Runnable command)
    {
        execute(newTaskFor(command));
    }

    @Override
    public Task<?> submit(Runnable task)
    {
        return (Task<?>) super.submit(task);
    }

    @Override
    public <T> Task<T> submit(Runnable task, T result)
    {
        return (Task<T>) super.submit(task, result);
    }

    @Override
    public <T> Task<T> submit(Callable<T> task)
    {
        return (Task<T>) super.submit(task);
    }

    @Override
    public void shutdown()
    {

    }

    @Override
    public List<Runnable> shutdownNow()
    {
        return null;
    }

    @Override
    public boolean isShutdown()
    {
        return false;
    }

    @Override
    public boolean isTerminated()
    {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        return false;
    }
}