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

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import accord.api.Agent;
import accord.burn.random.FrequentLargeRange;
import accord.burn.random.RandomLong;
import accord.burn.random.RandomWalkRange;
import accord.utils.RandomSource;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class SimulatedDelayedExecutorService extends TaskExecutorService implements ScheduledExecutorService
{
    private class ScheduledTask<T> extends Task<T> implements ScheduledFuture<T>
    {
        private final long sequenceNumber;
        private final long periodMillis;
        private long nextExecuteAtMillis;
        private boolean canceled = false;

        private ScheduledTask(long sequenceNumber, long initialDelay, long value, TimeUnit unit, Callable<T> fn)
        {
            super(fn);
            this.sequenceNumber = sequenceNumber;
            periodMillis = unit.toMillis(value);
            nextExecuteAtMillis = triggerTime(initialDelay, unit);
        }

        private long triggerTime(long delay, TimeUnit unit)
        {
            long delayMillis = unit.toMillis(delay < 0 ? 0 : delay);
            return pending.nowInMillis() + delayMillis;
        }

        @Override
        public long getDelay(TimeUnit unit)
        {
            return unit.convert(nextExecuteAtMillis - pending.nowInMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other)
        {
            if (other == this) // compare zero if same object
                return 0;
            if (other instanceof ScheduledTask) {
                ScheduledTask<?> x = (ScheduledTask<?>)other;
                long diff = nextExecuteAtMillis - x.nextExecuteAtMillis;
                if (diff < 0)
                    return -1;
                else if (diff > 0)
                    return 1;
                else if (sequenceNumber < x.sequenceNumber)
                    return -1;
                else
                    return 1;
            }
            long diff = getDelay(NANOSECONDS) - other.getDelay(NANOSECONDS);
            return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
        }

        @Override
        public void run()
        {
            if (canceled)
                return;
            boolean periodic = periodMillis != 0;
            if (!periodic)
            {
                super.run();
            }
            else
            {
                // run without setting the result
                try
                {
                    fn.call();
                    long nowMillis = pending.nowInMillis();
                    if (periodMillis > 0)
                    {
                        // scheduleAtFixedRate
                        nextExecuteAtMillis += periodMillis;
                    }
                    else
                    {
                        // scheduleWithFixedDelay
                        nextExecuteAtMillis = nowMillis + (-periodMillis);
                    }
                    long delayMillis = nextExecuteAtMillis - nowMillis;
                    if (delayMillis < 0)
                        delayMillis = 0;
                    schedule(this, delayMillis, TimeUnit.MILLISECONDS);
                }
                catch (Throwable t)
                {
                    setFailure(t);
                }
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            if (canceled)
                return false;
            canceled = true;
            return pending.remove(this);
        }
    }

    private final PendingQueue pending;
    private final Agent agent;
    private final RandomSource random;
    private final RandomLong jitterInNano;
    private long sequenceNumber;

    public SimulatedDelayedExecutorService(PendingQueue pending, Agent agent, RandomSource random)
    {
        this.pending = pending;
        this.agent = agent;
        this.random = random;
        // this is different from Apache Cassandra Simulator as this is computed differently for each executor
        // rather than being a global config
        double ratio = random.nextInt(1, 11) / 100.0D;
        this.jitterInNano = new FrequentLargeRange(new RandomWalkRange(random, microToNanos(0), microToNanos(50)),
                                                   new RandomWalkRange(random, microToNanos(50), msToNanos(5)),
                                                   ratio);
    }

    private static int msToNanos(int value)
    {
        return Math.toIntExact(TimeUnit.MILLISECONDS.toNanos(value));
    }

    private static int microToNanos(int value)
    {
        return Math.toIntExact(TimeUnit.MICROSECONDS.toNanos(value));
    }

    @Override
    public void execute(Task<?> task)
    {
        pending.add(task, jitterInNano.getLong(random), TimeUnit.NANOSECONDS);
    }

    private void schedule(Task<?> task, long delay, TimeUnit unit)
    {
        pending.add(task, unit.toNanos(delay) + jitterInNano.getLong(random), TimeUnit.NANOSECONDS);
    }

    @Override
    public Agent agent()
    {
        return agent;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
    {
        ScheduledTask<?> task = new ScheduledTask<>(sequenceNumber++, delay, 0, NANOSECONDS, Executors.callable(command));
        schedule(task, delay, unit);
        return task;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
    {
        ScheduledTask<V> task = new ScheduledTask<>(sequenceNumber++, delay, 0, NANOSECONDS, callable);
        schedule(task, delay, unit);
        return task;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
    {
        ScheduledTask<?> task = new ScheduledTask<>(sequenceNumber++, initialDelay, period, unit, Executors.callable(command));
        schedule(task, initialDelay, unit);
        return task;
    }


    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
    {
        ScheduledTask<?> task = new ScheduledTask<>(sequenceNumber++, initialDelay, -delay, unit, Executors.callable(command));
        schedule(task, initialDelay, unit);
        return task;
    }
}