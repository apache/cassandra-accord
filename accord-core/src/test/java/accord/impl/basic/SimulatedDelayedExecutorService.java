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
import java.util.concurrent.atomic.AtomicLong;

import accord.api.Agent;
import accord.local.AgentExecutor;

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
                    callable.call();
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
                    trySetResult(null, t);
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
    private final AtomicLong sequenceNumber;

    public SimulatedDelayedExecutorService(PendingQueue pending, Agent agent)
    {
        this.pending = pending;
        this.agent = agent;
        sequenceNumber = new AtomicLong();
    }

    private SimulatedDelayedExecutorService(PendingQueue pending, Agent agent, AtomicLong sequenceNumber)
    {
        this.pending = pending;
        this.agent = agent;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public void execute(Task<?> task)
    {
        pending.add(task);
    }

    private void schedule(Task<?> task, long delay, TimeUnit unit)
    {
        pending.add(task, delay, unit);
    }

    @Override
    public Agent agent()
    {
        return agent;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
    {
        ScheduledTask<?> task = new ScheduledTask<>(sequenceNumber.incrementAndGet(), delay, 0, NANOSECONDS, Executors.callable(command));
        schedule(task, delay, unit);
        return task;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
    {
        ScheduledTask<V> task = new ScheduledTask<>(sequenceNumber.incrementAndGet(), delay, 0, NANOSECONDS, callable);
        schedule(task, delay, unit);
        return task;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
    {
        ScheduledTask<?> task = new ScheduledTask<>(sequenceNumber.incrementAndGet(), initialDelay, period, unit, Executors.callable(command));
        schedule(task, initialDelay, unit);
        return task;
    }


    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
    {
        ScheduledTask<?> task = new ScheduledTask<>(sequenceNumber.incrementAndGet(), initialDelay, -delay, unit, Executors.callable(command));
        schedule(task, initialDelay, unit);
        return task;
    }

    public AgentExecutor withAgent(Agent agent)
    {
        return new SimulatedDelayedExecutorService(pending, agent, sequenceNumber);
    }
}