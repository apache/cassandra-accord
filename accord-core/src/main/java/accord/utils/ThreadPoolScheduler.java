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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import accord.api.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static accord.utils.Invariants.illegalState;

public class ThreadPoolScheduler implements Scheduler
{
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolScheduler.class);
    final ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
    public ThreadPoolScheduler()
    {
        exec.setMaximumPoolSize(1);
    }

    static class FutureAsScheduled implements Scheduled
    {
        final ScheduledFuture<?> f;

        FutureAsScheduled(ScheduledFuture<?> f)
        {
            this.f = f;
        }

        @Override
        public void cancel()
        {
            f.cancel(true);
        }

        @Override
        public boolean isDone()
        {
            return f.isDone();
        }
    }

    static Runnable wrap(Runnable runnable)
    {
        return () ->
        {
            try
            {
                runnable.run();
            }
            catch (Throwable t)
            {
                logger.error("Unhandled Exception", t);
                throw t;
            }
        };
    }

    @Override
    public Scheduled recurring(Runnable run, long delay, TimeUnit units)
    {
        return new FutureAsScheduled(exec.scheduleWithFixedDelay(wrap(run), delay, delay, units));
    }

    @Override
    public Scheduled once(Runnable run, long delay, TimeUnit units)
    {
        return new FutureAsScheduled(exec.schedule(wrap(run), delay, units));
    }

    @Override
    public void now(Runnable run)
    {
        exec.execute(wrap(run));
    }

    public void stop()
    {
        exec.shutdown();
        try
        {
            if (!exec.awaitTermination(1L, TimeUnit.MINUTES))
                throw illegalState("did not terminate");
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
