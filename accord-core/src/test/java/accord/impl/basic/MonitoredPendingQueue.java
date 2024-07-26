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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoredPendingQueue implements PendingQueue
{
    private static final Logger logger = LoggerFactory.getLogger(MonitoredPendingQueue.class);
    private static final long MAX_PROGRESS_INTERVAL_MILLIS = TimeUnit.DAYS.toMillis(365L);

    final List<Throwable> failures;
    final AtomicLong progress;
    final PendingQueue wrapped;
    final long progressIntervalMillis;
    long nextProgressCheckAt = Long.MIN_VALUE;
    long nextProgressIntervalMillis;
    long lastProgressValue;

    public MonitoredPendingQueue(List<Throwable> failures, PendingQueue wrapped)
    {
        this.failures = failures;
        this.progress = null;
        this.wrapped = wrapped;
        this.progressIntervalMillis = 0;
    }

    public MonitoredPendingQueue(List<Throwable> failures, AtomicLong progress, long progressInterval, TimeUnit progressIntervalUnits, PendingQueue wrapped)
    {
        this.failures = failures;
        this.progress = progress;
        this.wrapped = wrapped;
        this.progressIntervalMillis = Math.min(MAX_PROGRESS_INTERVAL_MILLIS, progressIntervalUnits.toMillis(progressInterval));
        this.nextProgressIntervalMillis = progressIntervalMillis;
    }

    @Override
    public void add(Pending item)
    {
        wrapped.add(item);
    }

    @Override
    public void addNoDelay(Pending item)
    {
        wrapped.addNoDelay(item);
    }

    @Override
    public void add(Pending item, long delay, TimeUnit units)
    {
        wrapped.add(item, delay, units);
    }

    @Override
    public boolean remove(Pending item)
    {
        return wrapped.remove(item);
    }

    @Override
    public Pending poll()
    {
        checkFailures();
        Pending next = wrapped.poll();
        checkProgress();
        return next;
    }

    private void checkProgress()
    {
        if (progress != null && nextProgressCheckAt <= nowInMillis())
        {
            if (nextProgressCheckAt != Long.MIN_VALUE)
            {
                if (lastProgressValue == progress.get())
                {
                    logger.error("No progress in {} millis", Math.max(progressIntervalMillis, nextProgressIntervalMillis / 2));
                    nextProgressIntervalMillis *= 2;
                    if (nextProgressIntervalMillis < 0 || nextProgressIntervalMillis >= MAX_PROGRESS_INTERVAL_MILLIS)
                        nextProgressIntervalMillis = MAX_PROGRESS_INTERVAL_MILLIS;
                }
                else if (nextProgressIntervalMillis != progressIntervalMillis)
                {
                    logger.info("Progress!");
                    nextProgressIntervalMillis = progressIntervalMillis;
                }
            }
            lastProgressValue = progress.get();
            nextProgressCheckAt = nowInMillis() + nextProgressIntervalMillis;
        }
    }

    @Override
    public List<Pending> drain(Predicate<Pending> toDrain)
    {
        return wrapped.drain(toDrain);
    }

    public void checkFailures()
    {
        if (!failures.isEmpty())
        {
            AssertionError assertion = null;
            for (Throwable t : failures)
            {
                if (t instanceof AssertionError)
                {
                    assertion = (AssertionError) t;
                    break;
                }
            }
            if (assertion == null)
                assertion = new AssertionError("Unexpected exception encountered");

            for (Throwable t : failures)
            {
                if (t != assertion)
                    assertion.addSuppressed(t);
            }
            throw assertion;
        }
    }

    @Override
    public int size()
    {
        return wrapped.size();
    }

    @Override
    public long nowInMillis()
    {
        return wrapped.nowInMillis();
    }

    @Override
    public boolean hasNonRecurring()
    {
        return wrapped.hasNonRecurring();
    }

    @Override
    public Iterator<Pending> iterator()
    {
        return wrapped.iterator();
    }
}
