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

package accord.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.IntFunction;

import accord.api.RequestTimeouts;
import accord.local.TimeService;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.LogGroupTimers;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class AbstractRequestTimeouts<S extends AbstractRequestTimeouts.Stripe> implements RequestTimeouts
{
    protected interface Expiring
    {
        Expiring prepareToExpire(Stripe stripe, long now);
        void onExpire(long now);
    }

    protected static class Stripe implements Runnable, Function<Stripe.AbstractRegistered, Runnable>
    {
        protected abstract class AbstractRegistered extends LogGroupTimers.Timer implements RegisteredTimeout, Expiring
        {
            @Override
            public void cancel()
            {
                lock.lock();
                try
                {
                    if (isInHeap())
                        timeouts.remove(this);
                }
                finally
                {
                    lock.unlock();
                }
            }
        }

        protected class Registered extends AbstractRegistered
        {
            final Timeout timeout;

            Registered(Timeout timeout)
            {
                this.timeout = timeout;
            }


            @Override
            public void onExpire(long now)
            {
                timeout.timeout();
            }

            @Override
            public Expiring prepareToExpire(Stripe stripe, long now)
            {
                return this;
            }
        }

        private final ReentrantLock lock = new ReentrantLock();
        final TimeService time;
        final LogGroupTimers<AbstractRegistered> timeouts = new LogGroupTimers<>(TimeUnit.MICROSECONDS);

        public Stripe(TimeService time)
        {
            this.time = time;
        }

        @Override
        public void run()
        {
            long now = 0;
            try (BufferList<Expiring> collect = new BufferList<>())
            {
                int i = 0;
                try
                {
                    lock.lock();
                    try
                    {
                        now = time.elapsed(MICROSECONDS);
                        timeouts.advance(now, collect, BufferList::add);

                        // prepare expiration while we hold the lock - this is to permit expiring objects to
                        // reschedule themselves while returning some immediate expiry work to do
                        for (int j = 0 ; j < collect.size() ; ++j)
                        {
                            Expiring in = collect.get(j);
                            Expiring out = in.prepareToExpire(this, now);
                            if (in != out)
                                collect.set(j, out);
                        }
                    }
                    finally
                    {
                        lock.unlock();
                    }

                    while (i < collect.size())
                        collect.get(i++).onExpire(now);
                }
                catch (Throwable t)
                {
                    while (i < collect.size())
                    {
                        try
                        {
                            collect.get(i++).onExpire(now);
                        }
                        catch (Throwable t2)
                        {
                            t.addSuppressed(t2);
                        }
                    }
                    throw t;
                }
            }
        }

        protected void register(AbstractRegistered register, long deadline)
        {
            timeouts.add(deadline, register);
        }

        @Override
        public Runnable apply(AbstractRegistered registered)
        {
            return this;
        }

        RegisteredTimeout tryRegister(Timeout timeout, long now, long deadline)
        {
            if (!tryLock())
                return null;

            try
            {
                Registered registered = new Registered(timeout);
                register(registered, deadline);
                return registered;
            }
            finally
            {
                unlock(now);
            }
        }

        protected void lock()
        {
            lock.lock();
        }

        protected boolean tryLock()
        {
            return lock.tryLock();
        }

        protected void unlock()
        {
            try
            {
                if (timeouts.shouldWake(time.elapsed(MICROSECONDS)))
                    run();
            }
            finally
            {
                lock.unlock();
            }
        }

        protected void unlock(long now)
        {
            try
            {
                if (timeouts.shouldWake(now))
                    run();
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    final TimeService time;
    final S[] stripes;

    protected AbstractRequestTimeouts(TimeService time, IntFunction<S[]> arrayAllocator, Function<TimeService, S> stripeFactory)
    {
        this(time, 8, arrayAllocator, stripeFactory);
    }

    public AbstractRequestTimeouts(TimeService time, int stripeCount, IntFunction<S[]> allocator, Function<TimeService, S> stripeFactory)
    {
        if (stripeCount > 1024)
            throw new IllegalArgumentException("Far too many stripes requested");
        this.stripes = allocator.apply(stripeCount);
        for (int i = 0 ; i < stripes.length ; ++i)
            stripes[i] = stripeFactory.apply(time);
        this.time = time;
    }

    @Override
    public RegisteredTimeout register(Timeout timeout, long delay, TimeUnit units)
    {
        long now = time.elapsed(MICROSECONDS);
        long deadline = now + Math.max(1, MICROSECONDS.convert(delay, units));
        int i = timeout.stripe() & (stripes.length - 1);
        while (true)
        {
            RegisteredTimeout result = stripes[i].tryRegister(timeout, now, deadline);
            if (result != null)
                return result;
            i = (i + 1) & (stripes.length - 1);
        }
    }

    // a fallback method to be invoked at some frequency, to notify pending timeouts in the event that a stripe has been idle too long
    @Override
    public void maybeNotify()
    {
        long nowMicros = time.elapsed(MICROSECONDS);
        for (Stripe stripe : stripes)
        {
            if (stripe.timeouts.shouldWake(nowMicros) && stripe.tryLock())
                stripe.unlock(nowMicros);
        }
    }
}
