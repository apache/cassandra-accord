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

import accord.api.Timeouts;
import accord.local.TimeService;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.LogGroupTimers;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class AbstractTimeouts<S extends AbstractTimeouts.Stripe> implements Timeouts
{
    protected interface Expiring
    {
        Expiring prepareToExpire();
        void onExpire();
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
            public void onExpire()
            {
                timeout.timeout();
            }

            @Override
            public Expiring prepareToExpire()
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
            long now = time.elapsed(MICROSECONDS);
            lock();
            unlock(now);
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
                timeouts.add(deadline, registered);
                return registered;
            }
            finally
            {
                unlock(now);
            }
        }

        protected void lock()
        {
            //noinspection LockAcquiredButNotSafelyReleased
            lock.lock();
        }

        protected boolean tryLock()
        {
            return lock.tryLock();
        }

        protected void unlock(long now)
        {
            int i = 0;
            BufferList<Expiring> expire = null;
            try
            {
                try
                {
                    if (!timeouts.shouldWake(now))
                        return;

                    expire = new BufferList<>();
                    timeouts.advance(now, expire, BufferList::add);

                    // prepare expiration while we hold the lock - this is to permit expiring objects to
                    // reschedule themselves while returning some immediate expiry work to do
                    for (int j = 0; j < expire.size(); ++j)
                    {
                        Expiring in = expire.get(j);
                        Expiring out = in.prepareToExpire();
                        if (in != out)
                            expire.set(j, out);
                    }
                }
                finally
                {
                    lock.unlock();
                }

                // we want to process these without the lock
                while (i < expire.size())
                    expire.get(i++).onExpire();
            }
            catch (Throwable t)
            {
                if (expire != null)
                {
                    while (i < expire.size())
                    {
                        try
                        {
                            expire.get(i++).onExpire();
                        }
                        catch (Throwable t2)
                        {
                            t.addSuppressed(t2);
                        }
                    }
                }
            }
            finally
            {
                if (expire != null)
                    expire.close();
            }
        }
    }

    final TimeService time;
    final S[] stripes;

    protected AbstractTimeouts(TimeService time, IntFunction<S[]> arrayAllocator, Function<TimeService, S> stripeFactory)
    {
        this(time, 8, arrayAllocator, stripeFactory);
    }

    public AbstractTimeouts(TimeService time, int stripeCount, IntFunction<S[]> allocator, Function<TimeService, S> stripeFactory)
    {
        if (stripeCount > 1024)
            throw new IllegalArgumentException("Far too many stripes requested");
        this.stripes = allocator.apply(stripeCount);
        for (int i = 0 ; i < stripes.length ; ++i)
            stripes[i] = stripeFactory.apply(time);
        this.time = time;
    }

    @Override
    public RegisteredTimeout registerWithDelay(Timeout timeout, long delay, TimeUnit units)
    {
        long now = time.elapsed(MICROSECONDS);
        long deadline = now + Math.max(1, units.toMicros(delay));
        return registerAt(timeout, now, deadline);
    }

    @Override
    public RegisteredTimeout registerAt(Timeout timeout, long deadline, TimeUnit units)
    {
        long now = time.elapsed(MICROSECONDS);
        deadline = units.toMicros(deadline);
        return registerAt(timeout, now, deadline);
    }

    private RegisteredTimeout registerAt(Timeout timeout, long nowMicros, long deadlineMicros)
    {
        int i = timeout.stripe() & (stripes.length - 1);
        while (true)
        {
            RegisteredTimeout result = stripes[i].tryRegister(timeout, nowMicros, deadlineMicros);
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
