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

import accord.api.RequestTimeouts;
import accord.local.Node;
import accord.utils.LogGroupTimers;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.agrona.BitUtil.findNextPositivePowerOfTwo;

public class DefaultRequestTimeouts implements RequestTimeouts
{
    static class Stripe implements Runnable, Function<Stripe.Registered, Runnable>
    {
        class Registered extends LogGroupTimers.Timer implements RegisteredTimeout
        {
            final Timeout timeout;

            Registered(Timeout timeout)
            {
                this.timeout = timeout;
            }

            @Override
            public void cancel()
            {
                lock.lock();
                try
                {
                    timeouts.remove(this);
                }
                finally
                {
                    lock.unlock();
                }
            }
        }

        final ReentrantLock lock = new ReentrantLock();
        final Node node;
        final LogGroupTimers<Registered> timeouts = new LogGroupTimers<>(TimeUnit.MILLISECONDS);
        final LogGroupTimers<Registered>.Scheduling timeoutScheduling;

        public Stripe(Node node)
        {
            this.node = node;
            this.timeoutScheduling = timeouts.new Scheduling(node.scheduler(), this,
                                                             100, 1L, SECONDS.toMillis(1));
        }

        public RegisteredTimeout tryRegister(Timeout timeout, long now, long deadline)
        {
            if (!lock.tryLock())
                return null;

            try
            {
                Registered registered = new Registered(timeout);
                timeouts.add(deadline, registered);
                timeoutScheduling.maybeReschedule(now, deadline);
                return registered;
            }
            finally
            {
                lock.unlock();
            }
        }

        @Override
        public void run()
        {
            lock.lock();
            try
            {
                long now = node.elapsed(MILLISECONDS);
                // TODO (expected): should we handle reentrancy? Or at least throw an exception?
                timeouts.advance(now, this, (s, r) -> r.timeout.timeout());
            }
            finally
            {
                lock.unlock();
            }
        }

        @Override
        public Runnable apply(Registered registered)
        {
            return this;
        }
    }

    final Node node;
    final Stripe[] stripes;

    public DefaultRequestTimeouts(Node node)
    {
        this(node, 8);
    }

    public DefaultRequestTimeouts(Node node, int stripeCount)
    {
        if (stripeCount > 1024)
            throw new IllegalArgumentException("Far too many stripes requested");
        this.stripes = new Stripe[findNextPositivePowerOfTwo(stripeCount)];
        for (int i = 0 ; i < stripes.length; ++i)
            stripes[i] = new Stripe(node);
        this.node = node;
    }

    @Override
    public RegisteredTimeout register(Timeout timeout, long delay, TimeUnit units)
    {
        long now = node.elapsed(MILLISECONDS);
        long deadline = now + Math.max(1, MILLISECONDS.convert(delay, units));
        int i = timeout.hashCode() & (stripes.length - 1);
        while (true)
        {
            RegisteredTimeout result = stripes[i].tryRegister(timeout, now, deadline);
            if (result != null)
                return result;
            i = (i + 1) & (stripes.length - 1);
        }
    }
}
