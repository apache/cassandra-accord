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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.VisibleForImplementation;
import accord.local.Node;
import accord.local.TimeService;
import accord.messages.Callback;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;
import org.agrona.collections.Long2ObjectHashMap;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class RequestCallbacks extends AbstractTimeouts<RequestCallbacks.CallbackStripe>
{
    private static final Logger logger = LoggerFactory.getLogger(RequestCallbacks.class);

    public interface CallbackEntry
    {
        @VisibleForImplementation
        long registeredAt(TimeUnit units);
    }

    static class CallbackStripe extends Stripe
    {
        protected class RegisteredCallback<T> extends AbstractRegistered implements CallbackEntry
        {
            final Executor executor;
            final long callbackId;
            final Callback<T> callback;
            final Node.Id to;
            final long registeredAt;
            final long reportSlowAt;
            final long reportFailAt;
            boolean cancelInFlight;

            public RegisteredCallback(Executor executor, long callbackId, Callback<T> callback, Node.Id to, long registeredAt, long reportSlowAt, long reportFailAt)
            {
                this.callbackId = callbackId;
                this.executor = executor;
                this.callback = callback;
                this.to = to;
                this.registeredAt = registeredAt;
                this.reportSlowAt = reportSlowAt;
                this.reportFailAt = reportFailAt;
            }

            public long registeredAt(TimeUnit units)
            {
                return units.convert(registeredAt, MICROSECONDS);
            }

            void cancelAlreadyRemovedWithLock()
            {
                super.cancelWithLock();
                cancelInFlight = true;
            }

            @Override
            protected boolean cancelWithLock()
            {
                if (!super.cancelWithLock())
                    return false;

                callbacks.remove(callbackId);
                cancelInFlight = true;
                return true;
            }

            @Override
            public Expiring prepareToExpire()
            {
                if (deadline() == reportFailAt)
                {
                    callbacks.remove(callbackId);
                    cancelInFlight = true;
                    return this;
                }

                Invariants.require(callbacks.containsKey(callbackId));
                timeouts.add(reportFailAt, this);
                return new Expiring()
                {
                    @Override
                    public Expiring prepareToExpire()
                    {
                        return this;
                    }

                    @Override
                    public void onExpire(long nowMicros)
                    {
                        safeInvoke(RegisteredCallback::unsafeOnSlow, null);
                    }
                };
            }

            @Override
            public void onExpire(long nowMicros)
            {
                safeInvoke(RegisteredCallback::unsafeOnFailure, (Throwable)null);
            }

            private void unsafeOnSuccess(T reply)
            {
                callback.onSuccess(to, reply);
            }

            private void unsafeOnFailure(Throwable reply)
            {
                callback.onFailure(to, reply);
            }

            private void unsafeOnSlow(Object ignore)
            {
                if (!cancelInFlight)
                    callback.onSlow(to);
            }

            <P> void safeInvoke(BiConsumer<RegisteredCallback<T>, P> invoker, P param)
            {
                executor.execute(() -> {
                    try
                    {
                        invoker.accept(this, param);
                    }
                    catch (Throwable t)
                    {
                        boolean rethrow = false;
                        try
                        {
                            if (!callback.onCallbackFailure(to, t))
                                rethrow = true;
                        }
                        catch (Throwable t2)
                        {
                            rethrow = true;
                            t.addSuppressed(t2);
                        }
                        if (rethrow)
                            throw t;
                    }
                });
            }
        }

        final Long2ObjectHashMap<RegisteredCallback> callbacks = new Long2ObjectHashMap<>();

        public CallbackStripe(TimeService time, Executor executor)
        {
            super(time, executor);
        }

        <T> RegisteredCallback<T> register(long callbackId, Executor executor, Callback<T> callback, Node.Id to, long now, long failDeadline)
        {
            return register(callbackId, executor, callback, to, now, Long.MAX_VALUE, failDeadline);
        }

        <T> RegisteredCallback<T> register(long callbackId, Executor executor, Callback<T> callback, Node.Id to, long now, long reportSlowAt, long reportFailAt)
        {
            lock();
            try
            {
                RegisteredCallback<T> registered = new RegisteredCallback<>(executor, callbackId, callback, to, now, reportSlowAt, reportFailAt);
                Object existing = callbacks.putIfAbsent(callbackId, registered);
                Invariants.require(existing == null);
                timeouts.add(Math.min(reportSlowAt, reportFailAt), registered);
                return registered;
            }
            finally
            {
                unlock(now);
            }
        }

        <T> RegisteredCallback<T> onSuccess(long callbackId, Node.Id from, T reply, boolean remove)
        {
            return safeInvoke(callbackId, from, reply, RegisteredCallback::unsafeOnSuccess, remove);
        }

        RegisteredCallback onFailure(long callbackId, Node.Id from, Throwable reply)
        {
            return safeInvoke(callbackId, from, reply, RegisteredCallback::unsafeOnFailure, true);
        }

        private <T, P> RegisteredCallback<T> safeInvoke(long callbackId, Node.Id from, P param, BiConsumer<RegisteredCallback<T>, P> invoker, boolean remove)
        {
            RegisteredCallback<T> registered = null;
            long now = time.recentElapsed(MICROSECONDS);
            lock();
            try
            {
                try
                {
                    registered = remove ? callbacks.remove(callbackId) : callbacks.get(callbackId);
                    if (registered == null)
                        return null;

                    if (remove)
                        registered.cancelAlreadyRemovedWithLock();
                    Invariants.require(registered.to.equals(from));
                }
                finally
                {
                    unlock(now);
                }
            }
            catch (Throwable t)
            {
                // we don't want to hold the lock when we invoke the callback,
                // but we also want to make sure we invoke the callback even
                // if some other callback throws an exception
                try { if (registered != null) registered.safeInvoke(invoker, param); }
                catch (Throwable t2) { t.addSuppressed(t2); }
                throw t;
            }

            registered.safeInvoke(invoker, param);
            return registered;
        }
    }

    public RequestCallbacks(TimeService time, Executor executor)
    {
        super(time, executor, CallbackStripe[]::new, CallbackStripe::new);
    }

    public RequestCallbacks(TimeService time, Executor executor, int stripeCount)
    {
        super(time, executor, stripeCount, CallbackStripe[]::new, CallbackStripe::new);
    }

    public <T> Cancellable registerAt(long callbackId, Executor executor, Callback<T> callback, Node.Id to, long now, long reportFailAt, TimeUnit units)
    {
        return registerAt(callbackId, executor, callback, to, now, Long.MAX_VALUE, reportFailAt, units);
    }

    public <T> Cancellable registerAt(long callbackId, Executor executor, Callback<T> callback, Node.Id to, long now, long reportSlowAt, long reportFailAt, TimeUnit units)
    {
        if (units != MICROSECONDS)
        {
            now = units.toMicros(now);
            reportSlowAt = reportSlowAt >= reportFailAt ? Long.MAX_VALUE : units.toMicros(reportSlowAt);
            reportFailAt = units.toMicros(reportFailAt);
        }
        return stripes[(int)callbackId & (stripes.length - 1)].register(callbackId, executor, callback, to, now, reportSlowAt, reportFailAt);
    }

    public <T> CallbackEntry onSuccess(long callbackId, Node.Id from, T reply, boolean remove)
    {
        return stripes[(int)callbackId & (stripes.length - 1)].onSuccess(callbackId, from, reply, remove);
    }

    public CallbackEntry onFailure(long callbackId, Node.Id from, Throwable reply)
    {
        return stripes[(int)callbackId & (stripes.length - 1)].onFailure(callbackId, from, reply);
    }
}
