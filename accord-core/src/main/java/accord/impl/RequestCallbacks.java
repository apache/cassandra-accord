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
import java.util.function.BiConsumer;

import accord.local.AgentExecutor;
import accord.local.Node;
import accord.local.TimeService;
import accord.messages.Callback;
import accord.utils.Invariants;
import org.agrona.collections.Long2ObjectHashMap;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class RequestCallbacks extends AbstractRequestTimeouts<RequestCallbacks.CallbackStripe>
{
    public interface CallbackEntry
    {
        long registeredAt(TimeUnit units);
    }

    static class CallbackStripe extends Stripe
    {
        protected class RegisteredCallback<T> extends AbstractRegistered implements CallbackEntry
        {
            final AgentExecutor executor;
            final long callbackId;
            final Callback<T> callback;
            final Node.Id to;
            final long registeredAt;
            final long reportSlowAt;
            final long reportFailAt;

            public RegisteredCallback(AgentExecutor executor, long callbackId, Callback<T> callback, Node.Id to, long registeredAt, long reportSlowAt, long reportFailAt)
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

            // expects lock to already be held
            void cancelUnsafe()
            {
                if (isInHeap())
                    timeouts.remove(this);
            }

            @Override
            public Expiring prepareToExpire(Stripe stripe, long now)
            {
                Invariants.checkState(((CallbackStripe)stripe).callbacks.containsKey(callbackId));
                if (now >= reportFailAt)
                {
                    ((CallbackStripe)stripe).callbacks.remove(callbackId);
                    return this;
                }

                stripe.register(this, reportFailAt);
                return new Expiring()
                {
                    @Override
                    public Expiring prepareToExpire(Stripe stripe, long now)
                    {
                        return this;
                    }

                    @Override
                    public void onExpire(long now)
                    {
                        safeInvoke(RegisteredCallback::unsafeOnSlow, null);
                    }
                };
            }

            @Override
            public void onExpire(long now)
            {
                safeInvoke(RegisteredCallback::unsafeOnFailure, new accord.coordinate.Timeout(null, null));
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
                callback.onSlowResponse(to);
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
                        try
                        {
                            callback.onCallbackFailure(to, t);
                        }
                        catch (Throwable t2)
                        {
                            t.addSuppressed(t2);
                            executor.agent().onUncaughtException(t);
                        }
                    }
                });
            }
        }

        final Long2ObjectHashMap<RegisteredCallback> callbacks = new Long2ObjectHashMap<>();

        public CallbackStripe(TimeService time)
        {
            super(time);
        }

        <T> RegisteredCallback<T> register(long callbackId, AgentExecutor executor, Callback<T> callback, Node.Id to, long now, long failDeadline)
        {
            return register(callbackId, executor, callback, to, now, Long.MAX_VALUE, failDeadline);
        }

        <T> RegisteredCallback<T> register(long callbackId, AgentExecutor executor, Callback<T> callback, Node.Id to, long now, long reportSlowAt, long reportFailAt)
        {
            lock();
            try
            {
                RegisteredCallback<T> registered = new RegisteredCallback<>(executor, callbackId, callback, to, now, reportSlowAt, reportFailAt);
                Object existing = callbacks.putIfAbsent(callbackId, registered);
                timeouts.add(Math.min(reportSlowAt, reportFailAt), registered);
                Invariants.checkState(existing == null);
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
            lock();
            try
            {
                RegisteredCallback<T> registered = remove ? callbacks.remove(callbackId) : callbacks.get(callbackId);
                if (registered == null)
                    return null;

                if (remove) registered.cancelUnsafe();
                Invariants.checkState(registered.to.equals(from));
                registered.safeInvoke(invoker, param);
                return registered;
            }
            finally
            {
                unlock();
            }
        }
    }

    public RequestCallbacks(TimeService time)
    {
        super(time, CallbackStripe[]::new, CallbackStripe::new);
    }

    public RequestCallbacks(TimeService time, int stripeCount)
    {
        super(time, stripeCount, CallbackStripe[]::new, CallbackStripe::new);
    }

    public <T> void register(long callbackId, AgentExecutor executor, Callback<T> callback, Node.Id to, long failDelay, TimeUnit units)
    {
        long now = time.elapsed(MICROSECONDS);
        long failDeadline = now + units.toMicros(failDelay);
        stripes[(int)callbackId & (stripes.length - 1)].register(callbackId, executor, callback, to, now, failDeadline);
    }

    public <T> void register(long callbackId, AgentExecutor executor, Callback<T> callback, Node.Id to, long slowDelay, long failDelay, TimeUnit units)
    {
        long now = time.elapsed(MICROSECONDS);
        long reportFailAt = now + units.toMicros(failDelay);
        long reportSlowAt = slowDelay >= failDelay ? Long.MAX_VALUE : now + units.toMicros(slowDelay);
        stripes[(int)callbackId & (stripes.length - 1)].register(callbackId, executor, callback, to, now, reportSlowAt, reportFailAt);
    }

    public <T> void register(long callbackId, AgentExecutor executor, Callback<T> callback, Node.Id to, long now, long reportSlowAt, long reportFailAt, TimeUnit units)
    {
        if (units != MICROSECONDS)
        {
            now = units.toMicros(now);
            reportSlowAt = reportSlowAt >= reportFailAt ? Long.MAX_VALUE : units.toMicros(reportSlowAt);
            reportFailAt = units.toMicros(reportFailAt);
        }
        stripes[(int)callbackId & (stripes.length - 1)].register(callbackId, executor, callback, to, now, reportSlowAt, reportFailAt);
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
