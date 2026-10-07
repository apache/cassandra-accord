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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import accord.api.MessageSink;
import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.api.Timeouts.SentinelRegisteredTimeout;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class LocalDelivery<R extends Reply> implements ReplyContext
{
    // TODO (desired): refactor expiration passing, so we don't need to stash here
    final Node node;
    final Callback<R> callback;
    private long expiresAtMicros;
    private volatile RegisteredTimeout timeout;

    private static final SentinelRegisteredTimeout CLEARED_TIMEOUT = new SentinelRegisteredTimeout();
    private static final AtomicReferenceFieldUpdater<LocalDelivery, RegisteredTimeout> timeoutUpdater = AtomicReferenceFieldUpdater.newUpdater(LocalDelivery.class, RegisteredTimeout.class, "timeout");

    public LocalDelivery(Node node, Callback<R> callback)
    {
        this.node = node;
        this.callback = callback;
    }

    public Cancellable deliver(Request request)
    {
        TxnId txnId = request.primaryTxnId();
        MessageType messageType = request.type();
        this.expiresAtMicros = node.agent().selfExpiresAt(txnId, messageType, MICROSECONDS);
        long slowAtMicros = node.agent().selfSlowAt(txnId, messageType, MICROSECONDS);

        // always set a timeout, to ensure all local deliveries eventually report success or failure
        // note that this timeout does not cancel the local work, it only reports slow/expiry to the coordinator
        RegisteredTimeout timeout = slowOrExpiryTimeout(slowAtMicros);
        if (!timeoutUpdater.compareAndSet(this, null, timeout))
            timeout.cancel();
        return request.process(node, node.id(), this);
    }

    private RegisteredTimeout slowOrExpiryTimeout(long slowAtMicros)
    {
        if (slowAtMicros <= 0 || slowAtMicros >= expiresAtMicros)
            return expiresTimeout();

        return node.timeouts().registerAt(new Timeouts.Timeout()
        {
            @Override public void timeout() { onSlow(); }
            @Override public int stripe() { return (int)expiresAtMicros; }
        }, slowAtMicros, MICROSECONDS);
    }

    private RegisteredTimeout expiresTimeout()
    {
        return node.timeouts().registerAt(new Timeouts.Timeout()
        {
            @Override public void timeout()
            {
                if (clearTimeout() != CLEARED_TIMEOUT)
                    callback.onFailure(node.id(), null);
            }
            @Override public int stripe() { return (int)expiresAtMicros; }
        }, expiresAtMicros, MICROSECONDS);
    }

    private void onSlow()
    {
        RegisteredTimeout slowTimeout = this.timeout;
        if (slowTimeout == CLEARED_TIMEOUT)
            return;

        RegisteredTimeout expiresTimeout = expiresTimeout();
        if (timeoutUpdater.compareAndSet(this, slowTimeout, expiresTimeout)) callback.onSlow(node.id());
        else expiresTimeout.cancel();
    }

    private void onSuccess(R reply)
    {
        // TODO (desired): we can in theory race with a slow or expiry timeout here
        if (reply.isFinal() ? cancelTimeout() : timeout != CLEARED_TIMEOUT)
            callback.onSuccess(node.id(), reply);
    }

    private void onFailure(Throwable t)
    {
        if (cancelTimeout())
            callback.onFailure(node.id(), t);
    }

    private boolean cancelTimeout()
    {
        RegisteredTimeout timeout = clearTimeout();
        if (timeout == CLEARED_TIMEOUT)
            return false;
        if (timeout != null)
            timeout.cancel();
        return true;
    }

    private RegisteredTimeout clearTimeout()
    {
        return timeoutUpdater.getAndSet(this, CLEARED_TIMEOUT);
    }

    @Override
    public long expiresAt(TimeUnit units)
    {
        return units.convert(expiresAtMicros, MICROSECONDS);
    }

    @Override
    public void reply(Node.Id to, MessageSink sink, Reply success, Throwable failure)
    {
        if (success != null) onSuccess((R)success);
        else if (failure != null) onFailure(failure);
        else Invariants.expect(false, "Both success and failure are null");
    }
}
