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

package accord.messages;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import javax.annotation.Nullable;

import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.TxnId;
import accord.utils.MapReduceConsume;
import accord.utils.async.Cancellable;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public abstract class AbstractRequest<R extends Reply> implements PreLoadContext, Request, MapReduceConsume<SafeCommandStore, R>, Timeouts.Timeout
{
    static class Cancellation implements Cancellable
    {
        final RegisteredTimeout timeout;
        final Cancellable cancel;

        Cancellation(RegisteredTimeout timeout, Cancellable cancel)
        {
            this.timeout = timeout;
            this.cancel = cancel;
        }

        @Override
        public void cancel()
        {
            if (timeout != null) timeout.cancel();
            if (cancel != null) cancel.cancel();
        }
    }

    private static final Cancellation DONE = new Cancellation(null, null);
    private static final Cancellation EMPTY = new Cancellation(null, null);

    public final TxnId txnId;
    protected transient Node node;
    protected transient Node.Id replyTo;
    protected transient ReplyContext replyContext;

    private transient volatile Cancellation cancellation;
    private static final AtomicReferenceFieldUpdater<AbstractRequest, Cancellation> cancellationUpdater = AtomicReferenceFieldUpdater.newUpdater(AbstractRequest.class, Cancellation.class, "cancellation");

    protected AbstractRequest(TxnId txnId)
    {
        this.txnId = txnId;
    }

    @Override
    public final void process(Node on, Node.Id replyTo, ReplyContext replyContext)
    {
        this.node = on;
        this.replyTo = replyTo;
        this.replyContext = replyContext;
        Cancellable cancel = submit();
        if (cancel != null)
        {
            long expiresAt = node.agent().expiresAt(replyContext, MICROSECONDS);
            if (expiresAt > 0)
            {
                RegisteredTimeout timeout = node.timeouts().registerWithDelay(this, expiresAt, MICROSECONDS);
                Cancellation cancellation = new Cancellation(timeout, cancel);
                if (!cancellationUpdater.compareAndSet(this, null, cancellation))
                    cancellation.cancel();
            }
        }
    }

    protected abstract Cancellable submit();

    @Override
    public final void accept(R reply, Throwable failure)
    {
        cleanup(processedInternal());
        acceptInternal(reply, failure);
    }

    protected @Nullable Cancellable processedInternal()
    {
        return clearInternal().timeout;
    }

    protected void acceptInternal(R reply, Throwable failure)
    {
        node.reply(replyTo, replyContext, reply, failure);
    }

    @Override
    public final void timeout()
    {
        cleanup(timeoutInternal());
    }

    protected @Nullable Cancellable timeoutInternal()
    {
        return clearInternal().cancel;
    }

    protected boolean cancel()
    {
        Cancellation clear = clearInternal();
        if (clear == DONE)
            return false;

        cleanup(clear);
        return true;
    }

    /**
     * invoked on any termination, to ensure state is cleared
     * @return
     */
    protected void clear()
    {
        cleanup(clearInternal());
    }

    /**
     * invoked on any termination, to ensure state is cleared
     * @return
     */
    protected Cancellation clearInternal()
    {
        while (true)
        {
            // can loop at most once
            Cancellation cur = cancellation;
            if (cur == DONE || cancellationUpdater.compareAndSet(this, cur, DONE))
                return cur != null ? cur : EMPTY;
        }
    }

    @Override
    public R reduce(R o1, R o2)
    {
        throw new IllegalStateException();
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public int stripe()
    {
        return txnId.hashCode();
    }

    private static void cleanup(@Nullable Cancellable cancel)
    {
        if (cancel != null)
            cancel.cancel();
    }
}
