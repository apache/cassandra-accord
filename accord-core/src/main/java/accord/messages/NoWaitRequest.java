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

import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.api.VisibleForImplementationTesting;
import accord.impl.LocalDelivery;
import accord.local.Node;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.NoStackCancellationException;
import accord.utils.async.Cancellable;

import static accord.utils.Invariants.illegalState;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public abstract class NoWaitRequest<P extends Participants<?>, R> extends AbstractRequest<P, R> implements Timeouts.Timeout
{
    private boolean hasSentFinalReply;

    protected NoWaitRequest(TxnId txnId, P scope)
    {
        super(txnId, scope);
    }

    @Override
    public final Cancellable process(Node on, Node.Id replyTo, ReplyContext replyContext)
    {
        this.replyTo = replyTo;
        this.replyContext = replyContext;
        return process(on, replyContext.expiresAt(MICROSECONDS));
    }

    public final Cancellable process(Node on, long expiresAt)
    {
        this.node = on;
        if (tracing() != null)
            tracing().trace(null, "Submitting");
        Cancellable cancel = submit();
        RegisteredTimeout timeout = null;
        if (cancel != null)
        {
            if (expiresAt > 0)
                timeout = node.timeouts().registerAt(this, expiresAt, MICROSECONDS);
        }
        setupOnce(cancel, timeout);
        return this;
    }

    protected abstract Cancellable submit();

    @Override
    public final void accept(R reply, Throwable failure)
    {
        if (failure == null) trySuccess();
        else tryCancel();
        acceptInternal(reply, failure);
    }

    protected abstract void acceptInternal(R reply, Throwable failure);

    protected void acceptReply(Reply reply, Throwable failure)
    {
        {
            if (reply == null && failure == null)
            {
                Invariants.require(isCancelled());
                if (!(replyContext instanceof LocalDelivery<?>))
                {
                    if (tracing() != null)
                        tracing().trace(null, "Completed with no reply");
                    return; // for now we don't report cancellation/timeout remotely, and rely on the coordinator's timeouts
                }
                // we must report something for local delivery, as we rely on this callback instead of registering a separate timeout
                failure = new NoStackCancellationException();
            }
            if (failure != null || reply.isFinal())
            {
                Invariants.require(!hasSentFinalReply);
                hasSentFinalReply = true;
            }
            if (failure != null) tryCancel();
            node.reply(replyTo, replyContext, reply, failure, tracing());
        }
    }

    @Override
    public final void timeout()
    {
        if (tryTimeout() && tracing() != null)
            tracing().trace(null, "Timeout");
    }

    public ReplyContext replyContext()
    {
        return replyContext;
    }

    @Override
    public R reduce(R o1, R o2)
    {
        throw illegalState();
    }

    @Override
    public int stripe()
    {
        return txnId.hashCode();
    }

    @Override
    public String reason()
    {
        return getClass().getSimpleName();
    }

    @VisibleForImplementationTesting
    public void unsafeSetNode(Node node) { this.node = node; }
}
