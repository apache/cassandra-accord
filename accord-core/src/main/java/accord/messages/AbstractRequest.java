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
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Timeouts.RegisteredTimeout;
import accord.local.MapReduceConsumeCommandStores;
import accord.local.Node;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

// TODO (desired): migrate to ExclusiveExecutor approach used by Coordinator logic, rather than synchronized
// TODO (desired): allow a task to be associated with more than one ExclusiveExecutor, and only commit a thread when both are ready to schedule it
public abstract class AbstractRequest<P extends Participants<?>, R> extends MapReduceConsumeCommandStores<P, R> implements Request, Cancellable
{
    static class Pending implements Cancellable
    {
        final @Nullable Cancellable cancel;
        final @Nullable RegisteredTimeout timeout;

        private Pending(@Nullable Cancellable cancel, @Nullable RegisteredTimeout timeout)
        {
            this.cancel = cancel;
            this.timeout = timeout;
        }

        @Override
        public void cancel()
        {
            if (cancel != null) cancel.cancel(); // run cancel first; if throws less important that timeout isn't cancelled
            if (timeout != null) timeout.cancel();
        }

        final boolean isDone()
        {
            return getClass() == Done.class;
        }
    }

    private static final class Done extends Pending { Done() { super(null, null); } }

    private static final Done CANCELLED = new Done();
    private static final Done DONE = new Done();
    private static final Pending EMPTY = new Pending(null, null);

    private transient volatile Pending pending;
    private static final AtomicReferenceFieldUpdater<AbstractRequest, Pending> pendingUpdater = AtomicReferenceFieldUpdater.newUpdater(AbstractRequest.class, Pending.class, "pending");

    protected transient Node node;
    protected transient Node.Id replyTo;
    protected transient ReplyContext replyContext;

    public final TxnId txnId;

    protected AbstractRequest(TxnId txnId, P scope)
    {
        super(scope);
        this.txnId = txnId;
    }

    @Nullable
    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    /**
     * Setup a timeout for work produced by {@link #process} (may race with {@link #setupWork(Cancellable)}).
     */
    protected final void setupTimeout(@Nonnull RegisteredTimeout timeout)
    {
        Pending prev = setup(timeout, (cur, t) -> new Pending(cur.cancel, t));
        if (prev.isDone())
            timeout.cancel();
    }

    /**
     * Setup a cancellation for work produced by {@link #process} (may race with {@link #setupTimeout(RegisteredTimeout)}).
     */
    protected final void setupWork(@Nullable Cancellable cancel)
    {
        Pending prev = setup(cancel, (cur, c) -> new Pending(c, cur.timeout));
        if (CANCELLED == prev)
            cleanup(cancel);
    }

    private <C extends Cancellable> Pending setup(C cancel, BiFunction<Pending, C, Pending> merge)
    {
        Pending cur = pending;
        if (cur == null)
        {
            if (pendingUpdater.compareAndSet(this, null, merge.apply(EMPTY, cancel)))
                return EMPTY;

            cur = pending;
        }

        while (!cur.isDone() && !pendingUpdater.compareAndSet(this, cur, merge.apply(cur, cancel)))
            cur = pending;

        return cur;
    }

    /**
     * Setup any cancellation produced by {@link #process}, and associated timeout.
     * Expects no other updates to timeouts or work cancellations (besides completion via success/cancellation).
     */
    protected final void setupOnce(@Nullable Cancellable cancel, @Nullable RegisteredTimeout timeout)
    {
        Pending pending = cancel == null && timeout == null ? EMPTY : new Pending(cancel, timeout);
        if (!pendingUpdater.compareAndSet(this, null, pending))
            cleanup(this.pending == CANCELLED ? pending : pending.timeout);
    }

    protected boolean isDone()
    {
        return pending instanceof Done;
    }

    protected boolean isCancelled()
    {
        return pending == CANCELLED;
    }

    protected boolean ifDoneExpectCancelled()
    {
        if (!isDone())
            return false;
        Invariants.require(pending == CANCELLED);
        return true;
    }

    /**
     * Try to mark the request as DONE (success), returning the timeout (if any) for the caller to cancel, e.g. once outside a lock.
     * Ensures the request is either DONE or CANCELLED.
     */
    protected final @Nullable Cancellable ensureDone()
    {
        return trySetDone(DONE).timeout;
    }

    /**
     * If already done (e.g. cancelled) return false;
     * otherwise mark the request completed and cancel its timeout (if any).
     */
    protected final boolean trySuccess()
    {
        Pending result = trySetDone(DONE);
        if (result.isDone())
            return false;

        cleanupOnSuccess(result.timeout);
        return true;
    }

    @Override
    public final void cancel()
    {
        tryCancel();
    }

    /**
     * If already done/cancelled return false;
     * otherwise mark the request cancelled, and cancel its execution
     */
    protected boolean tryTimeout()
    {
        return tryCancel(true);
    }

    protected final boolean tryCancel()
    {
        return tryCancel(false);
    }

    /**
     * If already done/cancelled return false;
     * otherwise mark the request cancelled, and cancel its execution and timeout.
     */
    protected boolean tryCancel(boolean isTimeout)
    {
        Pending cancel = trySetDone(CANCELLED);
        if (cancel.isDone())
            return false;

        cleanupOnFailure(isTimeout ? cancel.cancel : cancel);
        return true;
    }

    Pending trySetCancelled()
    {
        return trySetDone(CANCELLED);
    }

    private Pending trySetDone(Done done)
    {
        while (true)
        {
            // can loop at most once
            Pending cur = pending;
            if (cur instanceof Done)
                return cur;

            if (pendingUpdater.compareAndSet(this, cur, done))
                return cur != null ? cur : EMPTY;
        }
    }

    protected void cleanupOnSuccess(@Nullable Cancellable cancel)
    {
        cleanup(cancel);
    }

    protected void cleanupOnFailure(@Nullable Cancellable cancel)
    {
        cleanup(cancel);
    }

    protected static void cleanup(@Nullable Cancellable cancel)
    {
        if (cancel != null)
            cancel.cancel();
    }
}
