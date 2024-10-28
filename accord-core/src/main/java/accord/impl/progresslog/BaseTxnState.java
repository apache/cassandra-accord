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

package accord.impl.progresslog;

import javax.annotation.Nullable;

import accord.api.ProgressLog.BlockedUntil;
import accord.local.SafeCommandStore;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.LogGroupTimers;

import static accord.impl.progresslog.TxnStateKind.Home;
import static accord.impl.progresslog.TxnStateKind.Waiting;

/**
 * We have a slightly odd class hierarchy here to cleanly group the logic, though TxnState can logically
 * be read as a single class.
 * <p>
 * We have:
 * - BaseTxnState defines the state that each of the child classes operate on and provides shared
 *   methods for updating it, particularly with relation to tracking timer scheduling
 * - WaitingState defines the methods for updating the WaitingState state machine (i.e. for local commands that are waiting on a dependency's outcome)
 * - HomeState defines the methods for updating the HomeState state machine (i.e. for coordinators ensuring a transaction completes)
 * <p>
 * TODO (required): unit test all bit fiddling methods
 */
abstract class BaseTxnState extends LogGroupTimers.Timer implements Comparable<BaseTxnState>
{
    private static final int PENDING_TIMER_SHIFT = 53;
    private static final int PENDING_TIMER_BITS = 9;
    private static final int PENDING_TIMER_LOW_BITS = PackedLogLinearInteger.validateLowBits(4, PENDING_TIMER_BITS);
    private static final long PENDING_TIMER_MASK = (1L << PENDING_TIMER_BITS) - 1;
    private static final long PENDING_TIMER_CLEAR_MASK = ~(PENDING_TIMER_MASK << PENDING_TIMER_SHIFT);
    private static final int SCHEDULED_TIMER_SHIFT = 62;
    private static final int CONTACT_ALL_SHIFT = 63;

    public final TxnId txnId;

    /**
     * bits [0..40) encode WaitingState
     * 2 bits for Progress
     * 3 bits for BlockedUntil target
     * 3 bits for BlockedUntil that home shard can satisfy
     * 32 bits for remote progress key counter [note: if we need to in future we can safely and easily reclaim bits here]
     * 3 bits for retry counter
     * <p>
     * bits [40..45) encode HomeState
     * 2 bits for Progress
     * 3 bits for CoordinatePhase
     * 3 bits for retry counter
     * <p>
     * bits [53..62) for pending timer delay
     * bit 62 for which kind of timer is scheduled
     * bit 63 for whether we should contact all candidate replicas (rather than just our preferred group)
     */
    long encodedState;

    BaseTxnState(TxnId txnId)
    {
        this.txnId = txnId;
    }

    @Override
    public int compareTo(BaseTxnState that)
    {
        return this.txnId.compareTo(that.txnId);
    }

    boolean contactEveryone()
    {
        return ((encodedState >>> CONTACT_ALL_SHIFT) & 1L) == 1L;
    }

    void setContactEveryone(boolean newContactEveryone)
    {
        encodedState &= encodedState & ~(1L << CONTACT_ALL_SHIFT);
        encodedState |= (newContactEveryone ? 1L : 0) << CONTACT_ALL_SHIFT;
    }

    /**
     * The state that has a timer actively scheduled, if any
     */
    TxnStateKind scheduledTimer()
    {
        if (!isScheduled())
            return null;
        return wasScheduledTimer();
    }

    /**
     * The state that last had a timer actively scheduled
     */
    TxnStateKind wasScheduledTimer()
    {
        return ((encodedState >>> SCHEDULED_TIMER_SHIFT) & 1) == 0 ? Waiting : Home;
    }

    /**
     * Set the state that has a timer actively scheduled
     */
    void setScheduledTimer(TxnStateKind kind)
    {
        encodedState &= ~(1L << SCHEDULED_TIMER_SHIFT);
        encodedState |= (long) kind.ordinal() << SCHEDULED_TIMER_SHIFT;
        Invariants.checkState(wasScheduledTimer() == kind);
    }

    /**
     * The state that has a timer pending, if any
     */
    TxnStateKind pendingTimer()
    {
        int pendingDelay = pendingTimerDelay();
        if (pendingDelay == 0)
            return null;
        return ((encodedState >>> SCHEDULED_TIMER_SHIFT) & 1) == 0 ? Home : Waiting;
    }

    void clearPendingTimerDelay()
    {
        encodedState &= PENDING_TIMER_CLEAR_MASK;
    }

    void setPendingTimerDelay(int deltaMicros)
    {
        clearPendingTimerDelay();
        encodedState |= (long)PackedLogLinearInteger.encode(deltaMicros, PENDING_TIMER_LOW_BITS, PENDING_TIMER_BITS) << PENDING_TIMER_SHIFT;
    }

    int pendingTimerDelay()
    {
        return PackedLogLinearInteger.decode((int)((encodedState >>> PENDING_TIMER_SHIFT) & PENDING_TIMER_MASK), PENDING_TIMER_LOW_BITS);
    }

    long pendingTimerDeadline(long scheduledDeadline)
    {
        long pendingTimerDelay = pendingTimerDelay();
        return pendingTimerDelay == 0 ? 0 : scheduledDeadline + pendingTimerDelay;
    }

    long pendingTimerDeadline()
    {
        long pendingTimerDelay = pendingTimerDelay();
        return pendingTimerDelay == 0 ? 0 : deadline() + pendingTimerDelay;
    }

    long scheduledTimerDeadline()
    {
        return deadline();
    }

    boolean isScheduled()
    {
        return isInHeap();
    }

    abstract void updateScheduling(SafeCommandStore safeStore, DefaultProgressLog instance, TxnStateKind updated, @Nullable BlockedUntil awaiting, Progress newProgress);

    abstract void maybeRemove(DefaultProgressLog instance);
}
