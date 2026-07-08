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

import com.google.common.primitives.Ints;

import accord.api.ProgressLog.BlockedUntil;
import accord.local.ExecutionContext;
import accord.local.SafeCommandStore;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class TxnState extends WaitingState implements ExecutionContext
{
    public static class SerializationSupport
    {
        public static TxnState create(TxnId txnId, long encoded)
        {
            TxnState result = new TxnState(txnId);
            result.encodedState = encoded;
            return result;
        }
    }

    TxnState(TxnId txnId)
    {
        super(txnId);
    }

    void updateScheduling(SafeCommandStore safeStore, DefaultProgressLog owner, TxnStateKind updated, @Nullable BlockedUntil blockedUntil, Progress newProgress)
    {
        long newDelay;
        switch (newProgress)
        {
            default:
                throw new UnhandledEnum(newProgress);
            case NoneExpected:
                newDelay = 0;
                break;
            case Querying:
                newDelay = NANOSECONDS.toMicros(owner.config().maxActiveRunTime.toNanos());
                Invariants.require(newDelay >= 0);
                break;
            case Queued:
                if (owner.isCatchingUp())
                {
                    newDelay = 1;
                }
                else
                {
                    switch (updated)
                    {
                        default: throw new UnhandledEnum(updated);
                        case Waiting:
                            newDelay = owner.commandStore.agent().slowReplicaDelay(owner.node, safeStore, txnId, 1 + waitingRunCounter(), blockedUntil, MICROSECONDS);
                            break;
                        case Home:
                            newDelay = owner.commandStore.agent().slowCoordinatorDelay(owner.node, safeStore, txnId, MICROSECONDS, 1 + homeRunCounter());
                    }
                }
                Invariants.require(newDelay > 0);
                break;
            case Awaiting:
                int retries = updated == TxnStateKind.Home ? homeRunCounter() : waitingRunCounter();
                newDelay = owner.commandStore.agent().slowAwaitDelay(owner.node, safeStore, txnId, 1 + retries, blockedUntil, MICROSECONDS);
                Invariants.require(newDelay > 0);
                break;
        }

        TxnStateKind scheduled = scheduledTimer();
        Invariants.require(scheduled != null || pendingTimer() == null);

        // previousDeadline is the previous deadline of <updated>;
        // otherDeadline is the active deadline (if any) of <updated.other()>
        long previousDeadline, otherDeadline;
        if (scheduled == updated)
        {
            previousDeadline = deadline();
            otherDeadline = pendingTimerDeadline(previousDeadline);
        }
        else if (scheduled != null)
        {
            otherDeadline = deadline();
            previousDeadline = pendingTimerDeadline(otherDeadline);
        }
        else
        {
            Invariants.require(pendingTimer() == null);
            otherDeadline = previousDeadline = 0;
        }

        if (newDelay == 0)
        {
            if (otherDeadline > 0)
            {
                clearPendingTimerDelay();
                setScheduledTimer(updated.other());
                owner.update(otherDeadline, this);
            }
            else if (previousDeadline > 0)
            {
                owner.unschedule(this);
            }
            else
            {
                Invariants.require(!isScheduled());
            }
        }
        else
        {
            long nowMicros = owner.node().recentElapsed(MICROSECONDS);
            long newDeadline = nowMicros + newDelay;
            if (otherDeadline == 0)
            {
                setScheduledTimer(updated);
                if (previousDeadline > 0) owner.update(newDeadline, this);
                else owner.add(newDeadline, this);
            }
            else if (newDeadline < otherDeadline)
            {
                setScheduledTimer(updated);
                setPendingTimerDelay(Ints.saturatedCast(otherDeadline - newDeadline));
                owner.update(newDeadline, this);
            }
            else
            {
                setScheduledTimer(updated.other());
                setPendingTimerDelay(Ints.saturatedCast(Math.max(1, newDeadline - otherDeadline)));
                owner.update(otherDeadline, this);
            }
        }
    }

    boolean maybeRemove(DefaultProgressLog instance)
    {
        if (!(isWaitingDone() && isHomeDoneOrUninitialised()))
            return false;

        instance.remove(txnId);
        return true;
    }

    HomeState home()
    {
        return this;
    }

    WaitingState waiting()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return txnId + ": " + toStateString();
    }

    public boolean isDone(TxnStateKind runKind)
    {
        return runKind == TxnStateKind.Home ? isHomeDone() : isWaitingDone();
    }

    @Nullable
    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public String reason()
    {
        return "Progress";
    }

    public TxnState snapshot()
    {
        TxnState copy = new TxnState(txnId);
        copy.encodedState = (encodedState & (SNAPSHOT_HOME_MASK | SNAPSHOT_WAITING_MASK)) | RESTORED_BIT;
        return copy;
    }

    public long encodedState()
    {
        return encodedState;
    }

    public boolean equals(Object that)
    {
        return that instanceof TxnState && equals((TxnState) that);
    }

    public boolean equals(TxnState that)
    {
        return this.txnId.equals(that.txnId) && this.encodedState == that.encodedState;
    }
}
