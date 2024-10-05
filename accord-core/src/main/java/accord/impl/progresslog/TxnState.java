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
import accord.local.SafeCommandStore;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

final class TxnState extends HomeState
{
    TxnState(TxnId txnId)
    {
        super(txnId);
    }

    void updateScheduling(SafeCommandStore safeStore, DefaultProgressLog instance, TxnStateKind updated, @Nullable BlockedUntil blockedUntil, Progress newProgress)
    {
        long newDelay;
        switch (newProgress)
        {
            default:
                throw new AssertionError("Unhandled Progress: " + newProgress);
            case NoneExpected:
            case Querying:
                newDelay = 0;
                break;
            case Queued:
                switch (updated)
                {
                    default: throw new AssertionError("Unhandled TxnStateKind: " + updated);
                    case Waiting:
                        newDelay = instance.commandStore.agent().seekProgressDelay(instance.node, safeStore, txnId, waitingRetryCounter(), blockedUntil, MICROSECONDS);
                        break;
                    case Home:
                        newDelay = instance.commandStore.agent().attemptCoordinationDelay(instance.node, safeStore, txnId, MICROSECONDS, homeRetryCounter());
                }
                Invariants.checkState(newDelay > 0);
                break;
            case Awaiting:
                int retries = updated == TxnStateKind.Home ? homeRetryCounter() : waitingRetryCounter();
                newDelay = instance.commandStore.agent().retryAwaitTimeout(instance.node, safeStore, txnId, retries, blockedUntil, MICROSECONDS);
                Invariants.checkState(newDelay > 0);
                break;
        }

        TxnStateKind scheduled = scheduledTimer();
        if (scheduled == null)
        {
            Invariants.checkState(pendingTimer() == null);
        }

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
            Invariants.checkState(pendingTimer() == null);
            otherDeadline = previousDeadline = 0;
        }

        if (newDelay == 0)
        {
            if (otherDeadline > 0)
            {
                clearPendingTimerDelay();
                setScheduledTimer(updated.other());
                instance.update(otherDeadline, this);
            }
            else if (previousDeadline > 0)
            {
                instance.unschedule(this);
            }
            else
            {
                Invariants.checkState(!isScheduled());
            }
        }
        else
        {
            long nowMicros = instance.node().elapsed(MICROSECONDS);
            long newDeadline = nowMicros + newDelay;
            if (otherDeadline == 0)
            {
                setScheduledTimer(updated);
                if (previousDeadline > 0) instance.update(newDeadline, this);
                else instance.add(newDeadline, this);
            }
            else if (newDeadline < otherDeadline)
            {
                setScheduledTimer(updated);
                setPendingTimerDelay(Ints.saturatedCast(otherDeadline - newDeadline));
                instance.update(newDeadline, this);
            }
            else
            {
                setScheduledTimer(updated.other());
                setPendingTimerDelay(Ints.saturatedCast(Math.max(1, newDeadline - otherDeadline)));
                instance.update(otherDeadline, this);
            }
        }
    }

    void maybeRemove(DefaultProgressLog instance)
    {
        if (isWaitingDone() && isHomeDoneOrUninitialised())
            instance.remove(txnId);
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
}
