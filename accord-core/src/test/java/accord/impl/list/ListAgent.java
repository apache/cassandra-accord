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

package accord.impl.list;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import accord.api.Agent;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.coordinate.ExecuteSyncPoint;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.impl.basic.Packet;
import accord.impl.mock.Network;
import accord.local.Command;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.messages.ReplyContext;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.RandomSource;

import static accord.local.Node.Id.NONE;
import static accord.utils.Invariants.checkState;
import static com.google.common.base.Functions.identity;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ListAgent implements Agent
{
    final RandomSource rnd;
    final long timeout;
    final Consumer<Throwable> onFailure;
    final Consumer<Runnable> retryBootstrap;
    final BiConsumer<Timestamp, Ranges> onStale;
    final IntSupplier coordinationDelays;
    final IntSupplier progressDelays;
    final IntSupplier timeoutDelays;
    final LongSupplier queueTimeMillis;
    final TimeService time;

    public ListAgent(RandomSource rnd, long timeout, Consumer<Throwable> onFailure, Consumer<Runnable> retryBootstrap, BiConsumer<Timestamp, Ranges> onStale, IntSupplier coordinationDelays, IntSupplier progressDelays, IntSupplier timeoutDelays, LongSupplier queueTimeMillis, TimeService time)
    {
        this.rnd = rnd;
        this.timeout = timeout;
        this.onFailure = onFailure;
        this.retryBootstrap = retryBootstrap;
        this.onStale = onStale;
        this.coordinationDelays = coordinationDelays;
        this.progressDelays = progressDelays;
        this.timeoutDelays = timeoutDelays;
        this.queueTimeMillis = queueTimeMillis;
        this.time = time;
    }

    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        if (fail != null)
        {
            checkState(success == null, "fail (%s) and success (%s) are both not null", fail, success);
            // We don't really process errors for Recover here even though it is provided in the interface
        }
        if (success != null)
        {
            ListResult result = (ListResult) success;
            if (result.requestId > Integer.MIN_VALUE)
                node.reply(result.client, Network.replyCtxFor(result.requestId), result, null);
        }
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        throw new AssertionError("Inconsistent execution timestamp detected for command " + command + ": " + prev + " != " + next);
    }

    @Override
    public void onFailedBootstrap(String phase, Ranges ranges, Runnable retry, Throwable failure)
    {
        retryBootstrap.accept(retry);
    }

    @Override
    public void onStale(Timestamp staleSince, Ranges ranges)
    {
        onStale.accept(staleSince, ranges);
    }

    @Override
    public void onUncaughtException(Throwable t)
    {
        if (!(t instanceof Timeout) && !(t instanceof ExecuteSyncPoint.SyncPointErased) && !(t instanceof Preempted))
            onFailure.accept(t);
    }

    @Override
    public void onHandledException(Throwable t, String context)
    {
    }

    @Override
    public long preAcceptTimeout()
    {
        return timeout;
    }

    @Override
    public long cfkHlcPruneDelta()
    {
        return 100;
    }

    @Override
    public long maxConflictsPruneInterval()
    {
        return 0;
    }

    @Override
    public int cfkPruneInterval()
    {
        return 1;
    }

    @Override
    public long maxConflictsHlcPruneDelta()
    {
        return 50;
    }

    @Override
    public Txn emptySystemTxn(Txn.Kind kind, Domain domain)
    {
        return new Txn.InMemory(kind, domain == Domain.Key ? Keys.EMPTY : Ranges.EMPTY, new ListRead(identity(), false, Keys.EMPTY, Keys.EMPTY), new ListQuery(NONE, Integer.MIN_VALUE, false), null);
    }

    @Override
    public long attemptCoordinationDelay(Node node, SafeCommandStore safeStore, TxnId txnId, TimeUnit units, int retryCount)
    {
        // TODO (testing): meta randomise
        return units.convert(rnd.nextInt(100, 1000), MILLISECONDS);
    }

    @Override
    public long seekProgressDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int retryCount, ProgressLog.BlockedUntil blockedUntil, TimeUnit units)
    {
        return units.convert(rnd.nextInt(100, 1000), MILLISECONDS);
    }

    @Override
    public long retryAwaitTimeout(Node node, SafeCommandStore safeStore, TxnId txnId, int retryCount, ProgressLog.BlockedUntil retrying, TimeUnit units)
    {
        int retryDelay = Math.min(16, 1 << retryCount);
        return units.convert(retryDelay, SECONDS);
    }

    @Override
    public long expiresAt(ReplyContext replyContext, TimeUnit unit)
    {
        long expiresAt = ((Packet)replyContext).expiresAt;
        expiresAt -= queueTimeMillis.getAsLong();
        expiresAt *= 1.8f - rnd.nextFloat();
        expiresAt += time.elapsed(MILLISECONDS);
        return unit.convert(expiresAt, MILLISECONDS);
    }


    public boolean collectMaxApplied()
    {
        // TODO (expected): randomise this to exercise both code paths
        return false;
    }
}
