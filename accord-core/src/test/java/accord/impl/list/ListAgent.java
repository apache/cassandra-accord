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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import javax.annotation.Nullable;

import accord.api.CoordinatorEventListener;
import accord.api.OwnershipEventListener;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.Result;
import accord.api.Scheduler;
import accord.api.Tracing;
import accord.coordinate.Coordination;
import accord.coordinate.CoordinationFailed;
import accord.coordinate.ExecuteSyncPoint;
import accord.impl.InMemoryAgent;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemoryCommandStore.Snapshot;
import accord.impl.basic.NodeSink;
import accord.impl.basic.Packet;
import accord.impl.basic.SimulatedFault;
import accord.impl.mock.Network;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.LogUnavailableException;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.messages.ReplyContext;
import accord.primitives.Ballot;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.TopologyManager;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.agrona.collections.Int2ObjectHashMap;

import static accord.local.Node.Id.NONE;
import static com.google.common.base.Functions.identity;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ListAgent implements InMemoryAgent, CoordinatorEventListener, OwnershipEventListener
{
    final Scheduler scheduler;
    final RandomSource rnd;
    final long timeout;
    final Consumer<Throwable> onFailure;
    final Consumer<Runnable> retryBootstrap;
    final OwnershipEventListener ownershipEventListener;
    final IntSupplier coordinationDelays;
    final IntSupplier progressDelays;
    final IntSupplier timeoutDelays;
    final LongSupplier queueTimeMillis;
    final TimeService time;
    final NodeSink.TimeoutSupplier timeoutSupplier;
    final Int2ObjectHashMap<Snapshotter<Snapshot>> snapshotters = new Int2ObjectHashMap<>();

    public ListAgent(Scheduler scheduler, RandomSource rnd, long timeout, Consumer<Throwable> onFailure, Consumer<Runnable> retryBootstrap, OwnershipEventListener ownershipEventListener, IntSupplier coordinationDelays, IntSupplier progressDelays, IntSupplier timeoutDelays, LongSupplier queueTimeMillis, TimeService time, NodeSink.TimeoutSupplier timeoutSupplier)
    {
        this.scheduler = scheduler;
        this.rnd = rnd;
        this.timeout = timeout;
        this.onFailure = onFailure;
        this.retryBootstrap = retryBootstrap;
        this.ownershipEventListener = ownershipEventListener;
        this.timeoutSupplier = timeoutSupplier;
        this.coordinationDelays = coordinationDelays;
        this.progressDelays = progressDelays;
        this.timeoutDelays = timeoutDelays;
        this.queueTimeMillis = queueTimeMillis;
        this.time = time;
    }

    @Override
    public void onRecoveryStopped(Node node, TxnId txnId, Ballot ballot, Result success, Throwable fail)
    {
        if (fail != null)
        {
            Invariants.require(success == null, "fail (%s) and success (%s) are both not null", fail, success);
            // We don't really process errors for Recover here even though it is provided in the interface
        }
        if (success != null)
        {
            ListResult result = (ListResult) success;
            if (result.requestId > Integer.MIN_VALUE)
                node.reply(result.client, Network.replyCtxFor(result.requestId), result, null);
        }
    }

    @Nullable
    @Override
    public Tracing trace(TxnId txnId, Participants<?> participants, Coordination.CoordinationKind eventType)
    {
        if (rnd.nextFloat() < 0.01f)
            return (store, message) -> {};

        return null;
    }

    @Override
    public OwnershipEventListener ownershipEvents()
    {
        return this;
    }

    @Override
    public CoordinatorEventListener coordinatorEvents()
    {
        return this;
    }

    @Override
    public void onStale(Timestamp staleSince, Ranges ranges)
    {
        ownershipEventListener.onStale(staleSince, ranges);
    }

    @Override
    public void onSuccessfulBootstrap(CommandStore commandStore, int attempt, long epoch, Ranges ranges)
    {
        ownershipEventListener.onSuccessfulBootstrap(commandStore, attempt, epoch, ranges);
    }

    @Override
    public void onFailedBootstrap(int attempt, String phase, Ranges ranges, Runnable retry, Throwable failure)
    {
        retryBootstrap.accept(retry);
        ownershipEventListener.onFailedBootstrap(attempt, phase, ranges, retry, failure);
    }

    private static final Set<Class<?>> expectedExceptions = new HashSet<>(Arrays.asList(SimulatedFault.class, ExecuteSyncPoint.SyncPointErased.class, CancellationException.class, TopologyManager.TopologyRetiredException.class, Snapshotter.SnapshotAborted.class, TimeoutException.class, LogUnavailableException.class));
    @Override
    public void onUncaughtException(Throwable t)
    {
        if (expectedExceptions.contains(t.getClass()))
            return;

        if (!(t instanceof CoordinationFailed)
            && !(t.getCause() instanceof CancellationException))
            onFailure.accept(t);
    }

    @Override
    public void onCaughtException(Throwable t, String context)
    {
        onUncaughtException(t);
    }

    @Override
    public boolean rejectPreAccept(TimeService time, TxnId txnId)
    {
        return this.time.now() - txnId.hlc() > SECONDS.toMicros(10);
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
    public long slowCoordinatorDelay(Node node, SafeCommandStore safeStore, TxnId txnId, TimeUnit units, int attempt)
    {
        // TODO (required): meta randomise
        return units.convert(rnd.nextLong(100, 1000) * attempt, MILLISECONDS);
    }

    @Override
    public boolean isSlowCoordinator(long elapsed, TimeUnit units, TxnId txnId, int attempt)
    {
        return false;
    }

    @Override
    public long slowReplicaDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int attempt, BlockedUntil blockedUntil, TimeUnit units)
    {
        return units.convert(rnd.nextInt(100, 1000), MILLISECONDS);
    }

    @Override
    public long slowAwaitDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int attempt, BlockedUntil retrying, TimeUnit units)
    {
        int retryDelay = Math.min(16, 1 << attempt);
        return units.convert(retryDelay, SECONDS);
    }

    @Override
    public long retrySyncPointDelay(Node node, int attempt, TimeUnit units)
    {
        return units.convert(rnd.nextInt(30, 300), SECONDS);
    }

    @Override
    public long retryDurabilityDelay(Node node, int attempt, TimeUnit units)
    {
        return units.convert(rnd.nextInt(30, 300), SECONDS);
    }

    @Override
    public long expireEpochWait(TimeUnit units)
    {
        return units.convert(rnd.nextInt(10, 60), SECONDS);
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

    @Override
    public AsyncChain<TxnId> awaitStaleId(Node node, TxnId staleId, boolean isRequested)
    {
        // TODO (expected): metarandomise
        long lag = rnd.nextBoolean() ? rnd.nextInt(100, 1000) : rnd.nextInt(1000, 10000);
        long wait = staleId.hlc() + lag - node.now();
        if (wait <= 0)
            return AsyncChains.success(staleId);
        AsyncResult.Settable<TxnId> result = AsyncResults.settable();
        node.scheduler().selfRecurring(() -> result.setSuccess(staleId), wait, MILLISECONDS);
        return result.chain();
    }

    public long minStaleHlc(Node node, boolean isRequested)
    {
        return node.now() - SECONDS.toMillis(rnd.nextBoolean() ? 1 : 10);
    }

    @Override
    public long selfSlowAt(TxnId txnId, Status.Phase phase, TimeUnit unit)
    {
        return time.elapsed(unit) + unit.convert(timeoutSupplier.slowDelay(), timeoutSupplier.units());
    }

    @Override
    public long selfExpiresAt(TxnId txnId, Status.Phase phase, TimeUnit unit)
    {
        return time.elapsed(unit) + unit.convert(timeoutSupplier.expiresDelay(), timeoutSupplier.units());
    }

    @Override
    public AsyncResult<Void> snapshot(InMemoryCommandStore commandStore)
    {
        Snapshotter<Snapshot> snapshotter = snapshotters.computeIfAbsent(commandStore.id(), ignore -> new Snapshotter<>(scheduler, rnd));
        return commandStore.submit((PreLoadContext.Empty)() -> "Snapshot", safeStore -> snapshotter.snapshot(false, Snapshot.snapshot(commandStore)))
                           .flatMap(Function.identity());
    }

    public void restore(InMemoryCommandStore commandStore)
    {
        Snapshotter<Snapshot> snapshotter = snapshotters.get(commandStore.id());
        if (snapshotter == null)
            return;
        snapshotter.restore(snapshot -> snapshot.restore(commandStore));
    }
}
