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

package accord.local;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import accord.api.Agent;
import accord.api.DataStore.FetchKind;
import accord.api.DataStore.FetchResult;
import accord.coordinate.CoordinateMaxConflict;
import accord.local.ExecutionContext.Empty;
import accord.local.durability.DurabilityResults;
import accord.local.durability.DurabilityResults.ByIdEntry;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.DeterministicIdentitySet;
import accord.utils.Invariants;
import accord.utils.Reduce;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.api.DataStore.FetchKind.Image;
import static accord.local.BootstrapReason.CATCHUP;
import static accord.local.durability.DurabilityService.SyncLocal.NoLocal;
import static accord.local.durability.DurabilityService.SyncReadable.KnownReadable;
import static accord.local.durability.DurabilityService.SyncRemote.MinorityQuorumAndWaitedForAll;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

/**
 * Captures state associated with a command store's adoption of a collection of new ranges.
 * There are a number of layers to support sensible retries:
 *
 *  - The outer Bootstrap initiates one initial {@link Attempt}.
 *  - This attempt may fail some portion as it is being processed, and this portion may then be retried
 *    by the node's {@link Agent}. This will create a new {@link Attempt}
 *  - The {@link Attempt} may fail in its entirety, in which case the remaining ranges will get a new {@link Attempt}
 *  - Within each {@link Attempt} we then permit an implementation's coordinator to initiate multiple fetches for the
 *    same range, of which we only require one to succeed, but these must be managed separately as the ranges being
 *    fetched may not be identical.
 *  - Once all ranges have either been bootstrapped or invalidated (because the store no longer owns them)
 *    the promise is completed.
 *
 *   We also support aborting ranges that are no longer owned by the store, which may be passed down to the
 *   FetchCoordinator (or other implementation-defined coordinator).
 *
 * Important callback points:
 *   - Bootstrap.Attempt.starting()
 *       Invoked by system/impl, indicating we have sought a snapshot on a remote replica
 *   - FetchRange.started()
 *       Invoked by system/impl, indicating we have bound a snapshot on a remote replica and are fetching its contents
 *   - FetchRange.cancel()
 *       Invoked by system/impl, indicating we have failed an attempt to bind a snapshot on a remote replica.
 *   - Bootstrap.Attempt.invalidate()
 *       We no longer trying to fetch these ranges (perhaps because no longer own them)
 *   - Bootstrap.Attempt.maybeComplete
 *      - Invoked whenever we have finished fetching a range
 */
class Bootstrap
{
    // an attempt to fetch some portion of the range we are bootstrapping
    class Attempt extends FetchAttempt
    {
        final DurabilityResults ready;
        Attempt(Ranges ranges, int attempt, DurabilityResults ready)
        {
            super(ranges, attempt);
            this.ready = ready;
        }

        void start(SafeCommandStore safeStore)
        {
            Invariants.require(!valid.isEmpty());
            commandStore.markBootstrapping(safeStore, ready.rangesByTxnId());
            fetch(ready.byTxnId()).begin(this);
        }

        private AsyncChain<?> fetch(Map<TxnId, ByIdEntry> entries)
        {
            AsyncChain<?> chain = null;
            for (Map.Entry<TxnId, ByIdEntry> e : entries.entrySet())
            {
                if (chain == null) chain = fetch(e.getKey(), e.getValue());
                else chain = chain.flatMap(ranges -> fetch(e.getKey(), e.getValue()));
            }
            return chain != null ? chain : AsyncChains.success(null);
        }

        // TODO (expected): we should allow the implementation to define the split boundaries,
        //  so that e.g. Cassandra can prefer ranges that minimise anticompaction
        private AsyncChain<?> fetch(TxnId txnId, ByIdEntry e)
        {
            Ranges ranges;
            synchronized (this)
            {
                ranges = e.ranges.slice(valid, Minimal);
            }
            if (ranges.isEmpty())
                return AsyncChains.success(Ranges.EMPTY);

            return commandStore.chain((Empty)() -> "Submit Fetch of " + e, safeStore -> {
                FetchResult fetch = safeStore.dataStore().fetch(node, safeStore, ranges, txnId, e.readable, this, kind);
                synchronized (this)
                {
                    currentFetch = fetch;
                }
                return fetch;
            }).flatMapResult(i -> i);
        }

        @Override
        public void onNewFailure(Throwable failure, Ranges newlyFailed)
        {
            Runnable retry = () -> {
                node.scheduler().selfRecurring(() -> {
                    restart(newlyFailed, attempt + 1);
                }, 0L, TimeUnit.NANOSECONDS);
            };
            Runnable fail = () -> {
                reads.tryFailure(failure);
                data.tryFailure(failure);
            };
            commandStore.agent().ownershipEvents().onFailedBootstrap(attempt, "PartialFetch", newlyFailed, retry, fail, failure);
            Invariants.require(!newlyFailed.intersects(fetchedAndSafeToRead));
        }

        @Override
        protected AsyncResult<Void> markSafeToRead(Ranges ranges, Timestamp safeToReadAt)
        {
            List<AsyncResult<Void>> results = new ArrayList<>(ready.rangesByTxnId().size());
            for (Map.Entry<TxnId, Ranges> e : ready.rangesByTxnId().entrySet())
            {
                TxnId bound = e.getKey();
                results.add(commandStore.markSafeToRead(bound, TxnId.max(bound, safeToReadAt), ranges));
            }
            return AsyncResults.reduce(results, Reduce.toNull());
        }

        @Override
        protected void markUnsafeToRead(Ranges ranges)
        {
            commandStore.markUnsafeToRead(ranges);
        }

        @Override
        protected void complete(Ranges missing)
        {
            Bootstrap.this.complete(this);
            if (!missing.isEmpty())
            {
                Runnable retry = () -> {
                    node.scheduler().selfRecurring(() -> restart(missing, attempt + 1), 0L, TimeUnit.NANOSECONDS);
                };

                Runnable fail = () -> {
                    Throwable failure = fetchOutcome == null ? new RuntimeException("Unknown failure") : fetchOutcome;
                    reads.tryFailure(failure);
                    data.tryFailure(failure);
                };

                commandStore.agent().ownershipEvents().onFailedBootstrap(attempt, "Fetch", missing, retry, fail, fetchOutcome);
            }
            if (!fetchedAndSafeToRead.isEmpty())
                commandStore.agent().ownershipEvents().onSuccessfulBootstrap(commandStore, attempt, epoch, fetchedAndSafeToRead);
        }
    }

    final String description;
    final FetchKind kind;
    final BootstrapReason reason;
    final Node node;
    final CommandStore commandStore;
    final long epoch;
    final AsyncResult.Settable<Void> refusing;
    final AsyncResult.Settable<Void> notRefusing;
    final AsyncResult.Settable<Void> coordinate;
    final AsyncResult.Settable<Void> data;
    final AsyncResult.Settable<Void> reads;
    final Set<Attempt> inProgress = new DeterministicIdentitySet<>();
    long minEpoch, minHlc;
    TxnId min;

    final Ranges all;

    // cleared to empty when we no longer own the ranges; see maybeComplete()
    Ranges allValid, remaining;

    public Bootstrap(Node node, CommandStore commandStore, long epoch, Ranges ranges, BootstrapReason reason)
    {
        this(node, commandStore, epoch, ranges, Image, reason);
    }

    public Bootstrap(Node node, CommandStore commandStore, long epoch, Ranges ranges, FetchKind kind, BootstrapReason reason)
    {
        this.kind = kind;
        this.node = node;
        this.commandStore = commandStore;
        this.epoch = epoch;
        this.minEpoch = epoch;
        this.remaining = allValid = all = ranges;
        this.reason = reason;
        this.description = "Bootstrap " + ranges + " for epoch " + epoch + " in " + commandStore + " (" + reason + ")";
        this.refusing = new AsyncResults.SettableWithDescription<>(description);
        this.notRefusing = new AsyncResults.SettableWithDescription<>(description);
        this.coordinate = new AsyncResults.SettableWithDescription<>(description);
        this.data = new AsyncResults.SettableWithDescription<>(description);
        this.reads = new AsyncResults.SettableWithDescription<>(description);
    }

    void start(SafeCommandStore safeStore)
    {
        if (!node.topology().active().hasAtLeastEpoch(epoch))
        {
            // Ignore timeouts fetching the epoch, always keep trying to bootstrap
            node.withEpochAtLeast(epoch, null, (i1, f1) -> {
                commandStore.execute((Empty) () -> "Start Bootstrap", this::start, (i2, f2) -> {
                    if (f2 != null)
                        node.agent().acceptAndWrap(null, f2);
                });
            });
            return;
        }

        Invariants.require(all.equals(allValid));
        switch (reason)
        {
            default: throw new UnhandledEnum(reason);
            case LOG_INCOMPLETE:
            case LOG_CORRUPTED:
                commandStore.unsafeRefuseRequests(safeStore, all);
                refusing.trySuccess(null);
            case CATCHUP:
                safeStore.markUnsafeToRead(all);
            case GAIN_OWNERSHIP:
                refusing.trySuccess(null);
                notRefusing.trySuccess(null);
                withMaxConflict(0, reason.compareTo(CATCHUP) > 0);
                break;
        }
    }

    void restart(int attempt)
    {
        withQuorumBound(attempt, reason.compareTo(CATCHUP) > 0);
    }

    private void withMaxConflict(int attempt, boolean refusing)
    {
        CoordinateMaxConflict.maxConflict(node, all).begin((success, fail) -> {
            if (fail != null) commandStore.agent().ownershipEvents().onFailedBootstrap(attempt, "MaxConflict", allValid, () -> withMaxConflict(attempt + 1, refusing), doNotRetry(fail), fail);
            else
            {
                minEpoch = Math.max(minEpoch, success.epoch());
                minHlc = Math.max(minHlc, success.hlc());
                withQuorumBound(attempt, refusing);
            }
        });
    }

    /**
     * Note that we use precisely the bounds we obtain from the DurabilityService to update RedundantBefore,
     * even if we could in principle install a bound based on the max conflicts we derived first.
     * This means the dependencies of the transaction do not extend into the fuzzy period over which we're initiating
     * the process and so doesn't rely on pruning them after the fact.
     */
    private void withQuorumBound(int attempt, boolean refusing)
    {
        TxnId min = this.min = new TxnId(minEpoch, minHlc, ExclusiveSyncPoint, Domain.Range, node.id());
        MaxConflicts upsertMaxConflicts = MaxConflicts.create(allValid, MaxConflicts.Entry.create(min, min, min));
        node.durability().sync(description, null, min, allValid, null, SortedArrayList.ofSorted(node.id()), NoLocal, MinorityQuorumAndWaitedForAll, KnownReadable, 1L, TimeUnit.HOURS)
            .invoke((ready, fail) -> {
                if (fail != null)
                {
                    commandStore.agent().ownershipEvents().onFailedBootstrap(attempt, "EnsureQuorum", allValid, () -> withQuorumBound(attempt + 1, refusing), doNotRetry(fail), fail);
                    return;
                }

                commandStore.execute((Empty)() -> description, safeStore -> {
                    //noinspection SillyAssignment,DataFlowIssue
                    safeStore = safeStore;
                    // TODO (required): to guarantee we cannot break quorum, we should FIRST obtain a visibility sync point we can apply, or even make it part of the sync()
                    safeStore.upsertRedundantBefore(bounds(ready));
                    if (refusing)
                    {
                        commandStore.unsafeAcceptNonDepsRequests(safeStore, allValid);
                        notRefusing.trySuccess(null);
                    }
                    commandStore.unsafeSetMaxConflicts(commandStore.unsafeGetMaxConflicts().update(upsertMaxConflicts));
                    commandStore.readyToCoordinate(allValid, epoch)
                                .invoke(coordinate.settingCallback())
                                .invokeIfSuccess(() -> {
                                    if (refusing)
                                    {
                                        // TODO (expected): should we do something else if we lose some ranges before we reach here? Safe to refuse indefinitely.
                                        commandStore.execute((Empty)() -> description, safeStore0 -> {
                                            commandStore.unsafeAcceptRequests(safeStore0, allValid);
                                        }, node.agent());
                                    }
                                });

                    restart(safeStore, allValid, attempt, ready);
                }, node.agent());
            });

    }

    private RedundantBefore bounds(DurabilityResults ready)
    {
        if (ready == null)
            return RedundantBefore.EMPTY;

        Ranges valid = allValid;
        RedundantBefore result = RedundantBefore.EMPTY;
        for (Map.Entry<TxnId, Ranges> e : ready.rangesByTxnId().entrySet())
        {
            Ranges ranges = e.getValue().slice(valid, Minimal);
            if (ranges.isEmpty())
                continue;
            result = RedundantBefore.merge(result, RedundantBefore.create(ranges, e.getKey(), reason.redundantStatus));
        }
        return result;
    }

    private Runnable doNotRetry(Throwable failure)
    {
        return () -> {
            coordinate.tryFailure(failure);
            reads.tryFailure(failure);
            data.tryFailure(failure);
            refusing.tryFailure(failure);
            notRefusing.tryFailure(failure);
        };
    }

    private synchronized Ranges restart(Ranges ranges)
    {
        ranges = ranges.slice(allValid, Minimal);
        if (ranges.isEmpty())
            return null;

        for (Attempt attempt : inProgress)
            Invariants.requireArgument(!ranges.intersects(attempt.valid));

        return ranges;
    }

    private void restart(Ranges ranges, int attempt)
    {
        Ranges stillValid = restart(ranges);
        if (stillValid == null)
            return;

        node.durability().sync(description, null, min, stillValid, null, SortedArrayList.ofSorted(node.id()), NoLocal, MinorityQuorumAndWaitedForAll, KnownReadable, 1L, TimeUnit.HOURS)
        .invoke((ready, fail) -> {
            if (fail != null) commandStore.agent().ownershipEvents().onFailedBootstrap(attempt, "Restart Bootstrap", allValid, () -> restart(ranges, attempt + 1), doNotRetry(fail), fail);
            else commandStore.execute((Empty)() -> "", safeStore -> { restart(safeStore, stillValid, attempt, ready); }, node.agent());
        });
    }

    private synchronized void restart(SafeCommandStore safeStore, Ranges ranges, int attemptCounter, DurabilityResults ready)
    {
        ranges = restart(ranges);
        if (ranges == null)
            return;

        Attempt attempt = new Attempt(ranges, attemptCounter, ready);
        inProgress.add(attempt);
        attempt.start(safeStore);
    }

    synchronized void complete(Attempt attempt)
    {
        Invariants.requireArgument(inProgress.contains(attempt));
        Invariants.requireArgument(attempt.fetched.equals(attempt.fetchedAndSafeToRead));
        inProgress.remove(attempt);
        remaining = remaining.without(attempt.fetched);

        maybeComplete();
    }

    // distinct from abort as triggered by ourselves when we no longer own the range
    synchronized void invalidate(Ranges invalidate)
    {
        allValid = allValid.without(invalidate);
        remaining = remaining.without(invalidate);
        for (Attempt attempt : inProgress)
            attempt.invalidate(invalidate);

        maybeComplete();
    }

    private void maybeComplete()
    {
        if (inProgress.isEmpty() && remaining.isEmpty())
        {
            data.trySuccess(null);
            reads.trySuccess(null);
            commandStore.complete(this);
        }
    }
}
