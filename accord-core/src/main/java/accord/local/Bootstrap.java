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
import java.util.function.Consumer;

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
import accord.primitives.TxnId.Cardinality;
import accord.utils.DeterministicIdentitySet;
import accord.utils.Invariants;
import accord.utils.Reduce;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.api.DataStore.FetchKind.Image;
import static accord.local.BootstrapReason.CATCHUP;
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
        DurabilityResults atLeast;
        Attempt(Ranges ranges, int attempt)
        {
            super(ranges, attempt);
        }

        void start(SafeCommandStore safeStore)
        {
            if (valid.isEmpty())
            {
                maybeComplete();
                return;
            }

            if (!node.topology().active().hasAtLeastEpoch(epoch))
            {
                // Ignore timeouts fetching the epoch, always keep trying to bootstrap
                node.withEpochAtLeast(epoch, null, (ignored, failure) -> commandStore.execute((Empty) () -> "Start Bootstrap", (Consumer<SafeCommandStore>) Attempt.this::start, (ignored1, failure2) -> {
                    if (failure2 != null)
                        node.agent().acceptAndWrap(null, failure2);
                }));
                return;
            }

            // we fix here the ranges we use for the synthetic command, even though we may end up only finishing a subset
            // of these ranges as part of this attempt
            Ranges commitRanges = valid;
            //noinspection SillyAssignment,DataFlowIssue prevent accidental use in lambda
            safeStore = safeStore;
            CommandStore commandStore = safeStore.commandStore();
            commandStore.prepareToBootstrap(node, description, commitRanges, reason)
                        .flatMap(success -> commandStore.chain((Empty) () -> "Mark Bootstrapping", safeStore0 -> {
                            synchronized (this)
                            {
                                // we submit a separate execution so that we know markBootstrapping is durable before we initiate the fetch
                                this.atLeast = success;
                                if (!valid.isEmpty())
                                    commandStore.markBootstrapping(safeStore0, success.rangesByTxnId());
                                return success;
                            }
                        }))
                        .flatMap(success -> node.withEpochAtLeast(epoch, null, () -> fetch(success.byTxnId())))
                        .begin(this);
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
                    commandStore.execute((Empty) () -> "Restart Bootstrap", safeStore -> {
                        restart(safeStore, newlyFailed.slice(allValid, Minimal), attempt + 1);
                    }, commandStore.agent());
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
            List<AsyncResult<Void>> results = new ArrayList<>(atLeast.rangesByTxnId().size());
            for (Map.Entry<TxnId, Ranges> e : atLeast.rangesByTxnId().entrySet())
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
                    node.scheduler().selfRecurring(() -> {
                        commandStore.execute((Empty) () -> "Restart Bootstrap", safeStore -> {
                            restart(safeStore, missing, attempt + 1);
                        }, node.agent());
                    }, 0L, TimeUnit.NANOSECONDS);
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
    final AsyncResult.Settable<Void> coordinate;
    final AsyncResult.Settable<Void> data;
    final AsyncResult.Settable<Void> reads;
    final Set<Attempt> inProgress = new DeterministicIdentitySet<>();
    long minEpoch, minHlc;

    final Ranges all;

    // TODO (expected): handle case where we clear these to empty; should trigger promise immediately
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
        this.coordinate = new AsyncResults.SettableWithDescription<>(description);
        this.data = new AsyncResults.SettableWithDescription<>(description);
        this.reads = new AsyncResults.SettableWithDescription<>(description);
    }

    void start(SafeCommandStore safeStore)
    {
        Invariants.require(all.equals(allValid));
        switch (reason)
        {
            default: throw new UnhandledEnum(reason);
            case GAIN_OWNERSHIP:
                commandStore.readyToCoordinate(all, epoch).invoke(coordinate.settingCallback());
                restart(safeStore, all, 0);
                break;
            case LOG_INCOMPLETE:
            case LOG_CORRUPTED:
                commandStore.unsafeRefuseRequests(safeStore, all);
            case CATCHUP:
                safeStore.markUnsafeToRead(all);
                withMaxConflict(0, reason != CATCHUP);
                break;
        }
    }

    private void withMaxConflict(int attempt, boolean refusing)
    {
        CoordinateMaxConflict.maxConflict(node, all).begin((success, fail) -> {
            if (fail != null) commandStore.agent().ownershipEvents().onFailedBootstrap(attempt, "MaxConflict", allValid, () -> withMaxConflict(attempt + 1, refusing), doNotRetry(fail), fail);
            else
            {
                minEpoch = Math.max(minEpoch, success.epoch());
                minHlc = Math.max(minHlc, success.hlc());

                TxnId syncId = nextSyncId();
                RedundantBefore upsertRedundantBefore = RedundantBefore.create(allValid, syncId, reason.redundantStatus);
                MaxConflicts upsertMaxConflicts = MaxConflicts.create(allValid, MaxConflicts.Entry.create(syncId, syncId, syncId));
                commandStore.execute((Empty)() -> description, safeStore -> {
                    //noinspection SillyAssignment,DataFlowIssue
                    safeStore = safeStore;
                    safeStore.upsertRedundantBefore(upsertRedundantBefore);
                    if (refusing)
                        commandStore.unsafeAcceptNonDepsRequests(safeStore, allValid);
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
                    restart(safeStore, allValid, 0);
                }, node.agent());
            }
        });
    }

    private Runnable doNotRetry(Throwable failure)
    {
        return () -> {
            coordinate.tryFailure(failure);
            reads.tryFailure(failure);
            data.tryFailure(failure);
        };
    }

    private TxnId nextSyncId()
    {
        return node.nextTxnIdWithDefaultFlags(minEpoch, minHlc, allValid, ExclusiveSyncPoint, Domain.Range, Cardinality.Any);
    }

    private synchronized void restart(SafeCommandStore safeStore, Ranges ranges, int attemptCounter)
    {
        ranges = ranges.overlapping(allValid);
        if (ranges.isEmpty())
            return;

        for (Attempt attempt : inProgress)
            Invariants.requireArgument(!ranges.intersects(attempt.valid));

        Attempt attempt = new Attempt(ranges, attemptCounter);
        inProgress.add(attempt);
        attempt.start(safeStore);
    }

    synchronized void complete(Attempt attempt)
    {
        Invariants.requireArgument(inProgress.contains(attempt));
        Invariants.requireArgument(attempt.fetched.equals(attempt.fetchedAndSafeToRead));
        inProgress.remove(attempt);
        remaining = remaining.without(attempt.fetched);
        if (inProgress.isEmpty() && remaining.isEmpty())
        {
            data.setSuccess(null);
            reads.setSuccess(null);
            commandStore.complete(this);
        }
    }

    // distinct from abort as triggered by ourselves when we no longer own the range
    synchronized void invalidate(Ranges invalidate)
    {
        allValid = allValid.without(invalidate);
        remaining = remaining.without(invalidate);
        for (Attempt attempt : inProgress)
            attempt.invalidate(invalidate);
    }
}
