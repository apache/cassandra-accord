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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import accord.api.Agent;
import accord.api.DataStore.FetchKind;
import accord.coordinate.CoordinateSyncPoint;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.TxnId.Cardinality;
import accord.utils.DeterministicIdentitySet;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.api.DataStore.FetchKind.Image;
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
    static class UnsafeToRead
    {
        final Ranges ranges;

        UnsafeToRead(Ranges ranges)
        {
            this.ranges = ranges;
        }
    }

    // an attempt to fetch some portion of the range we are bootstrapping
    class Attempt extends FetchAttempt
    {
        TxnId globalSyncId;

        Attempt(Ranges ranges, int attempt)
        {
            super(ranges, attempt);
        }

        TxnId start(SafeCommandStore safeStore)
        {
            globalSyncId = node.nextTxnIdWithDefaultFlags(epoch, 0, valid, ExclusiveSyncPoint, Domain.Range, Cardinality.Any);
            Invariants.require(epoch <= globalSyncId.epoch(), "Attempting to use local epoch %d which is larger than global epoch %d", epoch, globalSyncId.epoch());

            if (valid.isEmpty())
            {
                maybeComplete();
                return globalSyncId;
            }

            if (!node.topology().active().hasAtLeastEpoch(globalSyncId.epoch()))
            {
                // Ignore timeouts fetching the epoch, always keep trying to bootstrap
                node.withEpochAtLeast(globalSyncId.epoch(), null, (ignored, failure) -> commandStore.execute((ExecutionContext.Empty) () -> "Start Bootstrap", (Consumer<SafeCommandStore>) Attempt.this::start, (ignored1, failure2) -> {
                    if (failure2 != null)
                        node.agent().acceptAndWrap(null, failure2);
                }));
                return globalSyncId;
            }

            // we fix here the ranges we use for the synthetic command, even though we may end up only finishing a subset
            // of these ranges as part of this attempt
            Ranges commitRanges = valid;
            safeStore = safeStore;
            CommandStore commandStore = safeStore.commandStore();
            CoordinateSyncPoint.exclusive(node, globalSyncId, commitRanges)
                               .flatMap(success -> commandStore.chain((ExecutionContext.Empty) () -> "Mark Bootstrapping", safeStore0 -> {

                                   // we submit a separate execution so that we know markBootstrapping is durable before we initiate the fetch
                                   if (!valid.isEmpty())
                                       commandStore.markBootstrapping(safeStore0, globalSyncId, valid);
                                   return success;
                               }))
                               .flatMap(syncPoint -> node.withEpochAtLeast(epoch, null, () -> commandStore.chain((ExecutionContext.Empty) () -> "Start Bootstrap Fetch", safeStore1 -> {
                                   if (valid.isEmpty()) // we've lost ownership of the range
                                       return AsyncResults.success(Ranges.EMPTY);
                                   return fetch = safeStore1.dataStore().fetch(node, safeStore1, valid, syncPoint, this, Image);
                               })))
                               .flatMapResult(i -> i)
                               .begin(this);
            return globalSyncId;
        }

        @Override
        public void onNewFailure(Throwable failure, Ranges newlyFailed)
        {
            Runnable retry = () -> {
                node.scheduler().selfRecurring(() -> {
                    commandStore.execute((ExecutionContext.Empty) () -> "Restart Bootstrap", safeStore -> {
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
            if (safeToReadAt.compareTo(globalSyncId) < 0)
                safeToReadAt = globalSyncId;
            return commandStore.markSafeToRead(globalSyncId, safeToReadAt, ranges);
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
                        commandStore.execute((ExecutionContext.Empty) () -> "Restart Bootstrap", safeStore -> {
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

    final FetchKind kind;
    final Node node;
    final CommandStore commandStore;
    final long epoch;
    final AsyncResult.Settable<Void> data;
    final AsyncResult.Settable<Void> reads;
    final Set<Attempt> inProgress = new DeterministicIdentitySet<>();

    final Ranges all;

    // TODO (expected): handle case where we clear these to empty; should trigger promise immediately
    Ranges allValid, remaining;

    public Bootstrap(Node node, CommandStore commandStore, long epoch, Ranges ranges)
    {
        this(node, commandStore, epoch, ranges, Image);
    }

    public Bootstrap(Node node, CommandStore commandStore, long epoch, Ranges ranges, FetchKind kind)
    {
        this.kind = kind;
        this.node = node;
        this.commandStore = commandStore;
        this.epoch = epoch;
        this.remaining = allValid = all = ranges;
        String description = "Bootstrap " + ranges + " for epoch " + epoch + " in " + commandStore;
        this.data = new AsyncResults.SettableWithDescription<>(description);
        this.reads = new AsyncResults.SettableWithDescription<>(description);
    }

    TxnId start(SafeCommandStore safeStore0)
    {
        return restart(safeStore0, allValid, 0);
    }

    private synchronized TxnId restart(SafeCommandStore safeStore, Ranges ranges, int count)
    {
        ranges = ranges.overlapping(allValid);
        if (ranges.isEmpty())
            return null;

        for (Attempt attempt : inProgress)
            Invariants.requireArgument(!ranges.intersects(attempt.valid));

        Attempt attempt = new Attempt(ranges, count);
        inProgress.add(attempt);
        return attempt.start(safeStore);
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
