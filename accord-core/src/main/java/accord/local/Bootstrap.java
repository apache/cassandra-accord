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
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.DataStore.FetchRanges;
import accord.api.DataStore.FetchResult;
import accord.api.DataStore.StartingRangeFetch;
import accord.coordinate.CoordinateSyncPoint;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.DeterministicIdentitySet;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.local.PreLoadContext.empty;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.Invariants.illegalState;

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
 * 
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
    class Attempt implements FetchRanges, BiConsumer<Object, Throwable>
    {
        final DeterministicIdentitySet<UnsafeToRead> unsafeToReads = new DeterministicIdentitySet<>();
        ReducingRangeMap<Timestamp> safeToReadAts = new ReducingRangeMap<>();

        Runnable cancel;
        FetchResult fetch;

        /**
         * valid: the ranges we are still meant to fetch - i.e. excluding those that have been invalidated or marked failed
         */
        Ranges valid, fetched = Ranges.EMPTY, fetchedAndSafeToRead = Ranges.EMPTY;
        boolean fetchCompleted;
        boolean completed; // we have finished fetching all the data we are able to, but we may still have in-flight fetches
        Throwable fetchOutcome;
        TxnId globalSyncId;

        Attempt(Ranges ranges)
        {
            this.valid = ranges;
        }

        void start(SafeCommandStore safeStore)
        {
            if (valid.isEmpty())
            {
                maybeComplete();
                return;
            }

            globalSyncId = node.nextTxnId(ExclusiveSyncPoint, Routable.Domain.Range);
            Invariants.checkArgument(epoch <= globalSyncId.epoch(), "Attempting to use local epoch %d which is larger than global epoch %d", epoch, globalSyncId.epoch());

            if (!node.topology().hasEpoch(globalSyncId.epoch()))
            {
                // Ignore timeouts fetching the epoch, always keep trying to bootstrap
                node.withEpoch(globalSyncId.epoch(), (ignored, failure) -> store.execute(empty(), Attempt.this::start).begin((ignored1, failure2) -> {
                    if (failure2 != null)
                        node.agent().acceptAndWrap(null, failure2);
                }));
                return;
            }

            // we fix here the ranges we use for the synthetic command, even though we may end up only finishing a subset
            // of these ranges as part of this attempt
            Ranges commitRanges = valid;
            safeStore = safeStore;
            // we submit a separate execution so that we know markBootstrapping is durable before we initiate the fetch
            safeStore.commandStore()
                     .submit(empty(), safeStore0 -> {
                         store.markBootstrapping(safeStore0, globalSyncId, commitRanges);
                         return CoordinateSyncPoint.exclusiveSyncPoint(node, globalSyncId, commitRanges);
                     })
                     .flatMap(i -> i)
                     .flatMap(syncPoint -> node.withEpoch(epoch, () -> store.submit(empty(), safeStore1 -> {
                         if (valid.isEmpty()) // we've lost ownership of the range
                             return AsyncResults.success(Ranges.EMPTY);
                         return fetch = safeStore1.dataStore().fetch(node, safeStore1, valid, syncPoint, this);
                     })))
                     .flatMap(i -> i)
                     .begin(this);
        }

        // we no longer want to fetch these ranges (perhaps we no longer own them)
        void invalidate(Ranges invalidate)
        {
            FetchResult abort; // the fetch we are coordinating, that may have not started, or may have completed
            Runnable cancel; // the outer future trying to coordinate us, that extends before and after the FetchFuture
            synchronized (this)
            {
                if (!valid.intersects(invalidate))
                    return;

                valid = valid.without(invalidate);
                abort = fetch;
                // only cancel the outer future if we have no more ranges to fetch
                cancel = valid.isEmpty() ? this.cancel : null;
                if (fetched.containsAll(valid))
                    maybeComplete();
            }
            // if we have started the fetch, ask it not to fetch these ranges
            if (abort != null)
                abort.abort(invalidate);
            if (cancel != null)
                cancel.run();
        }

        /**
         * our sync point is an inequality, i.e. we may have more data than we want and so before
         * we serve any *reads* we need to make sure we have applied any transaction that might have
         * been included in the data we bootstrapped. to this end we either rely on the implementation
         * to tell us what txnId it included up to, or else initiate a no-op transaction to
         * compute an executeAt from which we can safely begin serving read transactions whose
         * dependencies have all applied
         */
        @Override
        public synchronized StartingRangeFetch starting(Ranges ranges)
        {
            // mark all ranges unsafe to read
            // TODO (desired): if we have some ranges as safeToRead then we should really invalidate them immediately and not wait to execute under commandStore;
            //   could use synchronized to manage updates to these collections
            store.markUnsafeToRead(ranges);

            // find any pre-existing states we may overlap with, mark both as overlaps, and add ourselves to the collection
            UnsafeToRead newState = new UnsafeToRead(ranges);
            unsafeToReads.add(newState);

            return new StartingRangeFetch()
            {
                @Override
                public DataStore.AbortFetch started(Timestamp maxApplied)
                {
                    Attempt.this.started(newState, maxApplied);
                    return () -> abort(newState);
                }

                @Override
                public void cancel()
                {
                    Attempt.this.cancel(newState);
                }
            };
        }

        private void started(UnsafeToRead state, @Nonnull Timestamp maxAppliedLessOrEqualTo)
        {
            Invariants.checkState(maxAppliedLessOrEqualTo != null);
            synchronized (this)
            {
                if (!unsafeToReads.remove(state))
                    return;

                Timestamp safeToReadAt = maxAppliedLessOrEqualTo.compareTo(globalSyncId) <= 0 ? globalSyncId : maxAppliedLessOrEqualTo.next();
                safeToReadAts = ReducingRangeMap.add(safeToReadAts, state.ranges, safeToReadAt);
                maybeComplete(state.ranges);
            }
        }

        // starting cancelled, can just unlink and make sure we invoke onDone on any we may have interfered with
        private synchronized void cancel(UnsafeToRead state)
        {
            if (!unsafeToReads.remove(state))
                throw illegalState("Tried to cancel starting a fetch that had already started");

            maybeComplete(state.ranges);
        }

        // incomplete fetch cancelled, do we need to do anything?
        // it's fine to leave our no-op to complete, but there might be issues with failure states
        private synchronized void abort(UnsafeToRead state)
        {
            unsafeToReads.remove(state);
            // TODO (expected, consider): are there any edge cases here?
        }

        @Override
        public synchronized void fetched(Ranges ranges)
        {
            if (ranges.isEmpty())
                return;

            fetched = fetched.with(ranges);
            maybeComplete(ranges);
        }

        @Override
        public void fail(Ranges ranges, Throwable failure)
        {
            boolean hasFailed;
            Ranges newFailures;
            synchronized (this)
            {
                newFailures = ranges.slice(valid);
                if (newFailures.isEmpty())
                    return;

                valid = valid.without(newFailures);
                hasFailed = valid.isEmpty();
            }

            if (hasFailed)
                accept(null, failure);

            store.agent().onFailedBootstrap("PartialFetch", newFailures, () -> {
                store.execute(empty(), safeStore -> restart(safeStore, newFailures.slice(allValid))).begin(store.agent());
            }, failure);
            Invariants.checkState(!newFailures.intersects(fetchedAndSafeToRead));
        }

        /**
         * Completed successfully or abandoned, we simply process successfully bootstrapped ranges that intersect
         * with this safe-to-read boundary operation, and look to see if there remain newer operations in flight
         * for those ranges; if not, they're done.
         */
        private synchronized void maybeComplete(Ranges ranges)
        {
            Ranges newDone = fetched.slice(ranges.without(fetchedAndSafeToRead), Minimal);
            if (newDone.isEmpty())
                return;

            for (UnsafeToRead unsafeToRead : unsafeToReads)
                newDone = newDone.without(unsafeToRead.ranges);

            if (newDone.isEmpty())
                return;

            safeToReadAts.foldlWithInputAndBounds(newDone, (safeToReadAt, s, start, end, i, j) -> {
                Ranges mark = s.slice(Ranges.of(start.rangeFactory().newRange(start, end)), Minimal);
                store.markSafeToRead(globalSyncId, safeToReadAt, mark);
                return s;
            }, newDone, i -> false);
            fetchedAndSafeToRead = fetchedAndSafeToRead.with(newDone);
            maybeComplete();
        }

        @Override
        public void accept(Object success, Throwable failure)
        {
            if (completed)
                return;

            // TODO (now): we don't need this method, as we should have the user implementation invoke us as necessary
            //             at most this should interpret failure to account for non-fetch related breakages
            //             so as to schedule a retry
            synchronized (this)
            {
                fetchCompleted = true;
                if (failure != null)
                {
                    if (fetchOutcome == null) fetchOutcome = failure;
                    else fetchOutcome.addSuppressed(failure);
                }
            }
            maybeComplete();
        }

        void maybeComplete()
        {
            Ranges retry;
            synchronized (this)
            {
                if (completed)
                    return;

                if (!fetchedAndSafeToRead.containsAll(fetchCompleted ? fetched : valid))
                    return;

                // normalise fetched and fetchedAndSafeToRead against remaining valid ranges before completion
                fetched = fetched.slice(valid, Minimal);
                fetchedAndSafeToRead = fetchedAndSafeToRead.slice(valid, Minimal);
                retry = valid.without(fetchedAndSafeToRead);
                completed = true;
            }

            complete(this);
            if (!retry.isEmpty())
            {
                store.agent().onFailedBootstrap("Fetch", retry, () -> {
                    store.execute(empty(), safeStore -> restart(safeStore, retry)).begin(node.agent());
                }, fetchOutcome);
            }
        }
    }

    final Node node;
    final CommandStore store;
    final long epoch;
    // TODO (required): make sure this is triggered in event of partial expiration of work to do
    final AsyncResult.Settable<Void> data = AsyncResults.settable();
    final AsyncResult.Settable<Void> reads = AsyncResults.settable();
    final Set<Attempt> inProgress = new DeterministicIdentitySet<>();

    // TODO (expected): handle case where we clear these to empty; should trigger promise immediately
    Ranges allValid, remaining;

    public Bootstrap(Node node, CommandStore store, long epoch, Ranges ranges)
    {
        this.node = node;
        this.store = store;
        this.epoch = epoch;
        this.allValid = ranges;
        this.remaining = ranges;
    }

    void start(SafeCommandStore safeStore0)
    {
        restart(safeStore0, allValid);
    }

    private synchronized void restart(SafeCommandStore safeStore, Ranges ranges)
    {
        ranges = ranges.slice(allValid);
        if (ranges.isEmpty())
            return;

        for (Attempt attempt : inProgress)
            Invariants.checkArgument(!ranges.intersects(attempt.valid));

        Attempt attempt = new Attempt(ranges);
        inProgress.add(attempt);
        attempt.start(safeStore);
    }

    synchronized void complete(Attempt attempt)
    {
        Invariants.checkArgument(inProgress.contains(attempt));
        Invariants.checkArgument(attempt.fetched.equals(attempt.fetchedAndSafeToRead));
        inProgress.remove(attempt);
        remaining = remaining.without(attempt.fetched);
        if (inProgress.isEmpty() && remaining.isEmpty())
        {
            data.setSuccess(null);
            reads.setSuccess(null);
            store.complete(this);
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
