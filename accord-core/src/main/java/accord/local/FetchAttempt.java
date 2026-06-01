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
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;

import accord.api.DataStore;
import accord.api.DataStore.FetchResult;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.utils.DeterministicIdentitySet;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.illegalState;

abstract class FetchAttempt implements DataStore.FetchRanges, BiConsumer<Object, Throwable>
{
    static class UnsafeToRead
    {
        final Ranges ranges;

        UnsafeToRead(Ranges ranges)
        {
            this.ranges = ranges;
        }
    }

    final DeterministicIdentitySet<UnsafeToRead> unsafeToReads = new DeterministicIdentitySet<>();
    ReducingRangeMap<Timestamp> safeToReadAts;

    Runnable cancel;
    FetchResult currentFetch;

    /**
     * valid: the ranges we are still meant to fetch - i.e. excluding those that have been invalidated or marked failed
     */
    Ranges valid, fetched = Ranges.EMPTY, fetchedAndSafeToRead = Ranges.EMPTY;
    boolean fetchCompleted;
    boolean completed; // we have finished fetching all the data we are able to, but we may still have in-flight fetches
    Throwable fetchOutcome;
    final int attempt;

    FetchAttempt(Ranges ranges, int attempt)
    {
        this.valid = ranges;
        this.attempt = attempt;
    }

    // we no longer want to fetch these ranges (perhaps we no longer own them)
    protected void invalidate(Ranges invalidate)
    {
        FetchResult abort; // the fetch we are coordinating, that may have not started, or may have completed
        Runnable cancel; // the outer future trying to coordinate us, that extends before and after the FetchFuture
        synchronized (this)
        {
            if (!valid.intersects(invalidate))
                return;

            valid = valid.without(invalidate);
            abort = currentFetch;
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
    public synchronized DataStore.StartingRangeFetch starting(Ranges ranges)
    {
        if (!valid.containsAll(ranges))
            throw new IllegalArgumentException();

        // mark all ranges unsafe to read
        // TODO (desired): if we have some ranges as safeToRead then we should really invalidate them immediately and not wait to execute under commandStore;
        //   could use synchronized to manage updates to these collections
        markUnsafeToRead(ranges);

        // find any pre-existing states we may overlap with, mark both as overlaps, and add ourselves to the collection
        UnsafeToRead newState = new UnsafeToRead(ranges);
        unsafeToReads.add(newState);

        return new DataStore.StartingRangeFetch()
        {
            @Override
            public DataStore.AbortFetch started(Timestamp maxApplied)
            {
                FetchAttempt.this.started(newState, maxApplied);
                return () -> abort(newState);
            }

            @Override
            public void cancel()
            {
                FetchAttempt.this.cancel(newState);
            }
        };
    }

    private void started(UnsafeToRead state, @Nonnull Timestamp maxAppliedLessOrEqualTo)
    {
        Invariants.require(maxAppliedLessOrEqualTo != null);
        synchronized (this)
        {
            if (!unsafeToReads.remove(state))
                return;

            Timestamp safeToReadAt = maxAppliedLessOrEqualTo.next();
            safeToReadAts = ReducingRangeMap.add(safeToReadAts, state.ranges, safeToReadAt, Timestamp::mergeMax);
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
        Ranges newlyFailed;
        synchronized (this)
        {
            newlyFailed = ranges.slice(valid, Minimal);
            if (newlyFailed.isEmpty())
                return;

            valid = valid.without(newlyFailed);
            hasFailed = valid.isEmpty();
        }

        if (hasFailed)
            accept(null, failure);

        onNewFailure(failure, newlyFailed);
    }

    protected void onNewFailure(Throwable failure, Ranges newlyFailed)
    {
    }

    /**
     * Completed successfully or abandoned, we simply process successfully bootstrapped ranges that intersect
     * with this safe-to-read boundary operation, and look to see if there remain newer operations in flight
     * for those ranges; if not, they're done.
     */
    protected synchronized void maybeComplete(Ranges ranges)
    {
        Ranges newDone = fetched.slice(ranges.without(fetchedAndSafeToRead), Minimal);
        if (newDone.isEmpty())
            return;

        for (UnsafeToRead unsafeToRead : unsafeToReads)
            newDone = newDone.without(unsafeToRead.ranges);

        if (newDone.isEmpty())
            return;

        final Ranges safeToRead = newDone;
        List<AsyncResult<Void>> marking = new ArrayList<>();
        safeToReadAts.foldlWithInputAndBounds(newDone, (safeToReadAt, v, start, end, i, j) -> {
            Ranges mark = safeToRead.slice(Ranges.of(start.rangeFactory().newRange(start, end)), Minimal);
            marking.add(markSafeToRead(mark, safeToReadAt));
            return v;
        }, null);

        AsyncResults.allOf(marking).invoke((success, fail) -> {
            if (fail != null) fail(ranges, fail);
            else
            {
                synchronized (this)
                {
                    fetchedAndSafeToRead = fetchedAndSafeToRead.with(safeToRead.slice(valid, Minimal));
                    maybeComplete();
                }
            }
        });
    }

    @Override
    public void accept(Object success, Throwable failure)
    {
        if (completed)
            return;

        // TODO (desired): we shouldn't need this method, as we should have the user implementation invoke us as necessary
        //      at most this should interpret failure to account for non-fetch related breakages
        //      so as to schedule a retry
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
        Ranges missing;
        synchronized (this)
        {
            if (completed)
                return;

            if (!fetchedAndSafeToRead.containsAll(fetchCompleted ? fetched : valid))
                return;

            // normalise fetched and fetchedAndSafeToRead against remaining valid ranges before completion
            fetched = fetched.slice(valid, Minimal);
            fetchedAndSafeToRead = fetchedAndSafeToRead.slice(valid, Minimal);
            missing = valid.without(fetchedAndSafeToRead);
            completed = true;
        }

        complete(missing);
    }

    protected abstract AsyncResult<Void> markSafeToRead(Ranges ranges, Timestamp safeToReadAt);
    protected abstract void markUnsafeToRead(Ranges ranges);
    protected abstract void complete(Ranges missing);
}
