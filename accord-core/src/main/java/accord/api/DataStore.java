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

package accord.api;

import javax.annotation.Nullable;

import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncResult;

/**
 * A marker interface for a shard instance's storage, that is passed to
 * {@link Read} and {@link Write} objects for execution.
 *
 * This does not need to be isolated per-shard, it is fine to have a shared store
 * (so long as only one shard is responsible for reading/writing any given range
 * on a given epoch, and that on read the data store correctly filters newer writes
 * with a lower timestamp than older writes)
 */
public interface DataStore
{
    interface StartingRangeFetch
    {
        /**
         * To be invoked by implementation once we hear back from the replica that it
         * has taken a snapshot to process.
         *
         * Optional parameter maxApplied can supply the maximum timestamp of any applied
         * transaction in the data being bootstrapped from this source. If not provided
         * the store will coordinate a cheap global transaction to calculate a lower bound.
         *
         * Note: if the membership has entirely changed, or all other replicas are still themselves
         * bootstrapping, then this *must* be provided for the system to make progress.
         */
        AbortFetch started(@Nullable Timestamp maxApplied);

        /**
         * To be invoked by implementation once we decide to abort a fetch from a replica,
         * NO DATA must have been (or will be) fetched from this replica.
         */
        void cancel();
    }

    interface AbortFetch
    {
        /**
         * To be invoked by implementation once we decide to abort a fetch from a replica,
         * where data may have been (or will be) fetched from this replica.
         */
        void abort();
    }

    /**
     * A callback provided to the DataStore when fetching, so that it may inform the system when fetches
     * are started, completed or aborted. This permits the system to manage transactions around these points.
     */
    interface FetchRanges
    {
        /**
         * Invoke when we submit a request to a replica, and the Runnable should be invoked once we hear back
         * from the replica that it has taken a snapshot to process.
         *
         * We permit as many parallel fetches as an implementation desires for the same ranges, and only require
         * one to succeed. It is up to implementations to cancel/abort any in flight that are no longer needed.
         */
        StartingRangeFetch starting(Ranges ranges);

        /**
         * Mark some ranges as successfully bootstrapped. The same ranges can be marked multiple times, and
         * the ranges do not have to correspond to a precise set of ranges that were previously started, so
         * long as all ranges were previously started.
         */
        void fetched(Ranges ranges);

        /**
         * Mark some ranges as failed. A new attempt will be scheduled for these ranges by the node's {@link Agent}.
         */
        void fail(Ranges ranges, Throwable failure);
    }

    /**
     * A Future that represents the completion of a fetch by and to a DataStore. Offers an additional method
     * that may optionally be implemented to abort a portion of the fetch that may no longer be necessary.
     */
    interface FetchResult extends AsyncResult<Ranges>
    {
        /**
         * A set of ranges is no longer relevant; ask the implementation to stop fetching them if possible
         * (This method's behaviour is optional, and may be left a no-op)
         */
        void abort(Ranges ranges);
    }

    FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback);
    AsyncResult<Void> snapshot(Ranges ranges, TxnId before);
}
