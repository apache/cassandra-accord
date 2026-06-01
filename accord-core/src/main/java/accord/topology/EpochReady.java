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

package accord.topology;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import accord.utils.Invariants;
import accord.utils.Reduce;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.NestedAsyncResult;

public class EpochReady
{
    public static final AsyncResult<Void> DONE = AsyncResults.success(null);

    public final long epoch;

    /**
     * The new epoch has been setup locally and the node is ready to process commands for it.
     */
    public final AsyncResult<Void> active;

    /**
     * If necessary, relevant tasks are being refused by the participating CommandStore(s).
     * It is safe to receive messages, as any unsafe messages will not be processed.
     */
    public final AsyncResult<Void> refusing;

    /**
     * The CommandStore(s) are no longer refusing all requests, but may be refusing DEPS requests.
     */
    public final AsyncResult<Void> notRefusing;

    /**
     * The node has retrieved enough remote information to answer coordination decisions for the epoch
     * (including fast path decisions).
     * Once a quorum of the new epoch has achieved this, earlier epochs do not need to be contacted
     * by coordinators of transactions started in the new epoch (or later).
     */
    public final AsyncResult<Void> coordinate;

    /**
     * The node has successfully replicated the underlying DataStore information for the new epoch, but may need
     * to perform some additional coordination before it can execute the read portion of a transaction.
     */
    public final AsyncResult<Void> data;

    /**
     * The node has retrieved enough remote information to safely process reads, including replicating all
     * necessary DataStore information, and any additional transactions necessary for consistency.
     *
     * Note that {@link #coordinate} may need to also be waited on, depending on the consumer's semantics.
     */
    public final AsyncResult<Void> reads;

    public EpochReady(long epoch, AsyncResult<Void> active, AsyncResult<Void> coordinate, AsyncResult<Void> data, AsyncResult<Void> reads)
    {
        this(epoch, active, DONE, DONE, coordinate, data, reads);
    }

    public EpochReady(long epoch, AsyncResult<Void> active, AsyncResult<Void> refusing, AsyncResult<Void> notRefusing, AsyncResult<Void> coordinate, AsyncResult<Void> data, AsyncResult<Void> reads)
    {
        this.epoch = epoch;
        this.active = Invariants.nonNull(active);
        this.refusing = Invariants.nonNull(refusing);
        this.notRefusing = Invariants.nonNull(notRefusing);
        this.coordinate = Invariants.nonNull(coordinate);
        this.data = Invariants.nonNull(data);
        this.reads = Invariants.nonNull(reads);
    }

    public AsyncResult<Void> active()
    {
        return active;
    }

    public AsyncResult<Void> refusing()
    {
        return refusing;
    }

    public AsyncResult<Void> notRefusing()
    {
        return notRefusing;
    }

    public AsyncResult<Void> coordinate()
    {
        return coordinate;
    }

    public AsyncResult<Void> data()
    {
        return data;
    }

    public AsyncResult<Void> reads()
    {
        return reads;
    }

    public AsyncResult<Void> coordinateAndReads()
    {
        if (coordinate == reads)
            return reads;
        List<AsyncResult<Void>> results = ImmutableList.of(coordinate, reads);
        return AsyncResults.debuggableReduce(results, Reduce.toNull());
    }

    public static EpochReady done(long epoch)
    {
        return all(epoch, DONE);
    }

    public static EpochReady all(long epoch, AsyncResult<Void> done)
    {
        return new EpochReady(epoch, done, done, done, done, done, done);
    }

    @Override
    public String toString()
    {
        return "EpochReady{" +
               "epoch=" + epoch +
               ", active=<" + active +
               ">, refusals=<" + refusing +
               ">, accepting=<" + notRefusing +
               ">, coordinate=<" + coordinate +
               ">, data=<" + data +
               ">, reads=<" + reads +
               ">}";
    }

    /** combine one EpochReady per command store into one for the node: every phase waits for every store */
    public static EpochReady merge(long epoch, java.util.List<EpochReady> merge)
    {
        if (merge.isEmpty())
            return done(epoch);
        return new EpochReady(epoch,
                              AsyncResults.debuggableReduce(Lists.transform(merge, EpochReady::active), Reduce.toNull()),
                              AsyncResults.debuggableReduce(Lists.transform(merge, EpochReady::refusing), Reduce.toNull()),
                              AsyncResults.debuggableReduce(Lists.transform(merge, EpochReady::notRefusing), Reduce.toNull()),
                              AsyncResults.debuggableReduce(Lists.transform(merge, EpochReady::coordinate), Reduce.toNull()),
                              AsyncResults.debuggableReduce(Lists.transform(merge, EpochReady::data), Reduce.toNull()),
                              AsyncResults.debuggableReduce(Lists.transform(merge, EpochReady::reads), Reduce.toNull()));
    }

    public static EpochReady wrap(long epoch, AsyncResult<EpochReady> async)
    {
        return new EpochReady(epoch,
                              NestedAsyncResult.flatMap(async, e -> e.active),
                              NestedAsyncResult.flatMap(async, e -> e.refusing),
                              NestedAsyncResult.flatMap(async, e -> e.notRefusing),
                              NestedAsyncResult.flatMap(async, e -> e.coordinate),
                              NestedAsyncResult.flatMap(async, e -> e.data),
                              NestedAsyncResult.flatMap(async, e -> e.reads)
        );
    }
}
