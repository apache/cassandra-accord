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

package accord.local.durability;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Timeouts;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.primitives.AbstractRanges;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncResults;

import static accord.local.durability.DurabilityService.SyncLocal.NoLocal;
import static accord.local.durability.DurabilityService.SyncReadable.UnknownReadable;
import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Txn.Kind.VisibilitySyncPoint;

public class DurabilityRequest
{
    private static final Logger logger = LoggerFactory.getLogger(DurabilityRequest.class);

    static class DurableEvents
    {
        private final long requestedAt;
        private long lastAttemptAt;
        private long durableAt;
        private int attempts;

        DurableEvents(long requestedAt)
        {
            this.requestedAt = requestedAt;
        }

        public long requestedAt() { return requestedAt; }
        public int attempts() { return attempts; }
        public long durableAt() { return durableAt; }
        public long lastAttemptAt() { return lastAttemptAt; }
    }

    final AsyncResults.SettableResult<DurabilityResults> result = new AsyncResults.SettableResult<>();
    final Object requestedBy;
    final Txn.Kind kind;
    final Timestamp min;
    final Ranges ranges;
    final DurabilityLevel require;
    final long startedAt, timeoutAt;
    Timeouts.RegisteredTimeout timeout;

    private Ranges agreed = Ranges.EMPTY;
    private Ranges achieved = Ranges.EMPTY;
    private DurabilityResults success = DurabilityResults.EMPTY;

    private LinkedHashMap<TxnId, DurableEvents> events;

    DurabilityRequest(Object requestedBy, Txn.Kind kind, Timestamp min, Ranges ranges, DurabilityLevel require, long startedAt, long timeoutAt)
    {
        this.requestedBy = requestedBy;
        this.kind = kind;
        this.min = min == null ? TxnId.NONE : min;
        this.ranges = ranges;
        this.require = require;
        this.startedAt = startedAt;
        this.timeoutAt = timeoutAt;
        Invariants.require(require.excluding == null);
    }

    public synchronized void reportAttempt(TxnId txnId, long now)
    {
        DurableEvents e = events.get(txnId);
        e.lastAttemptAt = now;
        e.attempts++;
    }

    synchronized void register(TxnId txnId, long now)
    {
        if (events == null)
            events = new LinkedHashMap<>();
        events.put(txnId, new DurableEvents(now));
    }

    private DurableEvents ensure(TxnId txnId)
    {
        if (events == null)
            events = new LinkedHashMap<>();
        return events.computeIfAbsent(txnId, k -> new DurableEvents(Long.MIN_VALUE));
    }

    synchronized DurableEvents get(TxnId txnId)
    {
        if (events == null)
            return null;
        return events.get(txnId);
    }

    boolean isDone()
    {
        return result.isDone();
    }

    synchronized Ranges achieved()
    {
        return achieved;
    }

    synchronized boolean isDone(AbstractRanges ranges)
    {
        if (isDone())
            return true;
        return achieved.containsAll(ranges);
    }

    void timeout()
    {
        if (result.tryFailure(new TimeoutException()))
            logger.info("Durability request timeout {}", this);
    }

    void report(DurableBefore durableBefore)
    {
        boolean canUseDurableBefore = kind != Txn.Kind.VisibilitySyncPoint
                                      && !min.equals(TxnId.NONE)
                                      && require.including == null // can't construct participants from DurableBefore
                                      && require.readable == UnknownReadable // can't construct readability from DurableBefore
                                      && require.local == NoLocal;

        if (canUseDurableBefore)
        {
            boolean useUniversal;
            switch (require.remote)
            {
                default: throw new UnhandledEnum(require.remote);
                case All:
                case QuorumAndWaitedForAll:
                case MinorityQuorumAndWaitedForAll:
                    useUniversal = true;
                    break;
                case Quorum:
                case MinorityQuorum:
                case NoRemote:
                    useUniversal = false;
            }

            durableBefore.foldl(ranges, (e, b, min, universal) -> {
                TxnId bound = universal ? e.universal : e.quorum;
                if (min.compareTo(bound) <= 0)
                {
                    DurabilityLevel level = new DurabilityLevel(NoLocal, universal ? SyncRemote.All : SyncRemote.Quorum, UnknownReadable, null);
                    DurabilityResult result = new DurabilityResult(bound, Ranges.of(e.toPlainRange()), level, SortedArrayList.empty(), null, null);
                    if (report(result, 0))
                        reportSuccess();
                }
                return null;
            }, null, min, useUniversal);
        }
    }

    void reportSuccess()
    {
        Invariants.require(success.ranges().containsAll(ranges));
        Invariants.require(achieved.containsAll(ranges));
        result.trySuccess(success);
        Timeouts.RegisteredTimeout cancel = timeout;
        if (cancel != null) cancel.cancel();
    }

    synchronized boolean report(DurabilityResult durability, long finishedAt)
    {
        TxnId syncId = durability.syncId;
        Ranges successRanges = durability.ranges;
        if (kind == VisibilitySyncPoint && !syncId.is(VisibilitySyncPoint))
            return false;

        Ranges intersecting = ranges.slice(successRanges, Minimal);
        if (intersecting.isEmpty())
            return false;

        DurableEvents e = get(syncId);
        if (min.compareTo(syncId) > 0)
        {
            if (e != null) logger.error("{}: too early to satisfy {}, but the request was submitted on its behalf.", syncId, this);
            else if (logger.isDebugEnabled()) logger.debug("{}: too early to satisfy {}", syncId, this);
            return false;
        }

        agreed = agreed.union(MERGE_ADJACENT, intersecting);
        Ranges waitingOn = ranges.without(achieved);

        Ranges expect = waitingOn.slice(intersecting, Minimal);
        Ranges satisfies = expect.slice(durability.satisfies(require), Minimal);
        Ranges success = satisfies.slice(waitingOn, Minimal);
        Ranges failed = expect.without(satisfies);

        if (!failed.isEmpty())
            logFailure(success, failed, e, durability);

        Ranges newAchieved = this.achieved.union(MERGE_ADJACENT, success);
        if (this.achieved != newAchieved && e == null)
            e = ensure(syncId);

        if (e != null && e.durableAt == 0 && finishedAt > 0)
            e.durableAt = finishedAt;

        if (newAchieved == this.achieved)
            return false;

        this.achieved = newAchieved;
        this.success = this.success.merge(DurabilityResults.of(newAchieved, syncId, durability.readable));
        if (e != null) logger.info("{}: Successfully achieved durability for {} requested by {}. Remaining: {}.", syncId, ranges, this, ranges.without(this.achieved));
        else if (logger.isDebugEnabled()) logger.debug("{}: partially satisfies {}. Remaining: {}.", syncId, this, ranges.without(this.achieved));
        return this.achieved.containsAll(ranges);
    }

    private void logFailure(Ranges success, Ranges failed, DurableEvents e, DurabilityResult durability)
    {
        if (require.local.compareTo(durability.min.local) > 0 || require.remote.compareTo(durability.min.remote) > 0)
        {
            if (e != null || logger.isDebugEnabled())
            {
                String successString = success.isEmpty() ? "achieved" : String.format("achieved %s/%s for %s but only", require.local, require.remote, success);
                String message = String.format("%s: %s %s/%s for %s; insufficient to satisfy %s/%s requested by %s", durability.syncId, successString, durability.min.local, durability.min.remote, failed, require.local, require.remote, this);
                if (e != null) logger.info(message);
                else logger.debug(message);
            }
            return;
        }

        if (require.including != null && (durability.min.including == null || !durability.min.including.containsAll(require.including)))
        {
            if (e != null || logger.isDebugEnabled())
            {
                String message = String.format("%s: missing nodes %s for ranges %s requested by %s", durability.syncId, missingIds(require.including, durability.min.including), failed, this);
                if (e != null) logger.info(message);
                else logger.debug(message);
            }
        }
    }

    @Override
    public String toString()
    {
        return "[" + requestedBy + " requires >= " + min + " for "
               + ranges + " with local:" + require.local + " and remote:" + require.remote
               + (require.including == null ? "" : " including:" + require.including) + ']';
    }

    private static String missingIds(Collection<Node.Id> require, @Nullable Collection<Node.Id> actual)
    {
        if (actual == null)
            return require.toString();

        StringBuilder sb = new StringBuilder("[");
        for (Node.Id id : require)
        {
            if (!actual.contains(id))
            {
                sb.append(id);
                sb.append(',');
            }
        }
        sb.setCharAt(sb.length() - 1, ']');
        return sb.toString();
    }

    synchronized Ranges stillWaiting(Route<Range> intersecting)
    {
        return ranges.without(achieved).intersecting(intersecting, Minimal);
    }
}
