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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Timeouts.Timeout;
import accord.api.TopologyListener;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.ActiveEpoch;
import accord.topology.ActiveEpochs;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.local.durability.DurabilityService.SyncReadable.UnknownReadable;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class DurabilityService implements TopologyListener
{
    private static final Logger logger = LoggerFactory.getLogger(DurabilityService.class);

    public enum SyncLocal  { NoLocal, KnownToSelf, Self }
    public enum SyncRemote
    {
        NoRemote,
        MinorityQuorum, MinorityQuorumAndWaitedForAll,
        Quorum, QuorumAndWaitedForAll,
        All
    }
    public enum SyncReadable
    {
        UnknownReadable, KnownReadable;
        public static SyncReadable known(boolean known)
        {
            return known ? KnownReadable : UnknownReadable;
        }
    }

    private boolean started;
    private final Node node;
    private final ShardDurability shards;
    private final GlobalDurability global;

    private final Set<DurabilityRequest> requests = new LinkedHashSet<>();

    public DurabilityService(Node node)
    {
        this.node = node;
        this.shards = new ShardDurability(node);
        this.global = new GlobalDurability(node);
    }

    public DurabilityQueue queue()
    {
        return shards.queue();
    }

    public ShardDurability shards()
    {
        return shards;
    }

    public GlobalDurability global()
    {
        return global;
    }

    public synchronized boolean isStarted()
    {
        return started;
    }

    public void start()
    {
        synchronized (this)
        {
            Invariants.require(!started);
            started = true;
            node.topology().addListener(this);
        }
        Topology current = node.topology().current();
        shards.updateTopology(current);
        global.updateTopology(current);
        shards.start();
        global.start();
    }

    public void stop()
    {
        synchronized (this)
        {
            if (!started)
                return;

            Invariants.require(started);
            started = false;
            node.topology().removeListener(this);
        }
        shards.stop();
        global.stop();
    }

    public AsyncResult<DurabilityResults> close(String requestedBy, Txn.Kind kind, Ranges ranges, SyncLocal local, long timeoutDelay, TimeUnit timeoutUnits)
    {
        return close(requestedBy, kind, TxnId.NONE, ranges, local, timeoutDelay, timeoutUnits);
    }

    public AsyncResult<DurabilityResults> close(Object requestedBy, Txn.Kind kind, Timestamp minBound, Ranges ranges, SyncLocal local, long timeoutDelay, TimeUnit timeoutUnits)
    {
        long startedAt = node.elapsed(MICROSECONDS);
        long timeoutAt = startedAt + timeoutUnits.toMicros(timeoutDelay);
        return submit(new DurabilityRequest(requestedBy, kind, minBound, ranges, new DurabilityLevel(local, SyncRemote.NoRemote, UnknownReadable, null), startedAt, timeoutAt), true).result;
    }

    public AsyncResult<DurabilityResults> sync(Object requestedBy, Txn.Kind kind, Ranges ranges, SyncLocal local, SyncRemote remote, SyncReadable readable, long timeoutDelay, TimeUnit timeoutUnits)
    {
        return sync(requestedBy, kind, TxnId.NONE, ranges, local, remote, readable, timeoutDelay, timeoutUnits);
    }

    public AsyncResult<DurabilityResults> sync(Object requestedBy, Txn.Kind kind, Timestamp minBound, Ranges ranges, SyncLocal local, SyncRemote remote, SyncReadable readable, long timeoutDelay, TimeUnit timeoutUnits)
    {
        if (ranges.isEmpty())
            return AsyncResults.success(null);

        long startedAt = node.elapsed(MICROSECONDS);
        long timeoutAt = startedAt + timeoutUnits.toMicros(timeoutDelay);
        return submit(new DurabilityRequest(requestedBy, kind, minBound, ranges, new DurabilityLevel(local, remote, readable, null), startedAt, timeoutAt), true).result;
    }

    public AsyncResult<DurabilityResults> sync(Object requestedBy, Txn.Kind kind, Ranges ranges, @Nullable Collection<Node.Id> include, @Nullable Collection<Node.Id> ineligible, SyncLocal local, SyncRemote remote, SyncReadable readable, long timeoutDelay, TimeUnit timeoutUnits)
    {
        return sync(requestedBy, kind, TxnId.NONE, ranges, include, ineligible, local, remote, readable, timeoutDelay, timeoutUnits);
    }

    public AsyncResult<DurabilityResults> sync(Object requestedBy, Txn.Kind kind, Timestamp minBound, Ranges ranges, @Nullable Collection<Node.Id> include, @Nullable Collection<Node.Id> ineligible, SyncLocal local, SyncRemote remote, SyncReadable readable, long timeoutDelay, TimeUnit timeoutUnits)
    {
        if (ranges.isEmpty())
            return AsyncResults.success(null);

        long startedAt = node.elapsed(MICROSECONDS);
        long timeoutAt = startedAt + timeoutUnits.toMicros(timeoutDelay);
        return submit(new DurabilityRequest(requestedBy, kind, minBound, ranges, durabilityLevel(include, ineligible, local, remote, readable), startedAt, timeoutAt), true).result;
    }

    public AsyncResult<DurabilityResults> sync(Object requestedBy, SyncPoint syncPoint, @Nullable Collection<Node.Id> include, @Nullable Collection<Node.Id> ineligible, SyncLocal local, SyncRemote remote, SyncReadable readable, long timeoutDelay, TimeUnit timeoutUnits)
    {
        if (syncPoint.route.isEmpty())
            return AsyncResults.success(null);

        long startedAt = node.elapsed(MICROSECONDS);
        long timeoutAt = startedAt + timeoutUnits.toMicros(timeoutDelay);
        Ranges ranges = syncPoint.route.toRanges();
        return submit(new DurabilityRequest(requestedBy, null, syncPoint.syncId, ranges, durabilityLevel(include, ineligible, local, remote, readable), startedAt, timeoutAt), false).result;
    }

    private DurabilityLevel durabilityLevel(@Nullable Collection<Node.Id> include, @Nullable Collection<Node.Id> ineligible, SyncLocal local, SyncRemote remote, SyncReadable readable)
    {
        SortedArrayList<Node.Id> sortedInclude = include == null || include instanceof SortedArrayList<?>
                                                 ? (SortedArrayList<Node.Id>) include
                                                 : SortedArrayList.copyUnsorted(include, Node.Id[]::new);
        SortedArrayList<Node.Id> sortedIneligible = ineligible == null || ineligible instanceof SortedArrayList<?>
                                                    ? (SortedArrayList<Node.Id>) ineligible
                                                    : SortedArrayList.copyUnsorted(ineligible, Node.Id[]::new);
        return new DurabilityLevel(local, remote, readable, sortedInclude, null, sortedIneligible);
    }

    private DurabilityRequest submit(DurabilityRequest request, boolean submitToShardHandler)
    {
        request.report(node.durableBefore());
        if (request.isDone())
        {
            logger.info("Durability request {} satisfied before submission", request);
        }
        else
        {
            register(request);
            logger.info("Requesting durability {}", request);
            if (submitToShardHandler)
                shards.request(request);
        }
        return request;
    }

    void register(DurabilityRequest request)
    {
        request.timeout = node.timeouts().registerAt(new Timeout()
        {
            @Override public int stripe() { return request.ranges.hashCode(); }
            @Override public void timeout()
            {
                request.timeout();
                unregister(request);
            }
        }, request.timeoutAt, MICROSECONDS);

        synchronized (this)
        {
            if (!request.isDone()) // guard against unlikely scenario of timeout firing before we register here
                requests.add(request);
        }
    }

    void unregister(DurabilityRequest request)
    {
        synchronized (this)
        {
            requests.remove(request);
        }
    }

    public void report(DurabilityResult durability)
    {
        logger.debug("Reporting durability {}", durability);
        List<DurabilityRequest> notify = null;
        synchronized (this)
        {
            for (DurabilityRequest next : requests)
            {
                if (next.report(durability, node.elapsed(MICROSECONDS)))
                {
                    if (notify == null)
                        notify = new ArrayList<>();
                    notify.add(next);
                }
            }

            if (notify == null)
                return;

            for (DurabilityRequest next : notify)
            {
                logger.info("Completed durability {}.", next);
                requests.remove(next);
            }

            if (!requests.isEmpty())
                logger.debug("Still awaiting durability: {}.", requests);
        }

        for (DurabilityRequest next : notify)
            next.reportSuccess();
    }

    @Override
    public void onActive(ActiveEpoch epoch)
    {
        shards.updateTopology(epoch.all());
        global.updateTopology(epoch.all());
    }

    @Override
    public void onEpochRetired(Ranges retiredRanges, long epoch, @Nullable Topology topology)
    {
        // No need to cancel work for ranges that are still active
        ActiveEpochs epochs = node.topology().active();
        ActiveEpoch e = epochs.ifExists(epoch);
        if (e == null)
        {
            if (epoch > epochs.epoch())
                return;

            e = epochs.getKnown(epochs.minEpoch());
        }

        Ranges retiredAndRemoved = e.all().foldl(retiredRanges, (shard, rs, i) -> {
            if (shard.is(Shard.Flag.PENDING_REMOVAL))
                return rs.with(Ranges.of(shard.range));
            return rs;
        }, Ranges.EMPTY);
        // if the ranges are retired and have been removed in the epoch in which they're retired, then we can retire the associated scheduler(s)
        if (!retiredAndRemoved.isEmpty())
            shards.retireRanges(retiredAndRemoved, epoch);
    }
}