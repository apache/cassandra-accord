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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Scheduler.Scheduled;
import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.CoordinationFailed;
import accord.local.Node;
import accord.local.ShardDistributor;
import accord.primitives.FullRoute;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.SyncPoint;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;
import org.agrona.BitUtil;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.SortedArrays.SortedArrayList.ofSorted;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Helper methods and classes to invoke coordination to propagate information about durability.
 *
 * ShardDurability uses wall time to loosely coordinate between nodes so that they take non-overlapping
 * (hopefully) turns doing the coordination.
 *
 * The current round is known based on time since the epoch, and the point in time where a node should do something
 * in a given round is known based on its index in the sorted list of nodes ids in the current epoch.
 *
 * ExecuteSyncPoint needs nodes to not overlap on the ranges they operate on or the exclusive sync points overlap
 * with each other and block progress. A target duration to process the entire ring is set, and then each node in the
 * current round has a time in the round that it should start processing, and the time it starts and the subranges it is
 * responsible for rotates backwards every round so that a down node doesn't prevent a subrange from being processed.
 *
 * The work for ExecuteSyncPoint is further subdivided where each subrange a node operates on is divided a fixed
 * number of times and then processed one at a time with a fixed wait between them.
 */
public class ShardDurability
{
    private static final Logger logger = LoggerFactory.getLogger(ShardDurability.class);

    public static class Waiting
    {
        final DurabilityRequest request;
        final Ranges ranges;
        Waiting next;

        Waiting(DurabilityRequest request, Ranges ranges)
        {
            this.request = request;
            this.ranges = ranges;
        }

        public Waiting next()
        {
            return next;
        }
    }

    // TODO (required): support intra-shard parallelism
    private class ShardScheduler
    {
        Shard shard;
        boolean ticks, stopping, stopped, readyToStop;

        int nodeOffset;
        /**
         * The position within the split list that we started this particular cycle from
         */
        int cycleOffset;
        /**
         * activeIndex: -1 if active is a user request; otherwise the active cycle index
         * nextIndex: the next cycle index to consume; if activeIndex >= 0 then activeIndex + 1
         * toIndex: the cycle index limit we are permitted to visit; this is bumped on a schedule for pacing
         */
        int activeIndex, nextIndex, nextToIndex, cycleLength;
        int currentSplits, maxSplits;

        long lastStartedAtMicros;
        long cycleStartedAtMicros;
        int retries;

        Cancellable scheduled;

        Ranges active;
        DurabilityRequest activeRequest;
        Waiting waiting;

        private ShardScheduler()
        {
        }

        synchronized void update(Shard newShard, int newNodeOffset, boolean newTicks)
        {
            Invariants.require(shard == null || shard.range.equals(newShard.range));
            shard = newShard;
            ShardDistributor shardDistributor = node.commandStores().shardDistributor();
            maxSplits = Math.min(maxNumberOfShardSplits, shardDistributor.numberOfSplitsPossible(newShard.range));
            int targetSplits = Math.min(targetShardSplits, maxSplits);
            if (currentSplits == 0 || currentSplits < targetSplits)
            {
                currentSplits = targetSplits;
            }
            if (cycleLength == 0 || cycleLength != targetSplits || newNodeOffset != nodeOffset || newTicks != ticks)
            {
                cycleLength = targetSplits;
                nodeOffset = newNodeOffset;
                ticks = newTicks;
                resetCycleIndexes();
                readyToStop = !ticks;
            }
        }

        synchronized void resetCycleIndexes()
        {
            long nowMicros = node.elapsed(MICROSECONDS);
            long cycleStart = nowMicros - (nowMicros % shardCycleTimeMicros);
            int globalCycleIndex = Math.toIntExact((nowMicros - cycleStart) / (shardCycleTimeMicros / cycleLength));
            cycleOffset = (globalCycleIndex + nodeOffset) % cycleLength;
            activeIndex = nextIndex = -1;
            nextToIndex = ticks ? 1 : -1;
        }

        void markDefunct()
        {
            logger.info("Marking shard durability scheduler for {} defunct", shard);
        }

        synchronized void markDefunctSilent()
        {
            stopping = true;
        }

        synchronized void tick()
        {
            if (!ticks)
                return;

            if (nextToIndex < cycleLength)
                ++nextToIndex;
            updateActive();
        }

        synchronized void updateActive()
        {
            if (active != null)
                return;

            if (waiting != null && (activeIndex >= 0 || nextIndex == nextToIndex))
            {
                activeIndex = -1;
                activeRequest = waiting.request;
                active = waiting.ranges;
                waiting = waiting.next;
            }
            else if (nextIndex < nextToIndex && nextToIndex > 0)
            {
                activeIndex = Math.max(0, nextIndex);
                nextIndex = activeIndex + 1;
                int pos = (activeIndex + cycleOffset) % cycleLength;
                Range range = node.commandStores().shardDistributor().splitRange(shard.range, pos, pos + 1, cycleLength);
                active = Ranges.single(range);
            }
            else if (nextIndex == cycleLength || !ticks)
            {
                if (stopping)
                {
                    if (!readyToStop)
                    {
                        readyToStop = true;
                    }
                    else
                    {
                        stopped = true;
                        node.scheduler().once(() -> {
                            synchronized (ShardDurability.this)
                            {
                                shardSchedulers.remove(shard.range, this);
                            }
                        }, 0, MICROSECONDS);
                    }
                    return;
                }
                resetCycleIndexes();
            }
            if (active != null)
                start();
        }

        synchronized void success(SyncPoint<Range> success, Ranges ranges)
        {
            Object requestedBy = null;
            if (activeRequest != null)
                requestedBy = activeRequest.requestedBy;
            durabilityQueue.submit(success, activeRequest);
            decrementBackoff();
            int index = activeIndex;
            active = active.without(ranges);
            if (!active.isEmpty())
            {
                start();
            }
            else
            {
                if (index >= 0) logCycleProgress(index);
                else logger.info("Successfully agreed RX requested by {} for {} with {}. Remaining ranges: {}.", requestedBy, ranges, success.syncId, active);
                active = null;
                activeRequest = null;
                updateActive();
            }
        }

        private void logCycleProgress(int index)
        {
            if (++index == cycleLength)
            {
                long nowMicros = node.elapsed(MICROSECONDS);
                long timeTakenSeconds = MICROSECONDS.toSeconds(nowMicros - cycleStartedAtMicros);
                long targetTimeSeconds = MICROSECONDS.toSeconds(shardCycleTimeMicros);
                logger.info("Successfully completed one cycle of durability scheduling for shard {} in {}s (vs {}s target)", shard.range, timeTakenSeconds, targetTimeSeconds);
            }
            else
            {
                long nowMicros = node.elapsed(MICROSECONDS);
                int rfCycleSize = (cycleLength + shard.rf - 1) / shard.rf;
                if (index % rfCycleSize == 0)
                {
                    long targetTimeSeconds = MICROSECONDS.toSeconds((index * shardCycleTimeMicros) / cycleLength);
                    long timeTakenSeconds = MICROSECONDS.toSeconds(nowMicros - cycleStartedAtMicros);
                    Range range = node.commandStores().shardDistributor().splitRange(shard.range, index - rfCycleSize, index, cycleLength);
                    logger.info("Successfully completed {}/{} cycle of durability scheduling covering range {}. Completed in {}s (vs {}s target).", index/rfCycleSize, shard.rf(), range, timeTakenSeconds, targetTimeSeconds);
                }
            }
        }

        private void decrementBackoff()
        {
            if (retries > 0)
                --retries;
            else if (currentSplits > maxSplits)
                currentSplits = Math.max(maxSplits, (int)(currentSplits * 0.9));
        }

        synchronized void retry(Ranges ranges)
        {
            if (stopped)
                return;

            ++retries;
            if (currentSplits < maxSplits)
            {
                currentSplits = Math.min(currentSplits * 2, maxSplits);
                logger.info("Increased numberOfSplits to {} for shard {}", currentSplits, shard.range);
            }
            long retryDelay = node.agent().retrySyncPointDelay(node, retries, MICROSECONDS);
            if (activeRequest != null)
                logger.info("Retrying {} for {} in {}s", ranges, activeRequest.requestedBy, String.format("%.2f", retryDelay/1000_000.0));
            scheduled = node.scheduler().selfRecurring(() -> {
                synchronized (this)
                {
                    scheduled = null;
                    start();
                }
            }, retryDelay, MICROSECONDS);
        }

        synchronized void start()
        {
            Invariants.require(scheduled == null);
            Invariants.require(active != null);
            ShardDistributor distributor = node.commandStores().shardDistributor();
            Ranges ranges = distributor.selectFirstSubRanges(shard.range, active, currentSplits);

            lastStartedAtMicros = node.elapsed(MICROSECONDS);
            long minEpoch = 0;
            long minHlc = 0;
            if (activeRequest != null && activeRequest.min != null)
            {
                minEpoch = activeRequest.min.epoch();
                minHlc = activeRequest.min.hlc();
            }
            minHlc = Math.max(minHlc, node.agent().minStaleHlc(node, activeRequest != null));
            TxnId staleId = node.nextStaleTxnId(minEpoch, minHlc, ExclusiveSyncPoint, Domain.Range);
            if (activeRequest != null)
                logger.info("Initiating RX requested by {} for {} with TxnId {}. Remaining: {}.", activeRequest.requestedBy, ranges, staleId, active);

            scheduled = node.agent().awaitStaleId(node, staleId, activeIndex >= 0)
                            .flatMap(
                                syncId -> node.withEpoch(syncId.epoch(),
                                    () -> syncPointControl.submit(
                                        () -> CoordinateSyncPoint.exclusive(node, syncId, (FullRoute<Range>) node.computeRoute(syncId, ranges))
                                                                 .addCallback(logSyncPoint(syncId, ranges))
                            )))
                            .begin((success, fail) -> {
                                scheduled = null;
                                if (fail != null) retry(ranges);
                                else success(success, ranges);
                            });
        }

        private BiConsumer<Object, Throwable> logSyncPoint(TxnId syncId, Ranges ranges)
        {
            return (success, fail) -> {
                if (fail != null && (!(fail instanceof CoordinationFailed)) && activeRequest != null)
                    logger.warn("{}: Failed to agree RX requested by {} for {}.", syncId, activeRequest.requestedBy, ranges, fail);
                if (fail != null && activeRequest != null)
                    logger.warn("{}: Failed to agree RX requested by {} for {}: {}.", syncId, activeRequest.requestedBy, ranges, fail.getMessage());
                else if (fail != null && (!(fail instanceof CoordinationFailed)))
                    logger.warn("{}: Failed to agree RX for {}.", syncId, ranges, fail);
                else if (fail != null)
                    logger.warn("{}: Failed to agree RX for {}: {}.", syncId, ranges, fail.getMessage());
                else if (activeRequest != null)
                    logger.info("{}: Successfully agreed RX requested by {} for {}.", syncId, activeRequest.requestedBy, ranges);
                else
                    logger.debug("{}: Successfully agreed RX for {}.", syncId, ranges);
            };
        }

        synchronized boolean request(DurabilityRequest request, Range range)
        {
            if (stopped)
                return false;

            Invariants.require(this.shard.range.contains(range));
            if (waiting == null)
            {
                waiting = new Waiting(request, Ranges.of(range));
            }
            else
            {
                Waiting tail = waiting;
                while (tail.next != null)
                    tail = tail.next;
                tail.next = new Waiting(request, Ranges.of(range));
            }
            updateActive();
            return true;
        }
    }

    private final Node node;

    /*
     * In each cycle, attempt to split the range into this many pieces; if we fail, we increase the number of pieces
     */
    private int targetShardSplits = 64;
    private int maxNumberOfShardSplits = 1 << 10;

    /*
     * Target for how often the entire ring should be processed in microseconds. Every node will start at an offset in the current round that is based
     * on this value / by (total # replicas * its index in the current round). The current round is determined by dividing time since the epoch by this
     * duration.
     */
    private long shardCycleTimeMicros = TimeUnit.SECONDS.toMicros(30);

    private final NavigableMap<Range, ShardScheduler> shardSchedulers = new TreeMap<>(Range::compare);
    private final ConcurrencyControl syncPointControl = new ConcurrencyControl(8);
    private final DurabilityQueue durabilityQueue;
    private long latestEpoch;

    volatile boolean stop;
    volatile Scheduled scheduled;

    public ShardDurability(Node node)
    {
        this.node = node;
        this.durabilityQueue = new DurabilityQueue(node);
    }

    public void setMaxShardSplits(int maxNumberOfShardSplits)
    {
        this.maxNumberOfShardSplits = maxNumberOfShardSplits;
    }

    public synchronized void setTargetShardSplits(int targetShardSplits)
    {
        this.targetShardSplits = BitUtil.findNextPositivePowerOfTwo(targetShardSplits);
        if (scheduled != null)
            scheduled.cancel();
        scheduled = node.scheduler().recurring(this::tick, shardCycleTimeMicros / targetShardSplits, MICROSECONDS);
    }

    public synchronized void setShardCycleTime(long newShardCycleTime, TimeUnit units)
    {
        shardCycleTimeMicros = units.toMicros(newShardCycleTime);
        if (scheduled != null)
            scheduled.cancel();
        scheduled = node.scheduler().recurring(this::tick, shardCycleTimeMicros / targetShardSplits, MICROSECONDS);
    }

    /**
     * Schedule regular invocations of CoordinateShardDurable and CoordinateGloballyDurable
     */
    public synchronized void start()
    {
        Invariants.require(!stop); // cannot currently restart safely
        scheduled = node.scheduler().recurring(this::tick, shardCycleTimeMicros / targetShardSplits, MICROSECONDS);
    }

    private synchronized void tick()
    {
        for (ShardScheduler scheduler : shardSchedulers.values())
            scheduler.tick();
    }

    public synchronized void stop()
    {
        stop = true;
        for (ShardScheduler scheduler : shardSchedulers.values())
            scheduler.markDefunct();
        shardSchedulers.clear();
    }

    public synchronized void request(DurabilityRequest request)
    {
        request(request, request.ranges);
    }

    synchronized void request(DurabilityRequest request, Ranges ranges)
    {
        for (Range range : ranges)
            request(request, range);
    }

    private void request(DurabilityRequest request, Range range)
    {
        {
            Map.Entry<Range, ShardScheduler> e = shardSchedulers.floorEntry(range);
            if (e != null)
            {
                Range shardRange = e.getKey();
                ShardScheduler shardScheduler = e.getValue();
                if (shardRange.compareIntersecting(range) == 0)
                {
                    Range r = shardRange.contains(range) ? range : range.slice(e.getKey());
                    if (shardScheduler.request(request, r))
                    {
                        if (range.start().compareTo(r.start()) < 0)
                            request(request, range.newRange(range.start(), r.start()));
                        if (range.end().compareTo(r.end()) > 0)
                            request(request, range.newRange(r.end(), range.end()));
                        return;
                    }
                }
            }
        }

        while (true)
        {
            // TODO (desired): seed the numberOfSplits based on our permanent shardScheduler average?
            ShardScheduler shardScheduler = new ShardScheduler();
            Topology topology = node.topology().current();
            int i = topology.indexForKey(range.start());
            if (i >= 0 && !range.startInclusive() && range.start().equals(topology.get(i).range.end())) ++i;
            Invariants.require(i < 0 || topology.get(i).range.compareIntersecting(range) == 0);
            Shard shard = i >= 0 ? topology.get(i) : Shard.create(range, ofSorted(node.id()), ofSorted(node.id()));
            shardScheduler.update(shard, 0, false);
            shardScheduler.request(request, range.intersection(shard.range));
            shardScheduler.markDefunctSilent();
            shardSchedulers.put(shard.range, shardScheduler);
            if (shard.range.contains(range))
                break;
            range = range.newRange(shard.range.end(), range.end());
        }
    }

    synchronized void retireRanges(Ranges retiredRanges, long epoch)
    {
        Map<Range, ShardScheduler> prev = new HashMap<>(this.shardSchedulers);
        this.shardSchedulers.clear();

        for (Map.Entry<Range, ShardScheduler> e : prev.entrySet())
        {
            if (retiredRanges.contains(e.getKey()))
            {
                logger.info("Cancelling durability scheduling for {}, since it was retired in epoch {}",
                            e.getKey(), epoch);
                e.getValue().markDefunct();
            }
            else
                this.shardSchedulers.put(e.getKey(), e.getValue());
        }
    }

    synchronized void updateTopology(Topology latestGlobal)
    {
        if (latestGlobal.epoch() <= latestEpoch)
            return;

        Topology latestLocal = latestGlobal.forNode(node.id());
        Map<Range, ShardScheduler> prev = new HashMap<>(this.shardSchedulers);
        this.shardSchedulers.clear();
        for (Shard shard : latestLocal.shards())
        {
            ShardScheduler scheduler = prev.remove(shard.range);
            if (scheduler == null)
                scheduler = new ShardScheduler();
            shardSchedulers.put(shard.range, scheduler);
            scheduler.update(shard, shard.nodes.find(node.id()), true);
        }
        shardSchedulers.putAll(prev);
        prev.forEach((r, s) -> s.markDefunct());
        latestEpoch = latestGlobal.epoch();
    }

    public synchronized ImmutableView immutableView()
    {
        TreeMap<Range, ShardScheduler> schedulers = new TreeMap<>(Range::compare);
        schedulers.putAll(shardSchedulers);
        return new ImmutableView(schedulers);
    }

    public static class ImmutableView
    {
        private final TreeMap<Range, ShardScheduler> schedulers;
        ImmutableView(TreeMap<Range, ShardScheduler> schedulers)
        {
            this.schedulers = schedulers;
        }

        private Iterator<Map.Entry<Range, ShardScheduler>> iterator = null;

        Shard shard;
        boolean stopping, stopped;

        int nodeOffset;
        /**
         * The position within the split list that we started this particular cycle from
         */
        int cycleOffset;
        int activeIndex, nextIndex, nextToIndex, cycleLength;
        int currentSplits;

        long lastStartedAtMicros;
        long cycleStartedAtMicros;
        int retries;

        Ranges active;
        Waiting waiting;

        public boolean advance()
        {
            if (iterator == null)
                iterator = schedulers.entrySet().iterator();

            if (!iterator.hasNext())
            {
                shard = null;
                active = null;
                waiting = null;
                return false;
            }

            Map.Entry<Range, ShardScheduler> next = iterator.next();
            ShardScheduler scheduler = next.getValue();
            synchronized (scheduler)
            {
                this.shard = scheduler.shard;
                this.nodeOffset = scheduler.nodeOffset;
                this.cycleOffset = scheduler.cycleOffset;
                this.activeIndex = scheduler.activeIndex;
                this.nextIndex = scheduler.nextIndex;
                this.nextToIndex = scheduler.nextToIndex;
                this.cycleLength = scheduler.cycleLength;
                this.currentSplits = scheduler.currentSplits;
                this.lastStartedAtMicros = scheduler.lastStartedAtMicros;
                this.cycleStartedAtMicros = scheduler.cycleStartedAtMicros;
                this.retries = scheduler.retries;
                this.active = scheduler.active;
                this.waiting = scheduler.waiting;
                this.stopping = scheduler.stopping;
                this.stopped = scheduler.stopped;
            }
            return true;
        }

        public Shard shard()
        {
            return shard;
        }

        public boolean stopping()
        {
            return stopping;
        }

        public boolean stopped()
        {
            return stopped;
        }

        public int nodeOffset()
        {
            return nodeOffset;
        }

        public int cycleOffset()
        {
            return cycleOffset;
        }

        public int activeIndex()
        {
            return activeIndex;
        }

        public int nextIndex()
        {
            return nextIndex;
        }

        public int toIndex()
        {
            return nextToIndex;
        }

        public int cycleLength()
        {
            return cycleLength;
        }

        public int currentSplits()
        {
            return currentSplits;
        }

        public long lastStartedAtMicros()
        {
            return lastStartedAtMicros;
        }

        public long cycleStartedAtMicros()
        {
            return cycleStartedAtMicros;
        }

        public int retries()
        {
            return retries;
        }

        public Ranges active()
        {
            return active;
        }

        public Waiting waiting()
        {
            return waiting;
        }
    }
}