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

package accord.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ConfigurationService;
import accord.api.Scheduler;
import accord.coordinate.CoordinateGloballyDurable;
import accord.coordinate.CoordinationFailed;
import accord.coordinate.ExecuteSyncPoint.SyncPointErased;
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
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.agrona.BitUtil;

import static accord.coordinate.CoordinateShardDurable.coordinate;
import static accord.coordinate.CoordinateSyncPoint.exclusiveSyncPoint;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Helper methods and classes to invoke coordination to propagate information about durability.
 *
 * Both CoordinateShardDurable and CoordinateGloballyDurable use wall time to loosely coordinate between nodes
 * so that they take non-overlapping (hopefully) turns doing the coordination.
 *
 * Both of these have a concept of rounds where rounds have a known duration in wall time, and the current round is known
 * based on time since the epoch, and the point in time where a node should do something in a given round is known based
 * on its index in the sorted list of nodes ids in the current epoch.
 *
 * Coordinate globally durable is simpler because they just need to take turns so nodes just calculate when it is their
 * turn and invoke CoordinateGloballyDurable.
 *
 * CoordinateShardDurable needs nodes to not overlap on the ranges they operate on or the exlusive sync points overlap
 * with each other and block progress. A target duration to process the entire ring is set, and then each node in the
 * current round has a time in the round that it should start processing, and the time it starts and the subranges it is
 * responsible for rotates backwards every round so that a down node doesn't prevent a subrange from being processed.
 *
 * The work for CoordinateShardDurable is further subdivided where each subrange a node operates on is divided a fixed
 * number of times and then processed one at a time with a fixed wait between them.
 *
 * TODO (expected): cap number of coordinations we can have in flight at once
 * TODO (expected): do not start new ExclusiveSyncPoint if we have more than X already agreed and not yet applied
 * Didn't go with recurring because it doesn't play well with async execution of these tasks
 */
public class DurabilityScheduling implements ConfigurationService.Listener
{
    private static final Logger logger = LoggerFactory.getLogger(DurabilityScheduling.class);

    private final Node node;
    private Scheduler.Scheduled scheduled;

    /*
     * In each cycle, attempt to split the range into this many pieces; if we fail, we increase the number of pieces
     */
    private int targetShardSplits = 64;

    /*
     * In each round at each node wait this amount of time between allocating a CoordinateShardDurable txnId
     * and coordinating the shard durability
     */
    private long txnIdLagMicros = TimeUnit.SECONDS.toMicros(5L);

    /*
     * In each round at each node wait this amount of time between allocating a CoordinateShardDurable txnId
     * and coordinating the shard durability
     */
    private long durabilityLagMicros = TimeUnit.MILLISECONDS.toMicros(500L);

    /*
     * Target for how often the entire ring should be processed in microseconds. Every node will start at an offset in the current round that is based
     * on this value / by (total # replicas * its index in the current round). The current round is determined by dividing time since the epoch by this
     * duration.
     */
    private long shardCycleTimeMicros = TimeUnit.SECONDS.toMicros(30);

    /*
     * Every node will independently attempt to invoke CoordinateGloballyDurable
     * with a target gap between invocations of globalCycleTimeMicros
     *
     * This is done by nodes taking turns for each scheduled attempt that is due by calculating what # attempt is
     * next for the current node ordinal in the cluster and time since the elapsed epoch and attempting to invoke then. If all goes
     * well they end up doing it periodically in a timely fashion with the target gap achieved.
     *
     * TODO (desired): run this more often, but for less than the whole cluster, patterned in such a way as to ensure rapid cross-pollination
     */
    private long globalCycleTimeMicros = TimeUnit.SECONDS.toMicros(30);

    private long defaultRetryDelayMicros = TimeUnit.SECONDS.toMicros(1);
    private long maxRetryDelayMicros = TimeUnit.MINUTES.toMicros(1);
    private int maxNumberOfSplits = 1 << 10;

    private Topology currentGlobalTopology;
    private final Map<Range, ShardScheduler> shardSchedulers = new HashMap<>();
    private int globalIndex;

    boolean started;
    volatile boolean stop;

    @Override
    public AsyncResult<Void> onTopologyUpdate(Topology topology, boolean isLoad, boolean startSync)
    {
        updateTopology(topology);
        return AsyncResults.success(null);
    }

    private class ShardScheduler
    {
        Shard shard;

        // time to start a new cycle
        int nodeOffset;

        int index;
        int numberOfSplits, maxNumberOfSplits;
        Scheduler.Scheduled scheduled;
        long rangeStartedAtMicros, cycleStartedAtMicros;
        long retryDelayMicros = defaultRetryDelayMicros;
        boolean defunct;
        SyncPoint<Range> lastApplied, lastAgreed;

        private ShardScheduler()
        {
        }

        synchronized void update(Shard shard, int offset)
        {
            this.shard = shard;
            this.nodeOffset = offset;
            if (numberOfSplits == 0 || numberOfSplits < targetShardSplits)
                numberOfSplits = targetShardSplits;
        }

        synchronized void markDefunct()
        {
            defunct = true;
            logger.info("Discarding defunct shard durability scheduler for {}", shard);
        }

        synchronized void retryCoordinateDurability(Node node, SyncPoint<Range> exclusiveSyncPoint, int nextIndex)
        {
            if (defunct)
                return;

            // TODO (expected): back-off
            coordinateShardDurableAfterExclusiveSyncPoint(node, exclusiveSyncPoint, nextIndex);
        }

        synchronized void restart()
        {
            if (defunct)
                return;

            long nowMicros = node.elapsed(MICROSECONDS);
            long microsOffset = (nodeOffset * shardCycleTimeMicros) / shard.rf();
            long scheduleAt = nowMicros - (nowMicros % shardCycleTimeMicros) + microsOffset;
            if (nowMicros > scheduleAt + (shardCycleTimeMicros / numberOfSplits))
                scheduleAt += shardCycleTimeMicros;

            maxNumberOfSplits = Math.max(1, Math.min(DurabilityScheduling.this.maxNumberOfSplits, node.commandStores().shardDistributor().numberOfSplitsPossible(shard.range)));
            int target = Math.min(targetShardSplits, maxNumberOfSplits);
            if (numberOfSplits < target)
                numberOfSplits = target;
            if (numberOfSplits > maxNumberOfSplits)
                numberOfSplits = maxNumberOfSplits;

            index = 0;
            cycleStartedAtMicros = scheduleAt;
            scheduleAt(nowMicros, scheduleAt);
        }

        synchronized void schedule()
        {
            if (defunct)
                return;

            Invariants.checkState(index < numberOfSplits);
            long nowMicros = node.elapsed(MICROSECONDS);
            long microsOffset = (index * shardCycleTimeMicros) / numberOfSplits;
            long scheduleAt = cycleStartedAtMicros + microsOffset;
            if (retryDelayMicros > defaultRetryDelayMicros)
            {
                retryDelayMicros = Math.max(defaultRetryDelayMicros, (long)(0.9 * retryDelayMicros));
            }
            if (numberOfSplits > targetShardSplits && index % 4 == 0)
            {
                index /= 4;
                numberOfSplits /= 4;
            }
            scheduleAt(nowMicros, scheduleAt);
        }

        synchronized void retry()
        {
            if (defunct)
                return;

            long nowMicros = node.elapsed(MICROSECONDS);
            long scheduleAt = nowMicros + retryDelayMicros;
            retryDelayMicros += retryDelayMicros / 2;
            if (retryDelayMicros > maxRetryDelayMicros)
            {
                retryDelayMicros = maxRetryDelayMicros;
            }
            scheduleAt(nowMicros, scheduleAt);
        }

        synchronized void scheduleAt(long nowMicros, long scheduleAt)
        {
            ShardDistributor distributor = node.commandStores().shardDistributor();
            Range range;
            int nextIndex;
            {
                Invariants.checkState(index < numberOfSplits);
                int i = index;
                Range selectRange = null;
                while (selectRange == null && ++i <= numberOfSplits)
                    selectRange = distributor.splitRange(shard.range, index, i, numberOfSplits);
                if (selectRange == null)
                {
                    if (index == 0)
                    {
                        logger.warn("Range {} appears to be impossible to split. Using full range.", shard.range);
                        selectRange = shard.range;
                    }
                    else
                    {
                        restart();
                        return;
                    }
                }
                nextIndex = i;
                range = selectRange;
            }

            Runnable schedule = () -> {
                // TODO (expected): allocate stale HLC from a reservation of HLCs for this purpose
                TxnId syncId = node.nextTxnId(ExclusiveSyncPoint, Domain.Range);
                startShardSync(syncId, Ranges.of(range), nextIndex);
            };
            if (scheduleAt <= nowMicros) schedule.run();
            else scheduled = node.scheduler().selfRecurring(schedule, scheduleAt - nowMicros, MICROSECONDS);
        }

        /**
         * The first step for coordinating shard durable is to run an exclusive sync point
         * the result of which can then be used to run
         */
        private void startShardSync(TxnId syncId, Ranges ranges, int nextIndex)
        {
            scheduled = node.scheduler().selfRecurring(() -> node.withEpoch(syncId.epoch(), (ignored, withEpochFailure) -> {
                if (withEpochFailure != null)
                {
                    // don't wait on epoch failure - we aren't the cause of any problems
                    startShardSync(syncId, ranges, nextIndex);
                    Throwable wrapped = CoordinationFailed.wrap(withEpochFailure);
                    logger.trace("Exception waiting for epoch before coordinating exclusive sync point for local shard durability, epoch " + syncId.epoch(), wrapped);
                    node.agent().onUncaughtException(wrapped);
                    return;
                }
                scheduled = null;
                rangeStartedAtMicros = node.elapsed(MICROSECONDS);
                FullRoute<Range> route = (FullRoute<Range>) node.computeRoute(syncId, ranges);
                exclusiveSyncPoint(node, syncId, route)
                .addCallback((success, fail) -> {
                    if (fail != null)
                    {
                        synchronized (ShardScheduler.this)
                        {
                            // TODO (required): try to recover or invalidate prior sync point
                            retry();
                            if (numberOfSplits * 2 <= maxNumberOfSplits)
                            {
                                index *= 2;
                                numberOfSplits *= 2;
                            }
                            logger.warn("{}: Exception coordinating ExclusiveSyncPoint for {} durability. Increased numberOfSplits to " + numberOfSplits, syncId, ranges, fail);
                        }
                    }
                    else
                    {   // TODO (required): decouple CoordinateShardDurable concurrency from CoordinateSyncPoint (i.e., permit at least one CoordinateSyncPoint to queue up while we're coordinating durability)
                        coordinateShardDurableAfterExclusiveSyncPoint(node, success, nextIndex);
                        logger.debug("{}: Successfully coordinated ExclusiveSyncPoint for local shard durability of {}", syncId, ranges);
                        lastAgreed = success;
                    }
                });
            }), txnIdLagMicros, MICROSECONDS);
        }

        private void coordinateShardDurableAfterExclusiveSyncPoint(Node node, SyncPoint<Range> exclusiveSyncPoint, int nextIndex)
        {
            scheduled = node.scheduler().selfRecurring(() -> {
                scheduled = null;
                node.commandStores().any().execute(() -> {
                    coordinate(node, exclusiveSyncPoint)
                    .addCallback((success, fail) -> {
                        if (fail != null && fail.getClass() != SyncPointErased.class)
                        {
                            logger.debug("Exception coordinating shard durability for {}, will retry", exclusiveSyncPoint.route.toRanges(), fail);
                            retryCoordinateDurability(node, exclusiveSyncPoint, nextIndex);
                        }
                        else
                        {
                            try
                            {
                                synchronized (ShardScheduler.this)
                                {
                                    int prevIndex = index;
                                    index = nextIndex;
                                    if (index >= numberOfSplits)
                                    {
                                        long nowMicros = node.elapsed(MICROSECONDS);
                                        long timeTakenSeconds = MICROSECONDS.toSeconds(nowMicros - cycleStartedAtMicros);
                                        long targetTimeSeconds = MICROSECONDS.toSeconds(shardCycleTimeMicros);
                                        logger.info("Successfully completed one cycle of durability scheduling for shard {} in {}s (vs {}s target)", shard.range, timeTakenSeconds, targetTimeSeconds);
                                        restart();
                                    }
                                    else
                                    {
                                        long nowMicros = node.elapsed(MICROSECONDS);
                                        int prevRfCycle = (prevIndex * shard.rf()) / numberOfSplits;
                                        int curRfCycle = (index * shard.rf()) / numberOfSplits;
                                        if (prevRfCycle != curRfCycle)
                                        {
                                            long targetTimeSeconds = MICROSECONDS.toSeconds((index * shardCycleTimeMicros) / numberOfSplits);
                                            long timeTakenSeconds = MICROSECONDS.toSeconds(nowMicros - cycleStartedAtMicros);
                                            logger.info("Successfully completed {}/{} cycle of durability scheduling covering range {}. Completed in {}s (vs {}s target).", curRfCycle, shard.rf(), exclusiveSyncPoint.route.toRanges(), timeTakenSeconds, targetTimeSeconds);
                                        }
                                        else if (logger.isTraceEnabled())
                                        {
                                            logger.debug("Successfully coordinated shard durability for range {} in {}s", shard.range, MICROSECONDS.toSeconds(nowMicros - rangeStartedAtMicros));
                                        }
                                        schedule();
                                    }
                                }
                                lastApplied = success;
                            }
                            catch (Throwable t)
                            {
                                retry();
                                logger.error("Unexpected exception handling durability scheduling callback; starting from scratch", t);
                            }
                        }
                    });
                });
            }, durabilityLagMicros, MICROSECONDS);
        }
    }

    public DurabilityScheduling(Node node)
    {
        this.node = node;
    }

    public void setTargetShardSplits(int targetShardSplits)
    {
        this.targetShardSplits = BitUtil.findNextPositivePowerOfTwo(targetShardSplits);
    }

    public void setDefaultRetryDelay(long retryDelay, TimeUnit units)
    {
        this.defaultRetryDelayMicros = units.toMicros(retryDelay);
    }

    public void setMaxRetryDelay(long retryDelay, TimeUnit units)
    {
        this.maxRetryDelayMicros = units.toMicros(retryDelay);
    }

    public void setTxnIdLag(long txnIdLag, TimeUnit units)
    {
        this.txnIdLagMicros = Ints.saturatedCast(units.toMicros(txnIdLag));
    }

    public void setDurabilityLag(long durabilityLag, TimeUnit units)
    {
        this.durabilityLagMicros = Ints.saturatedCast(units.toMicros(durabilityLag));
    }

    public void setShardCycleTime(long shardCycleTime, TimeUnit units)
    {
        this.shardCycleTimeMicros = Ints.saturatedCast(units.toMicros(shardCycleTime));
    }

    public void setGlobalCycleTime(long globalCycleTime, TimeUnit units)
    {
        this.globalCycleTimeMicros = units.toMicros(globalCycleTime);
    }

    /**
     * Schedule regular invocations of CoordinateShardDurable and CoordinateGloballyDurable
     */
    public synchronized void start()
    {
        Invariants.checkState(!stop); // cannot currently restart safely
        started = true;
        updateTopology();
        long nowMicros = node.elapsed(MICROSECONDS);
        long scheduleAt = computeNextGlobalSyncTime(nowMicros);
        scheduled = node.scheduler().selfRecurring(this::run, scheduleAt - nowMicros, MICROSECONDS);
    }

    public synchronized void stop()
    {
        if (scheduled != null)
            scheduled.cancel();
        stop = true;
        for (ShardScheduler scheduler : shardSchedulers.values())
            scheduler.markDefunct();
        shardSchedulers.clear();
    }

    /**
     * Update our topology information, and schedule any global syncs that may be pending.
     */
    private void run()
    {
        if (stop)
            return;

        long nowMicros = node.elapsed(MICROSECONDS);
        try
        {
            if (currentGlobalTopology == null || currentGlobalTopology.size() == 0)
                return;

            startGlobalSync();
        }
        finally
        {
            long scheduleAt = computeNextGlobalSyncTime(nowMicros);
            node.scheduler().selfRecurring(this::run, scheduleAt - nowMicros, MICROSECONDS);
        }
    }

    private void startGlobalSync()
    {
        try
        {
            long epoch = node.epoch();
            AsyncChain<AsyncResult<Void>> resultChain = node.withEpoch(epoch, () -> node.commandStores().any().submit(() -> CoordinateGloballyDurable.coordinate(node, epoch)));
            resultChain.begin((success, fail) -> {
                if (fail != null) logger.trace("Exception initiating coordination of global durability", fail);
                else logger.trace("Successful coordination of global durability");
            });
        }
        catch (Exception e)
        {
            logger.error("Exception invoking withEpoch to start coordination for global durability", e);
        }
    }

    public synchronized void updateTopology()
    {
        Topology latestGlobal = node.topology().current();
        updateTopology(latestGlobal);
    }

    private synchronized void updateTopology(Topology latestGlobal)
    {
        if (!started)
            return;

        if (latestGlobal == currentGlobalTopology || (currentGlobalTopology != null && latestGlobal.epoch() < currentGlobalTopology.epoch()))
            return;

        Topology latestLocal = latestGlobal.forNode(node.id());
        if (latestLocal.size() == 0)
            return;

        currentGlobalTopology = latestGlobal;
        List<Node.Id> ids = new ArrayList<>(latestGlobal.nodes());
        Collections.sort(ids);
        globalIndex = ids.indexOf(node.id());

        Map<Range, ShardScheduler> prev = new HashMap<>(this.shardSchedulers);
        this.shardSchedulers.clear();
        for (Shard shard : latestLocal.shards())
        {
            ShardScheduler prevScheduler = prev.remove(shard.range);
            ShardScheduler scheduler = prevScheduler;
            if (scheduler == null)
                scheduler = new ShardScheduler();
            shardSchedulers.put(shard.range, scheduler);
            scheduler.update(shard, shard.nodes.find(node.id()));
            if (prevScheduler == null)
            {
                logger.info("Starting shard durability scheduler for {}", shard);
                scheduler.restart();
            }
        }
        prev.forEach((r, s) -> s.markDefunct());
    }

    /**
     * Based on the current elapsed time (simulated or otherwise) calculate the wait time in microseconds until the next turn of this
     * node for some activity with a target gap between nodes doing the activity.
     *
     * This is done by taking the index of the node in the current topology and the total number of nodes
     * and then using the target gap between invocations to calculate a "round" duration and point of time each node
     * should have its turn in each round based on its index and calculating the time to the next turn for that node will occur.
     *
     * It's assumed it is fine if nodes overlap or reorder or skip for whatever activity we are picking turns for as long as it is approximately
     * the right pacing.
     */
    private long computeNextGlobalSyncTime(long nowMicros)
    {
        if (currentGlobalTopology == null)
            return nowMicros + globalCycleTimeMicros;

        // How long it takes for all nodes to go once
        long totalRoundDuration = currentGlobalTopology.nodes().size() * globalCycleTimeMicros;
        long startOfCurrentRound = (nowMicros / totalRoundDuration) * totalRoundDuration;

        // In a given round at what time in the round should this node take its turn
        long ourOffsetInRound = globalIndex * globalCycleTimeMicros;

        long targetTimeInCurrentRound = startOfCurrentRound + ourOffsetInRound;
        long targetTime = targetTimeInCurrentRound;
        // If our time to run in the current round already passed then schedule it in the next round
        if (targetTimeInCurrentRound < nowMicros)
            targetTime += totalRoundDuration;

        return targetTime;
    }

    @Override
    public void onRemoteSyncComplete(Node.Id node, long epoch)
    {
    }

    @Override
    public void truncateTopologyUntil(long epoch)
    {
    }

    @Override
    public void onEpochClosed(Ranges ranges, long epoch)
    {
    }

    @Override
    public void onEpochRedundant(Ranges ranges, long epoch)
    {
    }
}