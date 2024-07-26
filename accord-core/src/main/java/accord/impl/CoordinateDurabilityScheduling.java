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

import accord.api.Scheduler;
import accord.coordinate.CoordinateGloballyDurable;
import accord.coordinate.CoordinateShardDurable;
import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.ExecuteSyncPoint.SyncPointErased;
import accord.local.Node;
import accord.local.ShardDistributor;
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
public class CoordinateDurabilityScheduling
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinateDurabilityScheduling.class);

    private final Node node;
    private Scheduler.Scheduled scheduled;

    /*
     * In each round at each node wait this amount of time between initiating new CoordinateShardDurable
     */
    private int frequencyMicros = Ints.saturatedCast(TimeUnit.MILLISECONDS.toMicros(500L));

    /*
     * In each round at each node wait this amount of time between allocating a CoordinateShardDurable txnId
     * and coordinating the shard durability
     */
    private int txnIdLagMicros = Ints.saturatedCast(TimeUnit.SECONDS.toMicros(1L));

    /*
     * In each round at each node wait this amount of time between allocating a CoordinateShardDurable txnId
     * and coordinating the shard durability
     */
    private int durabilityLagMicros = Ints.saturatedCast(TimeUnit.MILLISECONDS.toMicros(500L));

    /*
     * Target for how often the entire ring should be processed in microseconds. Every node will start at an offset in the current round that is based
     * on this value / by (total # replicas * its index in the current round). The current round is determined by dividing time since the epoch by this
     * duration.
     */
    private int shardCycleTimeMicros = Ints.saturatedCast(TimeUnit.SECONDS.toMicros(30));

    /*
     * Every node will independently attempt to invoke CoordinateGloballyDurable
     * with a target gap between invocations of globalCycleTimeMicros
     *
     * This is done by nodes taking turns for each scheduled attempt that is due by calculating what # attempt is
     * next for the current node ordinal in the cluster and time since the unix epoch and attempting to invoke then. If all goes
     * well they end up doing it periodically in a timely fashion with the target gap achieved.
     *
     * TODO (desired): run this more often, but for less than the whole cluster, patterned in such a way as to ensure rapid cross-pollination
     */
    private long globalCycleTimeMicros = TimeUnit.SECONDS.toMicros(30);

    private Topology currentGlobalTopology;
    private final Map<Shard, Integer> localIndexes = new HashMap<>();
    private int globalIndex;

    private long nextGlobalSyncTimeMicros;
    private long prevShardSyncTimeMicros;
    volatile boolean stop;

    public CoordinateDurabilityScheduling(Node node)
    {
        this.node = node;
    }

    public void setFrequency(int frequency, TimeUnit units)
    {
        this.frequencyMicros = Ints.saturatedCast(units.toMicros(frequency));
    }

    public void setTxnIdLag(int txnIdLag, TimeUnit units)
    {
        this.txnIdLagMicros = Ints.saturatedCast(units.toMicros(txnIdLag));
    }

    public void setDurabilityLag(int durabilityLag, TimeUnit units)
    {
        this.durabilityLagMicros = Ints.saturatedCast(units.toMicros(durabilityLag));
    }

    public void setShardCycleTime(int shardCycleTime, TimeUnit units)
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
        long nowMicros = node.unix(MICROSECONDS);
        prevShardSyncTimeMicros = nowMicros;
        setNextGlobalSyncTime(nowMicros);
        scheduled = node.scheduler().recurring(this::run, frequencyMicros, MICROSECONDS);
    }

    public void stop()
    {
        if (scheduled != null)
            scheduled.cancel();
        stop = true;
    }

    /**
     * Schedule the first CoordinateShardDurable execution for the current round. Sub-steps will be scheduled after
     * each sub-step completes, and once all are completed scheduleCoordinateShardDurable is called again.
     */
    private void run()
    {
        if (stop)
            return;

        updateTopology();
        if (currentGlobalTopology == null || currentGlobalTopology.size() == 0)
            return;

        long nowMicros = node.unix(MICROSECONDS);
        if (nextGlobalSyncTimeMicros <= nowMicros)
        {
            startGlobalSync();
            setNextGlobalSyncTime(nowMicros);
        }

        List<Ranges> coordinate = rangesToShardSync(nowMicros);
        prevShardSyncTimeMicros = nowMicros;
        if (coordinate.isEmpty())
        {
            logger.trace("Nothing pending in schedule for time slot at {}", nowMicros);
            return;
        }

        logger.trace("Scheduling CoordinateShardDurable for {} at {}", coordinate, nowMicros);
        for (Ranges ranges : coordinate)
            startShardSync(ranges);
    }

    /**
     * The first step for coordinating shard durable is to run an exclusive sync point
     * the result of which can then be used to run
     */
    private void startShardSync(Ranges ranges)
    {
        TxnId at = node.nextTxnId(ExclusiveSyncPoint, Domain.Range);
        logger.trace("{}: Coordinating ExclusiveSyncPoint for local shard durability of {}", at, ranges);
        node.scheduler().once(() -> node.withEpoch(at.epoch(), () -> {
                           CoordinateSyncPoint.exclusive(node, at, ranges)
                               .addCallback((success, fail) -> {
                                   if (fail != null)
                                   {
                                       logger.trace("{}: Exception coordinating ExclusiveSyncPoint for local shard durability of {}", at, ranges, fail);
                                   }
                                   else
                                   {
                                       coordinateShardDurableAfterExclusiveSyncPoint(node, success);
                                       logger.trace("{}: Successfully coordinated ExclusiveSyncPoint for local shard durability of {}", at, ranges);
                                   }
                               });
        }), txnIdLagMicros, MICROSECONDS);
    }

    private void coordinateShardDurableAfterExclusiveSyncPoint(Node node, SyncPoint exclusiveSyncPoint)
    {
        node.scheduler().once(() -> {
            node.commandStores().any().execute(() -> {
                CoordinateShardDurable.coordinate(node, exclusiveSyncPoint)
                                      .addCallback((success, fail) -> {
                                          if (fail != null && fail.getClass() != SyncPointErased.class)
                                          {
                                              logger.trace("Exception coordinating local shard durability, will retry immediately", fail);
                                              coordinateShardDurableAfterExclusiveSyncPoint(node, exclusiveSyncPoint);
                                          }
                                      });
            });
        }, durabilityLagMicros, MICROSECONDS);
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

    private List<Ranges> rangesToShardSync(long nowMicros)
    {
        ShardDistributor distributor = node.commandStores().shardDistributor();
        List<Ranges> result = new ArrayList<>();
        for (Map.Entry<Shard, Integer> e : localIndexes.entrySet())
        {
            Shard shard = e.getKey();
            int index = e.getValue();
            int shardCycleTimeMicros = Math.max(this.shardCycleTimeMicros, Ints.saturatedCast(shard.rf() * 3L * frequencyMicros));
            long microsOffset = (index * (long)shardCycleTimeMicros) / shard.rf();
            long prevSyncTimeMicros = Math.max(prevShardSyncTimeMicros, nowMicros - ((shardCycleTimeMicros / shard.rf()) / 2L));
            int from = (int) ((prevSyncTimeMicros + microsOffset) % shardCycleTimeMicros);
            int to = (int) ((nowMicros + microsOffset) % shardCycleTimeMicros);
            List<Range> ranges = new ArrayList<>();
            if (from > to)
            {
                Range range = distributor.splitRange(shard.range, to, shardCycleTimeMicros, shardCycleTimeMicros);
                if (range != null)
                    ranges.add(range);
                to = from;
                from = 0;
            }

            Range range = distributor.splitRange(shard.range, from, to, shardCycleTimeMicros);
            if (range != null)
                ranges.add(range);

            if (!ranges.isEmpty())
                result.add(Ranges.of(ranges.toArray(new Range[0])));
        }
        return result;
    }

    private void updateTopology()
    {
        Topology latestGlobal = node.topology().current();
        if (latestGlobal == currentGlobalTopology)
            return;

        Topology latestLocal = latestGlobal.forNode(node.id());
        if (latestLocal.size() == 0)
            return;

        currentGlobalTopology = latestGlobal;
        List<Node.Id> ids = new ArrayList<>(latestGlobal.nodes());
        Collections.sort(ids);
        globalIndex = ids.indexOf(node.id());

        localIndexes.clear();
        for (Shard shard : latestLocal.shards())
            localIndexes.put(shard, shard.sortedNodes.indexOf(node.id()));
    }

    /**
     * Based on the current unix time (simulated or otherwise) calculate the wait time in microseconds until the next turn of this
     * node for some activity with a target gap between nodes doing the activity.
     *
     * This is done by taking the index of the node in the current topology and the total number of nodes
     * and then using the target gap between invocations to calculate a "round" duration and point of time each node
     * should have its turn in each round based on its index and calculating the time to the next turn for that node will occur.
     *
     * It's assumed it is fine if nodes overlap or reorder or skip for whatever activity we are picking turns for as long as it is approximately
     * the right pacing.
     */
    private void setNextGlobalSyncTime(long nowMicros)
    {
        if (currentGlobalTopology == null)
        {
            nextGlobalSyncTimeMicros = nowMicros;
            return;
        }

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

        nextGlobalSyncTimeMicros = targetTime - nowMicros;
    }
}