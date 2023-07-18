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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.CoordinateGloballyDurable;
import accord.coordinate.CoordinateShardDurable;
import accord.coordinate.CoordinateSyncPoint;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.topology.Topology;
import accord.topology.TopologyManager.NodeAndTopologyInfo;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;

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
 * // TODO review will the scheduler shut down on its own or do the individual tasks need to be canceled manually?
 * Didn't go with recurring because it doesn't play well with async execution of these tasks
 */
public class CoordinateDurabilityScheduling
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinateDurabilityScheduling.class);

    /*
     * Divide each sub-range a node handles in a given round into COORDINATE_SHARD_DURABLE_RANGE_STEPS steps and then every
     * COORDINATE_SHARD_DURABLE_RANGE_STEP_GAP_MILLIS invoke CoordinateShardDurable for a sub-range of a sub-range.
     */
    static final int COORDINATE_SHARD_DURABLE_RANGE_STEPS = 10;

    /*
     * In each round at each node wait this amount of time between CoordinateShardDurable invocations for each sub-range (of a sub-range) step
     */
    static final int COORDINATE_SHARD_DURABLE_RANGE_STEP_GAP_MILLIS = 100;

    /*
     * Target for how often the entire ring should be processed in microseconds. Every node will start at an offset in the current round that is based
     * on this value / by (total # nodes * its index in the current round). The current round is determined by dividing time since the epoch by this
     * duration.
     */
    static final long COORDINATE_SHARD_DURABLE_FULL_RING_TARGET_PROCESSING_MICROS = TimeUnit.SECONDS.toMicros(30);

    /*
     * Every node will independently attempt to invoke CoordinateGloballyDurable
     * with a target gap between invocations of COORDINATE_GLOBALLY_DURABLE_TARGET_GAP_MICROS.
     *
     * This is done by nodes taking turns for each scheduled attempt that is due by calculating what # attempt is
     * next for the current node ordinal in the cluster and time since the unix epoch and attempting to invoke then. If all goes
     * well they end up doing it periodically in a timely fashion with the target gap achieved.
     *
     */
    static final long COORDINATE_GLOBALLY_DURABLE_TARGET_GAP_MICROS = TimeUnit.MILLISECONDS.toMicros(1000);

    /**
     * Schedule regular invocations of CoordinateShardDurable and CoordinateGloballyDurable
     */
    public static void scheduleDurabilityPropagation(Node node)
    {
        scheduleCoordinateShardDurable(node);
        scheduleCoordinateGloballyDurable(node);
    }

    /**
     * Schedule the first CoordinateShardDurable execution for the current round. Sub-steps will be scheduled after
     * each sub-step completes, and once all are completed scheduleCoordinateShardDurable is called again.
     */
    private static void scheduleCoordinateShardDurable(Node node)
    {
        logger.trace("Scheduling once CoordinateShardDurable node {} at {}",  node.id(), node.unix(TimeUnit.MICROSECONDS));
        NodeAndTopologyInfo nodeAndTopologyInfo = node.topology().currentNodeAndTopologyInfo();
        // Empty epochs happen during node removal, startup, and tests so check again in 1 second
        if (nodeAndTopologyInfo == null)
        {
            logger.trace("Empty topology, scheduling coordinate shard durable in 1 second at node {}", node.id());
            node.scheduler().once(() -> scheduleCoordinateShardDurable(node), 1, TimeUnit.SECONDS);
            return;
        }

        Topology topology = nodeAndTopologyInfo.topology;
        int nodeCount = topology.nodes().size();
        CommandStores commandStores = node.commandStores();
        ShardDistributor distributor = commandStores.shardDistributor();
        Ranges ranges = topology.ranges();

        // During startup or when there is nothing to do just skip
        // since it generates a lot of noisy errors if you move forward
        // with no shards
        if (ranges.isEmpty() || commandStores.count() == 0)
        {
            logger.trace("No ranges or no command stores, scheduling coordinate shard durable in 1 second at node {}", node.id());
            node.scheduler().once(() -> scheduleCoordinateShardDurable(node), 1, TimeUnit.SECONDS);
            return;
        }

        int ourIndex = nodeAndTopologyInfo.ourIndex;
        long nowMicros = node.unix(TimeUnit.MICROSECONDS);
        // Which round of iterating across the ring is currently occurring given the target processing time for the full ring
        long currentRound = nowMicros / COORDINATE_SHARD_DURABLE_FULL_RING_TARGET_PROCESSING_MICROS;
        // Wall time when the current round started
        long startOfCurrentRound = currentRound * COORDINATE_SHARD_DURABLE_FULL_RING_TARGET_PROCESSING_MICROS;

        // Every round rotate backwards which subranges this node is responsible for so if there is a down node
        // another node will eventually come along and propagate durability
        // Backwards so that a node doesn't start last in one round and then start first in the next round
        // and likely miss scheduling when it is supposed to run.
        int rotationForCurrentRound = Ints.checkedCast(currentRound % nodeCount);
        ourIndex = (ourIndex - rotationForCurrentRound) % (nodeCount + 1);
        if (ourIndex < 0)
            ourIndex += nodeCount + 1;
        long ourTargetStartTime = startOfCurrentRound + ((COORDINATE_SHARD_DURABLE_FULL_RING_TARGET_PROCESSING_MICROS / nodeCount) * ourIndex);
        // TODO review this can have the behavior of all nodes syncing up if we don't force them to wait for their turn in the next round
        long ourDelayUntilStarting = Math.max(0, ourTargetStartTime - nowMicros);

        // In each step coordinate shard durability for a subset of every range
        List<Range> slices = new ArrayList<>(ranges.size());
        for (int i = 0; i < ranges.size(); i++)
        {
            Range r = ranges.get(i);
            Range slice = distributor.splitRange(r, ourIndex, nodeCount);
            if (slice == r && ourIndex != 0)
                // Have node index 0 always be responsible for processing indivisible ranges
                // so multiple nodes don't conflict coordinating for the same indivisible range
                continue;
            slices.add(slice);
        }
        Range[] slicesArray = new Range[slices.size()];
        slicesArray = slices.toArray(slicesArray);

        node.scheduler().once(getCoordinateShardDurableRunnable(node, Ranges.ofSortedAndDeoverlapped(slicesArray), 0), ourDelayUntilStarting, TimeUnit. MICROSECONDS);
    }

    private static Runnable getCoordinateShardDurableRunnable(Node node, Ranges ranges, int currentStep)
    {
        return () -> {
            try
            {
                coordinateExclusiveSyncPointForCoordinateShardDurable(node, ranges, currentStep);
            }
            catch (Exception e)
            {
                logger.error("Exception in initial range calculation and initiation of exclusive sync point for local shard durability", e);
            }
        };
    }

    /**
     * The first step for coordinating shard durable is to run an exclusive sync point
     * the result of which can then be used to run
     */
    private static void coordinateExclusiveSyncPointForCoordinateShardDurable(Node node, Ranges ranges, int currentStep)
    {
        ShardDistributor distributor = node.commandStores().shardDistributor();
        // In each step coordinate shard durability for a subrange of each range
        Range[] slices = new Range[ranges.size()];
        for (int i = 0; i < ranges.size(); i++)
        {
            Range r = ranges.get(i);
            slices[i] = distributor.splitRange(r, currentStep, COORDINATE_SHARD_DURABLE_RANGE_STEPS);
        }

        CoordinateSyncPoint.exclusive(node, Ranges.ofSortedAndDeoverlapped(slices))
                .addCallback((success, fail) -> {
                    if (fail != null)
                    {
                        logger.error("Exception coordinating exclusive sync point for local shard durability", fail);
                        scheduleCoordinateShardDurable(node);
                    }
                    else
                    {
                        coordinateShardDurableAfterExclusiveSyncPoint(node, ranges, success, currentStep);
                    }
                });
    }

    private static void coordinateShardDurableAfterExclusiveSyncPoint(Node node, Ranges ranges, SyncPoint exclusiveSyncPoint, int currentStep)
    {
        CoordinateShardDurable.coordinate(node, exclusiveSyncPoint)
                .addCallback((success, fail) -> {
                    if (fail != null)
                    {
                        logger.error("Exception coordinating local shard durability, will retry immediately", fail);
                        // On failure don't increment currentStep
                        // TODO review is it better to increment the currentStep so if there is a stuck portion we will move past it and at least
                        // make some progress?
                        coordinateShardDurableAfterExclusiveSyncPoint(node, ranges, exclusiveSyncPoint, currentStep);
                    }
                    else
                    {
                        int nextStep = currentStep + 1;
                        if (nextStep >= COORDINATE_SHARD_DURABLE_RANGE_STEPS)
                            // Schedule the next time to start the steps from the beginning on whatever ranges we target in the next round
                            scheduleCoordinateShardDurable(node);
                        else
                            // Continue on to scheduling the next subrange step with a fixed delay
                            // TODO review Should there be a fixed gap here or just keep going immediately?
                            node.scheduler().once(() -> coordinateExclusiveSyncPointForCoordinateShardDurable(node, ranges, currentStep + 1), COORDINATE_SHARD_DURABLE_RANGE_STEP_GAP_MILLIS, TimeUnit.MILLISECONDS);
                    }
                });
    }

    private static void scheduleCoordinateGloballyDurable(Node node)
    {
        node.scheduler().once(getCoordinateGloballyDurableRunnable(node), getNextCoordinateGloballyDurableWaitMicros(node), TimeUnit.MICROSECONDS);
    }

    private static Runnable getCoordinateGloballyDurableRunnable(Node node)
    {
        return () -> {
            try
            {
                // During startup or when there is nothing to do just skip
                // since it generates a lot of noisy errors if you move forward
                // with no shards
                Ranges nodeRanges = node.topology().current().rangesForNode(node.id());
                if (nodeRanges.isEmpty() || node.commandStores().count() == 0)
                {
                    scheduleCoordinateGloballyDurable(node);
                    return;
                }

                long epoch = node.epoch();
                // TODO review Wanted this to execute async and not block the scheduler
                AsyncChain<AsyncResult<Void>> resultChain = node.withEpoch(epoch, () -> node.commandStores().any().submit(() -> CoordinateGloballyDurable.coordinate(node, epoch)));
                resultChain.begin((success, fail) -> {
                    if (fail != null)
                    {
                        logger.error("Exception initiating coordination in withEpoch for global durability", fail);
                        scheduleCoordinateGloballyDurable(node);
                    }
                    else
                    {
                        success.addCallback(coordinateGloballyDurableCallback(node));
                    }
                });
            }
            catch (Exception e)
            {
                logger.error("Exception invoking withEpoch to start coordination for global durability", e);
            }
        };
    }

    private static BiConsumer<Void, Throwable> coordinateGloballyDurableCallback(Node node)
    {
        return (success, fail) -> {
            if (fail != null)
                logger.error("Exception coordinating global durability", fail);
            // Reschedule even on failure because we never stop need to propagate global durability
            scheduleCoordinateGloballyDurable(node);
        };
    }

    private static long getNextCoordinateGloballyDurableWaitMicros(Node node)
    {
        return getNextTurn(node, COORDINATE_GLOBALLY_DURABLE_TARGET_GAP_MICROS);
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
    private static long getNextTurn(Node node, long targetGapMicros)
    {
        NodeAndTopologyInfo nodeAndTopologyInfo = node.topology().currentNodeAndTopologyInfo();
        // Empty epochs happen during node removal, startup, and tests so check again in 1 second
        if (nodeAndTopologyInfo == null)
            return TimeUnit.SECONDS.toMicros(1);

        int ourIndex = nodeAndTopologyInfo.ourIndex;
        long nowMicros = node.unix(TimeUnit.MICROSECONDS);
        // How long it takes for all nodes to go once
        long totalRoundDuration = nodeAndTopologyInfo.topology.nodes().size() * targetGapMicros;
        long startOfCurrentRound = (nowMicros / totalRoundDuration) * totalRoundDuration;

        // In a given round at what time in the round should this node take its turn
        long ourOffsetInRound = ourIndex * targetGapMicros;

        long targetTimeInCurrentRound = startOfCurrentRound + ourOffsetInRound;
        long targetTime = targetTimeInCurrentRound;
        // If our time to run in the current round already passed then schedule it in the next round
        if (targetTimeInCurrentRound < nowMicros)
            targetTime += totalRoundDuration;

        return targetTime - nowMicros;
    }
}