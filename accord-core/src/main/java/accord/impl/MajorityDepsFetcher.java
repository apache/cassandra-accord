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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Scheduler;
import accord.coordinate.CollectCalculatedDeps;
import accord.coordinate.CoordinationFailed;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.ShardDistributor;
import accord.primitives.FullRoute;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.local.KeyHistory.COMMANDS;
import static accord.local.PreLoadContext.contextFor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Copied from CoordinateDurabilityScheduling
 * TODO (required): deprecate in favour of piggy-backing on exclusive sync points
 */
public class MajorityDepsFetcher
{
    private static final Logger logger = LoggerFactory.getLogger(MajorityDepsFetcher.class);

    private final Node node;

    private int targetShardSplits = 64;
    private long defaultRetryDelayMicros = TimeUnit.SECONDS.toMicros(1);
    private long maxRetryDelayMicros = TimeUnit.MINUTES.toMicros(1);
    private int maxNumberOfSplits = 1 << 10;

    private final Map<Range, ShardScheduler> shardSchedulers = new HashMap<>();

    private class ShardScheduler
    {
        final CommandStore commandStore;
        final Range range;

        long epoch;
        boolean defunct;

        int index;
        int numberOfSplits;
        Scheduler.Scheduled scheduled;
        long retryDelayMicros = defaultRetryDelayMicros;

        private ShardScheduler(CommandStore commandStore, Range range, long epoch)
        {
            this.commandStore = commandStore;
            this.range = range;
            this.numberOfSplits = targetShardSplits;
            this.epoch = epoch;
        }

        void markDefunct()
        {
            this.defunct = true;
        }

        void schedule()
        {
            synchronized (MajorityDepsFetcher.this)
            {
                if (defunct)
                    return;

                long nowMicros = node.elapsed(MICROSECONDS);
                if (retryDelayMicros > defaultRetryDelayMicros)
                    retryDelayMicros = Math.max(defaultRetryDelayMicros, (long) (0.9 * retryDelayMicros));
                scheduleAt(nowMicros, nowMicros);
            }
        }

        void retry()
        {
            synchronized (MajorityDepsFetcher.this)
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
                if (numberOfSplits * 2 <= maxNumberOfSplits)
                {
                    index *= 2;
                    numberOfSplits *= 2;
                }
                scheduleAt(nowMicros, scheduleAt);
            }
        }

        void scheduleAt(long nowMicros, long scheduleAt)
        {
            synchronized (MajorityDepsFetcher.this)
            {
                ShardDistributor distributor = node.commandStores().shardDistributor();
                Range range;
                int nextIndex;
                {
                    int i = index;
                    Range selectRange = null;
                    while (selectRange == null)
                        selectRange = distributor.splitRange(this.range, index, ++i, numberOfSplits);
                    range = selectRange;
                    nextIndex = i;
                }

                Runnable schedule = () -> start(Ranges.of(range), nextIndex);
                if (scheduleAt <= nowMicros) schedule.run();
                else scheduled = node.scheduler().once(schedule, scheduleAt - nowMicros, MICROSECONDS);
            }
        }

        /**
         * The first step for coordinating shard durable is to run an exclusive sync point
         * the result of which can then be used to run
         */
        private void start(Ranges ranges, int nextIndex)
        {
            TxnId id = TxnId.fromValues(epoch - 1, 0, node.id());
            Timestamp before = Timestamp.minForEpoch(epoch);

            node.withEpoch(id.epoch(), (ignored, withEpochFailure) -> {
                if (withEpochFailure != null)
                {
                    // don't wait on epoch failure - we aren't the cause of any problems
                    start(ranges, nextIndex);
                    Throwable wrapped = CoordinationFailed.wrap(withEpochFailure);
                    logger.trace("Exception waiting for epoch before coordinating exclusive sync point for local shard durability, epoch " + id.epoch(), wrapped);
                    node.agent().onUncaughtException(wrapped);
                    return;
                }
                scheduled = null;
                FullRoute<Range> route = (FullRoute<Range>) node.computeRoute(id, ranges);
                logger.debug("Fetching deps to sync epoch {} for range {}", epoch, ranges);
                CollectCalculatedDeps.withCalculatedDeps(node, id, route, route, before, (deps, fail) -> {
                    if (fail != null)
                    {
                        logger.warn("Failed to fetch deps for syncing epoch {} for {}", epoch, ranges, fail);
                        retry();
                    }
                    else
                    {
                        // TODO (correctness) : PreLoadContext only works with Seekables, which doesn't allow mixing Keys and Ranges... But Deps has both Keys AND Ranges!
                        // ATM all known implementations store ranges in-memory, but this will not be true soon, so this will need to be addressed
                        commandStore.execute(contextFor(null, deps.keyDeps.keys(), COMMANDS), safeStore -> {
                            safeStore.registerHistoricalTransactions(deps);
                        }).begin((success, fail2) -> {
                            if (fail2 != null)
                            {
                                retry();
                                logger.warn("Failed to apply deps for syncing epoch {} for range {}", epoch, ranges, fail2);
                            }
                            else
                            {
                                try
                                {
                                    synchronized (MajorityDepsFetcher.this)
                                    {
                                        index = nextIndex;
                                        if (index >= numberOfSplits)
                                        {
                                            logger.info("Successfully fetched majority deps for {} at epoch {}", range, epoch);
                                            defunct = true;
                                            shardSchedulers.remove(range, ShardScheduler.this);
                                        }
                                        else
                                        {
                                            schedule();
                                        }
                                    }
                                }
                                catch (Throwable t)
                                {
                                    retry();
                                    logger.error("Unexpected exception handling durability scheduling callback; starting from scratch", t);
                                }
                            }
                        });
                    }
                });
            });
        }
    }

    public MajorityDepsFetcher(Node node)
    {
        this.node = node;
    }

    public void setTargetShardSplits(int targetShardSplits)
    {
        this.targetShardSplits = targetShardSplits;
    }

    public void setDefaultRetryDelay(long retryDelay, TimeUnit units)
    {
        this.defaultRetryDelayMicros = units.toMicros(retryDelay);
    }

    public void setMaxRetryDelay(long retryDelay, TimeUnit units)
    {
        this.maxRetryDelayMicros = units.toMicros(retryDelay);
    }

    public synchronized void fetchMajorityDeps(CommandStore commandStore, Range range, long epoch)
    {
        ShardScheduler scheduler = shardSchedulers.get(range);
        if (scheduler != null)
        {
            if (scheduler.epoch >= epoch)
                return;
            scheduler.markDefunct();
        }
        scheduler = new ShardScheduler(commandStore, range, epoch);
        scheduler.schedule();
    }

}