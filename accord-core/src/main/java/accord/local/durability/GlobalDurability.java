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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Scheduler;
import accord.coordinate.Timeout;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.GetDurableBefore;
import accord.messages.GetDurableBefore.DurableBeforeReply;
import accord.messages.SetGloballyDurable;
import accord.topology.Topology;
import accord.utils.Invariants;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class GlobalDurability implements Callback<Object>
{
    private static final Logger logger = LoggerFactory.getLogger(GlobalDurability.class);

    static class CountingSelector implements Function<Topology, Node.Id>
    {
        final Node.Id self;
        int counter;
        CountingSelector(Node.Id self)
        {
            this.self = self;
        }

        @Override
        public Node.Id apply(Topology topology)
        {
            counter = (counter + 1) & Integer.MAX_VALUE;
            int offset = topology.nodes().indexOf(self);
            return topology.nodes().get((counter + offset) % topology.nodes().size());
        }
    }

    private final Node node;
    private Scheduler.Scheduled scheduled;

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
    private Function<Topology, Node.Id> selectSendTo;
    private Function<Topology, Node.Id> selectReadFrom;

    private Topology currentGlobalTopology;
    private int globalIndex;
    private int inflight;
    private int maxConcurrency = 8;
    private volatile boolean stop;

    public GlobalDurability(Node node)
    {
        this.node = node;
        this.selectReadFrom = new CountingSelector(node.id());
        this.selectSendTo = new CountingSelector(node.id());
    }

    public void setGlobalCycleTime(long globalCycleTime, TimeUnit units)
    {
        this.globalCycleTimeMicros = units.toMicros(globalCycleTime);
    }

    public void setSelectSendTo(Function<Topology, Node.Id> newSelectSendTo)
    {
        this.selectSendTo = newSelectSendTo;
    }

    public void setSelectReadFrom(Function<Topology, Node.Id> newSelectReadFrom)
    {
        this.selectReadFrom = newSelectReadFrom;
    }

    /**
     * Schedule regular invocations of CoordinateShardDurable and CoordinateGloballyDurable
     */
    synchronized void start()
    {
        Invariants.require(!stop); // cannot currently restart safely
        long nowMicros = node.elapsed(MICROSECONDS);
        long scheduleAt = computeNextGlobalSyncTime(nowMicros);
        scheduled = node.scheduler().selfRecurring(this::run, scheduleAt - nowMicros, MICROSECONDS);
    }

    public synchronized void stop()
    {
        if (scheduled != null)
            scheduled.cancel();
        stop = true;
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
            if (currentGlobalTopology == null || currentGlobalTopology.isEmpty())
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

            AgentExecutor executor = node.commandStores().someExecutor();
            if (executor == null)
                return;

            synchronized (this)
            {
                if (inflight >= maxConcurrency)
                    return;
                inflight += 2;
            }

            node.send(selectSendTo.apply(currentGlobalTopology), new SetGloballyDurable(node.durableBefore()), executor, this);
            node.send(selectReadFrom.apply(currentGlobalTopology), new GetDurableBefore(), executor, this);
        }
        catch (Exception e)
        {
            logger.warn("Exception invoking withEpoch to start coordination for global durability", e);
        }
    }

    synchronized void updateTopology(Topology latestGlobal)
    {
        if (currentGlobalTopology != null && latestGlobal.epoch() <= currentGlobalTopology.epoch())
            return;

        currentGlobalTopology = latestGlobal;
        List<Node.Id> ids = new ArrayList<>(latestGlobal.nodes());
        Collections.sort(ids);
        globalIndex = ids.indexOf(node.id());
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
        if (currentGlobalTopology == null || currentGlobalTopology.nodes().isEmpty())
            return nowMicros + globalCycleTimeMicros;

        // How long it takes for all nodes to go once
        long totalRoundDuration = currentGlobalTopology.nodes().size() * globalCycleTimeMicros;
        long startOfCurrentRound = (nowMicros / totalRoundDuration) * totalRoundDuration;

        // In a given round at what time in the round should this node take its turn
        long ourOffsetInRound = globalIndex * globalCycleTimeMicros;

        long targetTimeInCurrentRound = startOfCurrentRound + ourOffsetInRound;
        long targetTime = targetTimeInCurrentRound;
        // If our time to run in the current round already passed then schedule it in the next round
        if (targetTimeInCurrentRound <= nowMicros)
            targetTime += totalRoundDuration;

        return targetTime;
    }

    @Override
    public synchronized void onSuccess(Node.Id from, Object reply)
    {
        --inflight;
        if (reply instanceof DurableBeforeReply)
            node.markDurable(((DurableBeforeReply)reply).durableBeforeMap);
    }

    @Override
    public synchronized void onFailure(Node.Id from, Throwable failure)
    {
        --inflight;
        if (failure instanceof Timeout) logger.warn("Failed to fetch DurableBefore from {} due to timeout", from);
        else logger.warn("Failed to fetch DurableBefore from {}", from, failure);
    }

    public synchronized void setMaxConcurrency(int newMaxConcurrency)
    {
        this.maxConcurrency = newMaxConcurrency;
    }

}