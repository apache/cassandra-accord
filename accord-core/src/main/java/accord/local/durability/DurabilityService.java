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

import accord.api.ConfigurationService;
import accord.api.Timeouts.Timeout;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class DurabilityService implements ConfigurationService.Listener
{
    private static final Logger logger = LoggerFactory.getLogger(DurabilityService.class);

    public enum SyncLocal  { NoLocal, Self }
    public enum SyncRemote { NoRemote, Quorum, All }

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

    public ShardDurability shards()
    {
        return shards;
    }

    public GlobalDurability global()
    {
        return global;
    }

    public void start()
    {
        synchronized (this)
        {
            Invariants.require(!started);
            started = true;
        }
        Topology current = node.topology().current();
        shards.updateTopology(current);
        global.updateTopology(current);
        shards.start();
        global.start();
    }

    public void stop()
    {
        shards.stop();
        global.stop();
        started = false;
    }

    public AsyncResult<Void> close(String requestedBy, Ranges ranges, long timeoutDelay, TimeUnit timeoutUnits)
    {
        return close(requestedBy, TxnId.NONE, ranges, timeoutDelay, timeoutUnits);
    }

    public AsyncResult<Void> close(Object requestedBy, Timestamp minBound, Ranges ranges, long timeoutDelay, TimeUnit timeoutUnits)
    {
        long startedAt = node.elapsed(MICROSECONDS);
        long timeoutAt = startedAt + timeoutUnits.toMicros(timeoutDelay);
        return submit(new DurabilityRequest(requestedBy, minBound, ranges, SyncLocal.NoLocal, SyncRemote.NoRemote, null, startedAt, timeoutAt)).result;
    }

    public AsyncResult<Void> sync(Object requestedBy, Ranges ranges, SyncLocal local, SyncRemote remote, long timeoutDelay, TimeUnit timeoutUnits)
    {
        return sync(requestedBy, TxnId.NONE, ranges, local, remote, timeoutDelay, timeoutUnits);
    }

    public AsyncResult<Void> sync(Object requestedBy, Timestamp minBound, Ranges ranges, SyncLocal local, SyncRemote remote, long timeoutDelay, TimeUnit timeoutUnits)
    {
        long startedAt = node.elapsed(MICROSECONDS);
        long timeoutAt = startedAt + timeoutUnits.toMicros(timeoutDelay);
        return submit(new DurabilityRequest(requestedBy, minBound, ranges, local, remote, null, startedAt, timeoutAt)).result;
    }

    public AsyncResult<Void> sync(Object requestedBy, Ranges ranges, @Nullable Collection<Node.Id> include, SyncLocal local, SyncRemote remote, long timeoutDelay, TimeUnit timeoutUnits)
    {
        return sync(requestedBy, TxnId.NONE, ranges, include, local, remote, timeoutDelay, timeoutUnits);
    }

    public AsyncResult<Void> sync(Object requestedBy, Timestamp minBound, Ranges ranges, @Nullable Collection<Node.Id> include, SyncLocal local, SyncRemote remote, long timeoutDelay, TimeUnit timeoutUnits)
    {
        long startedAt = node.elapsed(MICROSECONDS);
        long timeoutAt = startedAt + timeoutUnits.toMicros(timeoutDelay);
        return submit(new DurabilityRequest(requestedBy, minBound, ranges, local, remote, include, startedAt, timeoutAt)).result;
    }

    private DurabilityRequest submit(DurabilityRequest request)
    {
        register(request);
        logger.info("Requesting durability {}", request);
        shards.request(request);
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
    public AsyncResult<Void> onTopologyUpdate(Topology topology, boolean isLoad, boolean startSync)
    {
        shards.updateTopology(topology);
        global.updateTopology(topology);
        return AsyncResults.success(null);
    }

    @Override
    public void onRemoteSyncComplete(Node.Id node, long epoch)
    {
    }

    @Override
    public void onEpochClosed(Ranges ranges, long epoch)
    {
    }

    @Override
    public void onEpochRetired(Ranges retiredRanges, long epoch)
    {
        // No need to cancel work for ranges that are still active
        if (!node.topology().isFullyRetired(retiredRanges))
            return;

        shards.retireRanges(retiredRanges, epoch);
    }
}