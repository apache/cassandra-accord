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

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ConfigurationService;
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

    // TODO (required): cancel or cleanup expired requests - they may become unsatisfiable
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

    public synchronized void start()
    {
        Invariants.require(!started);
        started = true;
        Topology current = node.topology().current();
        shards.updateTopology(current);
        global.updateTopology(current);
        shards.start();
        global.start();
    }

    public synchronized void stop()
    {
        shards.stop();
        global.stop();
    }

    public AsyncResult<Void> close(String requestedBy, Ranges ranges)
    {
        return close(requestedBy, TxnId.NONE, ranges);
    }

    public AsyncResult<Void> close(Object requestedBy, Timestamp minBound, Ranges ranges)
    {
        return submit(new DurabilityRequest(requestedBy, minBound, ranges, SyncLocal.NoLocal, SyncRemote.NoRemote, null, node.elapsed(MICROSECONDS))).result;
    }

    public AsyncResult<Void> sync(Object requestedBy, Ranges ranges, SyncLocal local, SyncRemote remote)
    {
        return sync(requestedBy, TxnId.NONE, ranges, local, remote);
    }

    public AsyncResult<Void> sync(Object requestedBy, Timestamp minBound, Ranges ranges, SyncLocal local, SyncRemote remote)
    {
        return submit(new DurabilityRequest(requestedBy, minBound, ranges, local, remote, null, node.elapsed(MICROSECONDS))).result;
    }

    public AsyncResult<Void> sync(Object requestedBy, Ranges ranges, @Nullable Collection<Node.Id> include, SyncLocal local, SyncRemote remote)
    {
        return sync(requestedBy, TxnId.NONE, ranges, include, local, remote);
    }

    public AsyncResult<Void> sync(Object requestedBy, Timestamp minBound, Ranges ranges, @Nullable Collection<Node.Id> include, SyncLocal local, SyncRemote remote)
    {
        return submit(new DurabilityRequest(requestedBy, minBound, ranges, local, remote, include, node.elapsed(MICROSECONDS))).result;
    }

    private DurabilityRequest submit(DurabilityRequest request)
    {
        synchronized (this)
        {
            requests.add(request);
        }
        logger.info("Requesting durability {}", request);
        shards.request(request);
        return request;
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
            next.result.trySuccess(null);
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