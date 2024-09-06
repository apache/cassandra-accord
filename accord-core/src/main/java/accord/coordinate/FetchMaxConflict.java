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

package accord.coordinate;

import java.util.Collection;

import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.GetMaxConflict;
import accord.messages.GetMaxConflict.GetMaxConflictOk;
import accord.primitives.FullRoute;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.topology.Topologies;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class FetchMaxConflict extends AbstractCoordinatePreAccept<Timestamp, GetMaxConflictOk>
{
    final QuorumTracker tracker;
    final Seekables<?, ?> keysOrRanges;
    Timestamp maxConflict;
    long executionEpoch;

    private FetchMaxConflict(Node node, FullRoute<?> route, Seekables<?, ?> keysOrRanges, long executionEpoch)
    {
        this(node, route, keysOrRanges, executionEpoch, node.topology().withUnsyncedEpochs(route, executionEpoch, executionEpoch));
    }

    private FetchMaxConflict(Node node, FullRoute<?> route, Seekables<?, ?> keysOrRanges, long executionEpoch, Topologies topologies)
    {
        super(node, route, null, topologies);
        this.keysOrRanges = keysOrRanges;
        this.maxConflict = Timestamp.NONE;
        this.executionEpoch = executionEpoch;
        this.tracker = new QuorumTracker(topologies);
    }

    public static AsyncResult<Timestamp> fetchMaxConflict(Node node, Seekables<?, ?> keysOrRanges)
    {
        long epoch = node.epoch();
        FullRoute<?> route = node.computeRoute(epoch, keysOrRanges);
        // TODO (required): need to ensure we permanently fail any bootstrap that is now impossible and mark as stale
        TopologyMismatch mismatch = TopologyMismatch.checkForMismatch(node.topology().globalForEpoch(epoch), null, route.homeKey(), keysOrRanges);
        if (mismatch != null)
            return AsyncResults.failure(mismatch);
        FetchMaxConflict coordinate = new FetchMaxConflict(node, route, keysOrRanges, epoch);
        coordinate.start();
        return coordinate;
    }


    @Override
    Seekables<?, ?> keysOrRanges()
    {
        return keysOrRanges;
    }

    @Override
    void contact(Collection<Node.Id> nodes, Topologies topologies, Callback<GetMaxConflictOk> callback)
    {
        node.send(nodes, to -> new GetMaxConflict(to, topologies, route, keysOrRanges, executionEpoch), callback);
    }

    @Override
    void onSuccessInternal(Node.Id from, GetMaxConflictOk reply)
    {
        maxConflict = Timestamp.max(reply.maxConflict, maxConflict);
        executionEpoch = Math.max(executionEpoch, reply.latestEpoch);

        if (tracker.recordSuccess(from) == Success)
            onPreAcceptedOrNewEpoch();
    }

    @Override
    boolean onExtraSuccessInternal(Node.Id from, GetMaxConflictOk reply)
    {
        maxConflict = Timestamp.max(reply.maxConflict, maxConflict);
        return true;
    }

    @Override
    void onFailureInternal(Node.Id from, Throwable failure)
    {
        if (tracker.recordFailure(from) == Failed)
            tryFailure(failure);
    }

    @Override
    void onNewEpochTopologyMismatch(TopologyMismatch mismatch)
    {
        tryFailure(mismatch);
    }

    @Override
    long executeAtEpoch()
    {
        return executionEpoch;
    }

    @Override
    void onPreAccepted(Topologies topologies)
    {
        setSuccess(maxConflict);
    }
}
