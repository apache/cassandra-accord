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

import java.util.Set;

import accord.coordinate.tracking.AppliedTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.ReadData;
import accord.messages.SetLocallyDurable;
import accord.messages.WaitUntilApplied;
import accord.primitives.SyncPoint;
import accord.topology.Topologies;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults.SettableResult;

/**
 *
 */
public class CoordinateShardDurable extends SettableResult<Void> implements Callback<ReadData.ReadReply>
{
    final Node node;
    final Set<Node.Id> nonParticipants;
    final AppliedTracker tracker;
    final SyncPoint exclusiveSyncPoint;

    private CoordinateShardDurable(Node node, SyncPoint exclusiveSyncPoint, Set<Node.Id> nonParticipants)
    {
        Topologies topologies = node.topology().forEpoch(exclusiveSyncPoint.ranges, exclusiveSyncPoint.sourceEpoch());
        this.node = node;
        this.tracker = new AppliedTracker(topologies, nonParticipants);
        this.exclusiveSyncPoint = exclusiveSyncPoint;
        this.nonParticipants = nonParticipants;
    }

    public static AsyncResult<Void> coordinate(Node node, SyncPoint exclusiveSyncPoint, Set<Node.Id> nonParticipants)
    {
        CoordinateShardDurable coordinate = new CoordinateShardDurable(node, exclusiveSyncPoint, nonParticipants);
        coordinate.start();
        return coordinate;
    }

    private void start()
    {
        node.send(tracker.nodes(), to -> new WaitUntilApplied(to, tracker.topologies(), exclusiveSyncPoint.syncId, exclusiveSyncPoint.ranges, exclusiveSyncPoint.syncId.epoch()), this);
    }

    @Override
    public void onSuccess(Node.Id from, ReadData.ReadReply reply)
    {
        if (!reply.isOk())
        {
            switch ((ReadData.ReadNack)reply)
            {
                default: throw new AssertionError("Unhandled: " + reply);

                case NotCommitted:
                    CoordinateSyncPoint.sendApply(node, from, exclusiveSyncPoint.ranges, exclusiveSyncPoint);
                    return;

                case Redundant:
                    tryFailure(new RuntimeException("Unexpected reply"));
                    return;

                case Error:
                    // TODO (required): error propagation
                    tryFailure(new RuntimeException("Unknown error"));
                    return;

                case Invalid:
                    tryFailure(new Invalidated(exclusiveSyncPoint.syncId, exclusiveSyncPoint.homeKey));
                    return;
            }
        }
        else
        {
            if (tracker.recordSuccess(from) == RequestStatus.Success)
            {
                node.configService().reportEpochRedundant(exclusiveSyncPoint.ranges, exclusiveSyncPoint.syncId.epoch());
                node.send(tracker.nodes(), new SetLocallyDurable(exclusiveSyncPoint));
                trySuccess(null);
            }
        }
    }

    @Override
    public void onFailure(Node.Id from, Throwable failure)
    {
        if (tracker.recordFailure(from) == RequestStatus.Failed)
            tryFailure(new Exhausted(exclusiveSyncPoint.syncId, exclusiveSyncPoint.homeKey));
    }

    @Override
    public void onCallbackFailure(Node.Id from, Throwable failure)
    {
        tryFailure(failure);
    }
}
