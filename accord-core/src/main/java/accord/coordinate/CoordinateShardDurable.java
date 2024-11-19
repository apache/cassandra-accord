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

import accord.coordinate.ExecuteSyncPoint.ExecuteExclusive;
import accord.coordinate.tracking.AllTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.ReadData.ReadReply;
import accord.messages.SetShardDurable;
import accord.messages.WaitUntilApplied;
import accord.primitives.Range;
import accord.primitives.SyncPoint;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncResult;

public class CoordinateShardDurable extends ExecuteExclusive implements Callback<ReadReply>
{
    private CoordinateShardDurable(Node node, SyncPoint<Range> exclusiveSyncPoint)
    {
        super(node, exclusiveSyncPoint, AllTracker::new);
        addCallback((success, fail) -> {
            if (fail == null)
            {
                node.configService().reportEpochRedundant(syncPoint.route.toRanges(), syncPoint.syncId.epoch() - 1);
                node.send(tracker.nodes(), new SetShardDurable(syncPoint));
            }
        });
    }

    public static AsyncResult<SyncPoint<Range>> coordinate(Node node, SyncPoint<Range> exclusiveSyncPoint)
    {
        CoordinateShardDurable coordinate = new CoordinateShardDurable(node, exclusiveSyncPoint);
        coordinate.start();
        return coordinate;
    }

    protected void start()
    {
        SortedArrayList<Node.Id> contact = tracker.filterAndRecordFaulty();
        if (contact == null)
        {
            tryFailure(new Exhausted(syncPoint.syncId, syncPoint.route.homeKey(), null));
            return;
        }
        SortedArrayList<Node.Id> allStaleNodes = tracker.topologies().staleNodes();
        SortedArrayList<Node.Id> allCurrentNodes = tracker.topologies().current().nodes();
        SortedArrayList<Node.Id> staleNodes = contact.without(id -> !allStaleNodes.contains(id))
                                                     .without(id -> !allCurrentNodes.contains(id));
        contact = contact.without(staleNodes);
        for (Node.Id id :staleNodes)
        {
            RequestStatus newStatus = tracker.recordSuccess(id);
            Invariants.checkState(newStatus != RequestStatus.Success);
        }
        if (contact == null) tryFailure(new Exhausted(syncPoint.syncId, syncPoint.route.homeKey(), null));
        else node.send(contact, to -> new WaitUntilApplied(to, tracker.topologies(), syncPoint.syncId, syncPoint.route, syncPoint.syncId.epoch()), this);
    }
}
