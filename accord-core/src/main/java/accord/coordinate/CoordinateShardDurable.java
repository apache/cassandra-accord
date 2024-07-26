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

import accord.coordinate.ExecuteSyncPoint.ExecuteExclusiveSyncPoint;
import accord.coordinate.tracking.AppliedTracker;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.ReadData.ReadReply;
import accord.messages.SetShardDurable;
import accord.messages.WaitUntilApplied;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.utils.async.AsyncResult;

public class CoordinateShardDurable extends ExecuteExclusiveSyncPoint implements Callback<ReadReply>
{
    private CoordinateShardDurable(Node node, SyncPoint<Ranges> exclusiveSyncPoint)
    {
        super(node, exclusiveSyncPoint, AppliedTracker::new);
        addCallback((success, fail) -> {
            if (fail == null)
            {
                node.configService().reportEpochRedundant(syncPoint.keysOrRanges, syncPoint.syncId.epoch());
                node.send(tracker.nodes(), new SetShardDurable(syncPoint));
            }
        });
    }

    public static AsyncResult<SyncPoint<Ranges>> coordinate(Node node, SyncPoint<Ranges> exclusiveSyncPoint)
    {
        CoordinateShardDurable coordinate = new CoordinateShardDurable(node, exclusiveSyncPoint);
        coordinate.start();
        return coordinate;
    }

    protected void start()
    {
        node.send(tracker.nodes(), to -> new WaitUntilApplied(to, tracker.topologies(), syncPoint.syncId, syncPoint.keysOrRanges, syncPoint.syncId.epoch()), this);
    }
}
