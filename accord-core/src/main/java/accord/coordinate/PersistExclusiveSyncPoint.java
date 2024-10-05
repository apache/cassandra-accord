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

import accord.api.Result;
import accord.local.Node;
import accord.messages.Apply;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.SortedArrays;

public class PersistExclusiveSyncPoint extends Persist
{
    public PersistExclusiveSyncPoint(Node node, Topologies topologies, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(node, topologies, txnId, route, txn, executeAt, deps, writes, result);
    }

    @Override
    public void start(Apply.Factory factory, Apply.Kind kind, Topologies all, Writes writes, Result result)
    {
        factory = Apply.wrapForExclusiveSyncPoint(factory);
        SortedArrays.SortedArrayList<Node.Id> contact = tracker.filterAndRecordFaulty();
        if (contact == null)
        {
            // TODO (expected): we should report this somewhere?
        }
        else
        {
            for (Node.Id to : contact)
            {
                Apply apply = factory.create(kind, to, all, txnId, route, txn, executeAt, stableDeps, writes, result);
                if (apply == null)
                    tracker.recordSuccess(to);
                else
                    node.send(to, apply, this);
            }
        }
    }
}
