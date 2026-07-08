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

import accord.api.Result.PersistableResult;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.local.Node;
import accord.api.ExclusiveAsyncExecutor;
import accord.messages.Apply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;

public class PersistSyncPoint extends Persist
{
    public PersistSyncPoint(Node node, ExclusiveAsyncExecutor executor, Topologies topologies, TxnId txnId, Ballot ballot, Route<?> sendTo, Txn txn, Timestamp executeAt, Deps deps, Writes writes, PersistableResult result, boolean informDurableOnDone, FullRoute<?> route, Apply.Kind applyKind)
    {
        super(node, executor, topologies, txnId, ballot, sendTo, txn, executeAt, deps, writes, result, route, CoordinationFlags.none(), informDurableOnDone, Apply.FACTORY, applyKind);
    }
}
