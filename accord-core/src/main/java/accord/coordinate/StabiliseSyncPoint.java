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

import java.util.function.BiConsumer;

import accord.local.Node;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.topology.Topologies;

public class StabiliseSyncPoint<U extends Unseekable> extends Stabilise<SyncPoint<U>>
{
    final CoordinationAdapter<SyncPoint<U>> adapter;
    StabiliseSyncPoint(CoordinationAdapter<SyncPoint<U>> adapter, Node node, Topologies coordinates, Topologies all, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps unstableDeps, BiConsumer<? super SyncPoint<U>, Throwable> callback)
    {
        super(node, coordinates, all, route, txnId, ballot, txn, executeAt, unstableDeps, callback);
        this.adapter = adapter;
    }

    @Override
    protected CoordinationAdapter<SyncPoint<U>> adapter()
    {
        return adapter;
    }
}
