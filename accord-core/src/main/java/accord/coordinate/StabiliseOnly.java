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
import accord.api.ExclusiveAsyncExecutor;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

public class StabiliseOnly extends Stabilise<Deps>
{
    StabiliseOnly(Node node, ExclusiveAsyncExecutor executor, Topologies coordinates, Topologies all, Route<?> sendTo, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps unstableDeps, BiConsumer<? super Deps, Throwable> callback)
    {
        super(node, executor, coordinates, all, sendTo, route, txnId, ballot, txn, executeAt, unstableDeps, callback);
    }

    @Override
    protected void onStabilised()
    {
        finishWithSuccess(stabiliseDeps);
    }

    @Override
    protected CoordinationAdapter<Deps> adapter()
    {
        throw new UnsupportedOperationException();
    }
}
