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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.api.ExclusiveAsyncExecutor;
import accord.messages.Accept;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

public class ProposeSyncPoint<R> extends Propose<R>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ProposeSyncPoint.class);
    private final CoordinationAdapter<R> adapter;

    ProposeSyncPoint(CoordinationAdapter<R> adapter, Node node, ExclusiveAsyncExecutor executor, Topologies topologies, FullRoute<?> route, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
    {
        super(node, executor, topologies, kind, ballot, txnId, txn, route, route, executeAt, deps, callback);
        this.adapter = adapter;
    }

    @Override
    CoordinationAdapter<R> adapter()
    {
        return adapter;
    }
}
