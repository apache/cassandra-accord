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

import accord.api.Result;
import accord.local.Node;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.SortedListMap;

import static accord.api.ProtocolModifiers.Faults;
import static accord.coordinate.CoordinationAdapter.Factory.Step.Continue;

class ProposeTxn extends Propose<Result>
{
    ProposeTxn(Node node, Topologies topologies, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, topologies, ballot, txnId, txn, route, executeAt, deps, callback);
    }

    @Override
    void onAccepted()
    {
        Deps deps = Deps.merge(acceptOks, acceptOks.domainSize(), SortedListMap::getValue, ok -> ok.deps);
        if (!Faults.txnDiscardPreAcceptDeps)
            deps = deps.with(this.deps);

        adapter().stabilise(node, acceptTracker.topologies(), route, ballot, txnId, txn, executeAt, deps, callback);
    }

    protected CoordinationAdapter<Result> adapter()
    {
        return node.coordinationAdapter(txnId, Continue);
    }
}
