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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import accord.coordinate.tracking.QuorumTracker;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.GetDeps;
import accord.messages.GetDeps.GetDepsOk;
import accord.primitives.*;
import accord.topology.Topologies;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;

public class CollectDeps implements Callback<GetDepsOk>
{
    final Node node;
    final TxnId txnId;
    final Route<?> route;

    final Timestamp executeAt;

    private final List<GetDepsOk> oks;
    private final QuorumTracker tracker;
    private final BiConsumer<Deps, Throwable> callback;
    private boolean isDone;

    CollectDeps(Node node, Topologies topologies, TxnId txnId, FullRoute<?> route, Seekables<?, ?> keysOrRanges, Timestamp executeAt, BiConsumer<Deps, Throwable> callback)
    {
        this.node = node;
        this.txnId = txnId;
        this.route = route;
        this.executeAt = executeAt;
        this.callback = callback;
        this.oks = new ArrayList<>();
        this.tracker = new QuorumTracker(topologies);
    }

    public static void withDeps(Node node, TxnId txnId, FullRoute<?> route, Seekables<?, ?> keysOrRanges, Timestamp executeAt, BiConsumer<Deps, Throwable> callback)
    {
        Topologies topologies = node.topology().withUnsyncedEpochs(route, txnId, executeAt);
        CollectDeps collect = new CollectDeps(node, topologies, txnId, route, keysOrRanges, executeAt, callback);
        CommandStore store = CommandStore.maybeCurrent();
        if (store == null)
            store = node.commandStores().select(route);
        node.send(collect.tracker.nodes(), to -> new GetDeps(to, topologies, route, txnId, keysOrRanges, executeAt),
                  store, collect);
    }

    @Override
    public void onSuccess(Id from, GetDepsOk ok)
    {
        if (isDone)
            return;

        oks.add(ok);
        if (tracker.recordSuccess(from) == Success)
            onQuorum();
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        if (tracker.recordFailure(from) == Failed)
        {
            isDone = true;
            callback.accept(null, new Timeout(txnId, route.homeKey()));
        }
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        isDone = true;
        callback.accept(null, failure);
    }

    private void onQuorum()
    {
        if (isDone)
            return;

        isDone = true;
        Deps deps = Deps.merge(oks, ok -> ok.deps);
        callback.accept(deps, null);
    }
}
