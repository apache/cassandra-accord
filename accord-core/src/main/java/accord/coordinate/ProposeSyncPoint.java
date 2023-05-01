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
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Apply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

public class ProposeSyncPoint extends Propose<SyncPoint>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ProposeSyncPoint.class);

    // Whether to wait on the dependencies applying globally before returning a result
    final boolean async;
    final Set<Id> fastPathNodes;

    ProposeSyncPoint(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, Deps deps, Timestamp executeAt, BiConsumer<SyncPoint, Throwable> callback, boolean async, Set<Id> fastPathNodes)
    {
        super(node, topologies, ballot, txnId, txn, route, deps, executeAt, callback);
        this.async = async;
        this.fastPathNodes = fastPathNodes;
    }

    public static Propose<SyncPoint> proposeSyncPoint(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, Deps deps, Timestamp executeAt, BiConsumer<SyncPoint, Throwable> callback, boolean async, Set<Id> fastPathNodes)
    {
        ProposeSyncPoint proposeSyncPoint = new ProposeSyncPoint(node, topologies, ballot, txnId, txn, route, deps, executeAt, callback, async, fastPathNodes);
        proposeSyncPoint.start();
        return proposeSyncPoint;
    }

    @Override
    void onAccepted()
    {
        if (txnId.rw() == ExclusiveSyncPoint)
        {
            // TODO I changed this to acceptTracker instead of the FastPathTracker from CoordinateSyncPoint, is that safe/correct?
            // Looks to be fine
            node.send(acceptTracker.nodes(), id -> new Apply(id, acceptTracker.topologies(), acceptTracker.topologies(), txnId.epoch(), txnId, route, txn, txnId, deps, txn.execute(txnId, null, null), txn.result(txnId, txnId, null)));
            callback.accept(new SyncPoint(txnId, route.homeKey(), deps, true), null);
        }
        else
        {
            // If deps are empty there is nothing to wait on application for so we can return immediately
            boolean processAsyncCompletion = deps.isEmpty() || async;
            BlockOnDeps.blockOnDeps(node, txnId, txn, route, deps, (result, throwable) -> {
                // Don't want to process completion twice
                if (processAsyncCompletion)
                {
                    // Don't lose the error
                    if (throwable != null)
                        node.agent().onUncaughtException(throwable);
                    return;
                }
                if (throwable != null)
                    callback.accept(null, throwable);
                else
                    callback.accept(new SyncPoint(txnId, route.homeKey(), deps, false), null);
            });
            // Notify immediately and the caller can add a listener to command completion to track local application
            if (processAsyncCompletion)
                callback.accept(new SyncPoint(txnId, route.homeKey(), deps, true), null);
        }
    }
}
