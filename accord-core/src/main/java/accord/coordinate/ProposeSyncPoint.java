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
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

public class ProposeSyncPoint<S extends Seekables<?, ?>> extends Propose<SyncPoint<S>>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ProposeSyncPoint.class);

    // Whether to wait on the dependencies applying globally before returning a result
    final boolean async;
    final Set<Id> fastPathNodes;

    final S keysOrRanges;

    ProposeSyncPoint(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, Deps deps, Timestamp executeAt, BiConsumer<SyncPoint<S>, Throwable> callback, boolean async, Set<Id> fastPathNodes, S keysOrRanges)
    {
        super(node, topologies, ballot, txnId, txn, route, executeAt, deps, callback);
        this.async = async;
        this.fastPathNodes = fastPathNodes;
        this.keysOrRanges = keysOrRanges;
    }

    public static <S extends Seekables<?, ?>> Propose<SyncPoint<S>> proposeSyncPoint(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, Deps deps, Timestamp executeAt, BiConsumer<SyncPoint<S>, Throwable> callback, boolean async, Set<Id> fastPathNodes, S keysOrRanges)
    {
        ProposeSyncPoint proposeSyncPoint = new ProposeSyncPoint(node, topologies, ballot, txnId, txn, route, deps, executeAt, callback, async, fastPathNodes, keysOrRanges);
        proposeSyncPoint.start();
        return proposeSyncPoint;
    }

    @Override
    void onAccepted()
    {
        if (txnId.rw() == ExclusiveSyncPoint)
        {
            Apply.sendMaximal(node, txnId, route, txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null));
            node.configService().reportEpochClosed((Ranges)keysOrRanges, txnId.epoch());
            callback.accept(new SyncPoint<S>(txnId, deps, keysOrRanges, (FullRangeRoute) route, true), null);
        }
        else
        {
            CoordinateSyncPoint.blockOnDeps(node, txn, txnId, route, keysOrRanges, deps, callback, async);
        }
    }
}
