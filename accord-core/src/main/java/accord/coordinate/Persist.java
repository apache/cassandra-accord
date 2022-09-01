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

import java.util.HashSet;
import java.util.Set;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Apply;
import accord.messages.Apply.ApplyOk;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.InformOfPersistence;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.txn.Txn;
import accord.primitives.TxnId;
import accord.txn.Writes;

// TODO: do not extend AsyncFuture, just use a simple BiConsumer callback
public class Persist implements Callback<ApplyOk>
{
    final Node node;
    final TxnId txnId;
    final Key homeKey;
    final Timestamp executeAt;
    final QuorumTracker tracker;
    final Set<Id> persistedOn;
    boolean isDone;

    public static void persist(Node node, Topologies topologies, TxnId txnId, Key homeKey, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Persist persist = new Persist(node, topologies, txnId, homeKey, executeAt);
        node.send(topologies.nodes(), to -> new Apply(to, topologies, txnId, txn, homeKey, executeAt, deps, writes, result), persist);
    }

    public static void persistAndCommit(Node node, TxnId txnId, Key homeKey, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Topologies persistTo = node.topology().preciseEpochs(txn, executeAt.epoch);
        Persist persist = new Persist(node, persistTo, txnId, homeKey, executeAt);
        node.send(persistTo.nodes(), to -> new Apply(to, persistTo, txnId, txn, homeKey, executeAt, deps, writes, result), persist);
        if (txnId.epoch != executeAt.epoch)
        {
            Topologies earlierTopologies = node.topology().preciseEpochs(txn, txnId.epoch, executeAt.epoch - 1);
            Commit.commit(node, earlierTopologies, persistTo, txnId, txn, homeKey, executeAt, deps);
        }
    }

    private Persist(Node node, Topologies topologies, TxnId txnId, Key homeKey, Timestamp executeAt)
    {
        this.node = node;
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.tracker = new QuorumTracker(topologies);
        this.executeAt = executeAt;
        this.persistedOn = new HashSet<>();
    }

    @Override
    public void onSuccess(Id from, ApplyOk response)
    {
        persistedOn.add(from);
        if (tracker.success(from) && !isDone)
        {
            // TODO: send to non-home replicas also, so they may clear their log more easily?
            Shard homeShard = node.topology().forEpochIfKnown(homeKey, txnId.epoch);
            node.send(homeShard, new InformOfPersistence(txnId, homeKey, executeAt, persistedOn));
            isDone = true;
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        // TODO: send knowledge of partial persistence?
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
    }
}
