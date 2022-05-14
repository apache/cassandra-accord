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

import accord.api.RoutingKey;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.InformOfTxnId;
import accord.messages.SimpleReply;
import accord.topology.Shard;
import accord.primitives.TxnId;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

public class InformHomeOfTxn extends AsyncFuture<Void> implements Callback<SimpleReply>
{
    final TxnId txnId;
    final RoutingKey homeKey;
    final QuorumShardTracker tracker;
    Throwable failure;

    InformHomeOfTxn(TxnId txnId, RoutingKey homeKey, Shard homeShard)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.tracker = new QuorumShardTracker(homeShard);
    }

    public static Future<Void> inform(Node node, TxnId txnId, RoutingKey homeKey)
    {
        // TODO: we should not need to send the Txn here, but to avoid that we need to support no-ops
        return node.withEpoch(txnId.epoch, () -> {
            Shard homeShard = node.topology().forEpoch(homeKey, txnId.epoch);
            InformHomeOfTxn inform = new InformHomeOfTxn(txnId, homeKey, homeShard);
            node.send(homeShard.nodes, new InformOfTxnId(txnId, homeKey), inform);
            return inform;
        });
    }

    @Override
    public void onSuccess(Id from, SimpleReply reply)
    {
        switch (reply)
        {
            default:
            case Ok:
                if (tracker.success(from))
                    trySuccess(null);
                break;

            case Nack:
                // TODO: stale topology should be impossible right now
                onFailure(from, new StaleTopology());
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (this.failure == null) this.failure = failure;
        else this.failure.addSuppressed(failure);

        // TODO: if we fail and have an incorrect topology, trigger refresh
        if (tracker.failure(from))
            tryFailure(this.failure);
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        tryFailure(failure);
    }
}
