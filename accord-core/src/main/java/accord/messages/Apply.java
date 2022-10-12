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

package accord.messages;

import accord.api.Key;
import accord.primitives.Keys;
import accord.utils.ReducingFuture;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Result;
import accord.topology.Topologies;
import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.txn.Writes;
import accord.txn.Txn;
import accord.primitives.TxnId;
import com.google.common.collect.Iterables;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.Collections;
import java.util.List;

import static accord.messages.MessageType.APPLY_REQ;
import static accord.messages.MessageType.APPLY_RSP;

public class Apply extends TxnRequest
{
    public static class SerializationSupport
    {
        public static Apply create(Keys scope, long waitForEpoch, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Deps deps, Writes writes, Result result)
        {
            return new Apply(scope, waitForEpoch, txnId, txn, homeKey, executeAt, deps, writes, result);
        }
    }

    public final TxnId txnId;
    public final Txn txn;
    public final Key homeKey;
    public final Timestamp executeAt;
    public final Deps deps;
    public final Writes writes;
    public final Result result;

    public Apply(Node.Id to, Topologies topologies, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(to, topologies, txn.keys());
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.deps = deps;
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    protected Apply(Keys scope, long waitForEpoch, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(scope, waitForEpoch);
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
        this.deps = deps;
        this.writes = writes;
        this.result = result;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Key progressKey = node.trySelectProgressKey(txnId, txn.keys(), homeKey);
        List<Future<Void>> futures = node.mapLocalSince(this, scope(), executeAt, instance -> instance.command(txnId).apply(txn, homeKey, progressKey, executeAt, deps, writes, result));

        ReducingFuture.reduce(futures, (l, r) -> null).addCallback((unused, failure) -> {
            if (failure == null)
                // TODO: notify coordinator of failures
                // note, we do not also commit here if txnId.epoch != executeAt.epoch, as the scope() for a commit would be different
                node.reply(replyToNode, replyContext, ApplyOk.INSTANCE);
        });
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Iterables.concat(Collections.singleton(txnId), deps.txnIds());
    }

    @Override
    public Iterable<Key> keys()
    {
        return txn.keys();
    }

    @Override
    public MessageType type()
    {
        return APPLY_REQ;
    }

    public static class ApplyOk implements Reply
    {
        public static final ApplyOk INSTANCE = new ApplyOk();
        public ApplyOk() {}

        @Override
        public String toString()
        {
            return "ApplyOk";
        }

        @Override
        public MessageType type()
        {
            return APPLY_RSP;
        }
    }

    @Override
    public String toString()
    {
        return "Apply{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               ", deps:" + deps +
               ", executeAt:" + executeAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }
}
