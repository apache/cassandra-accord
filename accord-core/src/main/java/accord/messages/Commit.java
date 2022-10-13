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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import accord.api.Key;
import accord.utils.ReducingFuture;
import accord.local.Node;
import accord.local.Node.Id;
import accord.topology.Topologies;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.Deps;
import accord.txn.Txn;
import accord.primitives.TxnId;
import com.google.common.collect.Iterables;
import org.apache.cassandra.utils.concurrent.Future;

// TODO: CommitOk responses, so we can send again if no reply received? Or leave to recovery?
public class Commit extends ReadData
{
    public static class SerializerSupport
    {
        public static Commit create(Keys scope, long waitForEpoch, TxnId txnId, Txn txn, Deps deps, Key homeKey, Timestamp executeAt, boolean read)
        {
            return new Commit(scope, waitForEpoch, txnId, txn, deps, homeKey, executeAt, read);
        }
    }

    public final boolean read;

    public Commit(Id to, Topologies topologies, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Deps deps, boolean read)
    {
        super(to, topologies, txnId, txn, deps, homeKey, executeAt);
        this.read = read;
    }

    public Commit(Keys scope, long waitForEpoch, TxnId txnId, Txn txn, Deps deps, Key homeKey, Timestamp executeAt, boolean read)
    {
        super(scope, waitForEpoch, txnId, txn, deps, homeKey, executeAt);
        this.read = read;
    }

    // TODO: accept Topology not Topologies
    public static void commitAndRead(Node node, Topologies executeTopologies, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Deps deps, Set<Id> readSet, Callback<ReadReply> callback)
    {
        for (Node.Id to : executeTopologies.nodes())
        {
            boolean read = readSet.contains(to);
            Commit send = new Commit(to, executeTopologies, txnId, txn, homeKey, executeAt, deps, read);
            if (read) node.send(to, send, callback);
            else node.send(to, send);
        }
        if (txnId.epoch != executeAt.epoch)
        {
            Topologies earlierTopologies = node.topology().preciseEpochs(txn, txnId.epoch, executeAt.epoch - 1);
            Commit.commit(node, earlierTopologies, executeTopologies, txnId, txn, homeKey, executeAt, deps);
        }
    }

    public static void commit(Node node, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Deps deps)
    {
        Topologies commitTo = node.topology().preciseEpochs(txn, txnId.epoch, executeAt.epoch);
        for (Node.Id to : commitTo.nodes())
        {
            Commit send = new Commit(to, commitTo, txnId, txn, homeKey, executeAt, deps, false);
            node.send(to, send);
        }
    }

    public static void commit(Node node, Topologies commitTo, Set<Id> doNotCommitTo, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Deps deps)
    {
        for (Node.Id to : commitTo.nodes())
        {
            if (doNotCommitTo.contains(to))
                continue;

            Commit send = new Commit(to, commitTo, txnId, txn, homeKey, executeAt, deps, false);
            node.send(to, send);
        }
    }

    public static void commit(Node node, Topologies commitTo, Topologies appliedTo, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Deps deps)
    {
        // TODO: if we switch to Topology rather than Topologies we can avoid sending commits to nodes that Apply the same
        commit(node, commitTo, Collections.emptySet(), txnId, txn, homeKey, executeAt, deps);
    }

    public static void commitInvalidate(Node node, TxnId txnId, Keys someKeys, Timestamp until)
    {
        Topologies commitTo = node.topology().preciseEpochs(someKeys, txnId.epoch, until.epoch);
        commitInvalidate(node, commitTo, txnId, someKeys);
    }

    public static void commitInvalidate(Node node, Topologies commitTo, TxnId txnId, Keys someKeys)
    {
        for (Node.Id to : commitTo.nodes())
        {
            // TODO (now) confirm somekeys == txnkeys
            Invalidate send = new Invalidate(to, commitTo, txnId, someKeys, someKeys);
            node.send(to, send);
        }
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

    public void process(Node node, Id from, ReplyContext replyContext)
    {
        Key progressKey = node.trySelectProgressKey(txnId, txn.keys(), homeKey);
        List<Future<Void>> futures = node.mapLocal(this, txnId.epoch, executeAt.epoch,
                                                   instance -> instance.command(txnId).commitAndBeginExecution(txn, homeKey, progressKey, executeAt, deps));

        if (read)
        {
            ReducingFuture.reduce(futures, (l, r) -> null).addCallback((unused, throwable) -> {
                if (throwable == null)
                    super.process(node, from, replyContext);
                else
                    node.reply(from, replyContext, new ReadNack());
            });
        }
    }

    @Override
    public MessageType type()
    {
        return MessageType.COMMIT_REQ;
    }

    @Override
    public String toString()
    {
        return "Commit{txnId: " + txnId +
               ", executeAt: " + executeAt +
               ", deps: " + deps +
               ", read: " + read +
               '}';
    }

    public static class Invalidate extends TxnRequest
    {
        public static class SerializerSupport
        {
            public static Invalidate create(Keys scope, long waitForEpoch, TxnId txnId, Keys txnKeys)
            {
                return new Invalidate(scope, waitForEpoch, txnId, txnKeys);
            }
        }

        public final TxnId txnId;
        public final Keys txnKeys;

        public Invalidate(Id to, Topologies topologies, TxnId txnId, Keys txnKeys, Keys someKeys)
        {
            super(to, topologies, someKeys);
            this.txnId = txnId;
            this.txnKeys = txnKeys;
        }

        protected Invalidate(Keys scope, long waitForEpoch, TxnId txnId, Keys txnKeys)
        {
            super(scope, waitForEpoch);
            this.txnId = txnId;
            this.txnKeys = txnKeys;
        }

        @Override
        public Iterable<TxnId> txnIds()
        {
            return Collections.singleton(txnId);
        }

        @Override
        public Iterable<Key> keys()
        {
            return Collections.emptyList();
        }

        public void process(Node node, Id from, ReplyContext replyContext)
        {
            node.forEachLocal(this, txnId.epoch, instance -> instance.command(txnId).commitInvalidate());
        }

        @Override
        public MessageType type()
        {
            return MessageType.COMMIT_REQ;
        }

        @Override
        public String toString()
        {
            return "CommitInvalidate{txnId: " + txnId + '}';
        }
    }
}
