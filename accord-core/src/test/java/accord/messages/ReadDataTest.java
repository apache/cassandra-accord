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

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

import accord.Utils;
import accord.api.Data;
import accord.api.MessageSink;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Update;
import accord.api.Write;
import accord.impl.IntKey;
import accord.impl.TopologyFactory;
import accord.impl.mock.MockCluster;
import accord.local.CheckedCommands;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResults;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.utils.Utils.listOf;
import static org.mockito.ArgumentMatchers.any;

class ReadDataTest
{
    private static final Node.Id ID1 = id(1);
    private static final Node.Id ID2 = id(2);
    private static final Node.Id ID3 = id(3);
    private static final List<Node.Id> IDS = listOf(ID1, ID2, ID3);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 3, IntKey.range(0, 100));
    private static final Topologies TOPOLOGIES = Utils.topologies(TOPOLOGY);

    @Test
    public void statusChangeBeforeAck()
    {
        MessageSink sink = Mockito.mock(MessageSink.class);
        ReplyContext replyContext = Mockito.mock(ReplyContext.class);
        Node node = createNode(ID1, TOPOLOGY, sink, new MockCluster.Clock(100));

        TxnId txnId = node.nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
        Ranges ranges = Ranges.single(IntKey.range(0, 1000));
        IntKey.Raw key = IntKey.key(42);
        Keys keys = Keys.of(key);

        AsyncResults.SettableResult<Data> readResult = new AsyncResults.SettableResult<>();
        Data data = Mockito.mock(Data.class);

        Read read = Mockito.mock(Read.class);
        Mockito.when(read.slice(any())).thenReturn(read);
        Mockito.when(read.merge(any())).thenReturn(read);
        Mockito.when(read.read(any(), any(), any(), any(), any())).thenAnswer(new Answer<AsyncChain<Data>>()
        {
            private boolean called = false;
            @Override
            public AsyncChain<Data> answer(InvocationOnMock ignore) throws Throwable
            {
                if (called) throw new IllegalStateException("Multiple calls");
                return readResult;
            }
        });
        Query query = Mockito.mock(Query.class);
        Update update = Mockito.mock(Update.class);
        Mockito.when(update.slice(any())).thenReturn(update);
        Result result = Mockito.mock(Result.class);

        Txn txn = new Txn.InMemory(keys, read, query, update);
        PartialTxn partialTxn = txn.slice(ranges, true);
        FullRoute<?> route = keys.toRoute(key.toUnseekable());
        PartialRoute<?> partialRoute = route.slice(ranges);

        //TODO ProgressKey must equal HomeKey to get Home shard; home key = key
        RoutingKey progressKey = key.toUnseekable();

        PartialDeps deps = PartialDeps.builder(ranges).build();
        Timestamp executeAt = txnId;

        AsyncChain<Void> writeResult = new AsyncResults.SettableResult<>();
        Write write = Mockito.mock(Write.class);
        Mockito.when(write.apply(any(), any(), any(), any())).thenReturn(writeResult);
        Writes writes = new Writes(executeAt, keys, write);


        CommandStore store = node.commandStores().unsafeForKey(key);
        check(store.execute(PreLoadContext.contextFor(txnId, keys), safe -> {
            CheckedCommands.preaccept(safe, txnId, partialTxn, route, progressKey);
            CheckedCommands.accept(safe, txnId, Ballot.ZERO, partialRoute, partialTxn.keys(), progressKey, executeAt, deps);
            CheckedCommands.commit(safe, txnId, route, progressKey, partialTxn, executeAt, deps);
        }));

        ReadData readData = new ReadData(node.id(), TOPOLOGIES, txnId, keys, txnId);
        readData.process(node, node.id(), replyContext);

        Mockito.verifyNoInteractions(sink);

        check(store.execute(PreLoadContext.contextFor(txnId, keys), safe -> {
            CheckedCommands.apply(safe, txnId, safe.latestEpoch(), route, executeAt, deps, writes, result);
        }));

        readResult.setSuccess(data);

        Mockito.verify(sink).reply(Mockito.eq(node.id()), Mockito.eq(replyContext), Mockito.eq(ReadData.ReadNack.Redundant));
    }

    private void check(AsyncChain<Void> execute)
    {
        try
        {
            AsyncChains.getUninterruptibly(execute);
        }
        catch (ExecutionException e)
        {
            throw new AssertionError(e.getCause());
        }
    }
}