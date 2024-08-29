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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import accord.Utils;
import accord.api.Data;
import accord.api.Key;
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
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SaveStatus;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResults;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Utils.listOf;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static org.mockito.ArgumentMatchers.any;

class ReadDataTest
{
    private static final Node.Id ID1 = id(1);
    private static final Node.Id ID2 = id(2);
    private static final Node.Id ID3 = id(3);
    private static final List<Node.Id> IDS = listOf(ID1, ID2, ID3);
    private static final Range RANGE = IntKey.range(0, 100);
    private static final Ranges RANGES = Ranges.single(RANGE);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 3, RANGE);
    private static final Topologies TOPOLOGIES = Utils.topologies(TOPOLOGY);

    private void test(Consumer<State> fn)
    {
        MessageSink sink = Mockito.mock(MessageSink.class);
        Node node = createNode(ID1, TOPOLOGY, sink, new MockCluster.Clock(100));

        TxnId txnId = node.nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
        Keys keys = Keys.of(IntKey.key(1), IntKey.key(43));

        AsyncResults.SettableResult<Data> readResult = new AsyncResults.SettableResult<>();

        Read read = Mockito.mock(Read.class);
        Mockito.when(read.intersecting(any())).thenReturn(read);
        Mockito.when(read.merge(any())).thenReturn(read);
        Mockito.when(read.keys()).thenReturn((Seekables)keys);
        Mockito.when(read.read(any(), any(), any(), any())).thenAnswer(new Answer<AsyncChain<Data>>()
        {
            private final boolean called = false;
            @Override
            public AsyncChain<Data> answer(InvocationOnMock ignore) throws Throwable
            {
                if (called) throw illegalState("Multiple calls");
                return readResult;
            }
        });
        Query query = Mockito.mock(Query.class);
        Update update = Mockito.mock(Update.class);
        Mockito.when(update.intersecting(any())).thenReturn(update);

        Txn txn = new Txn.InMemory(keys, read, query, update);
        PartialTxn partialTxn = txn.intersecting(RANGES, true);

        fn.accept(new State(node, sink, txnId, partialTxn, readResult));
    }

    @Test
    public void readyToExecuteObsoleteFromTracker()
    {
        // status=ReadyToExecute, so read will happen right away; obsolete marked by ObsoleteTracker
        test(state -> {
            state.readyToExecute();

            ReplyContext replyContext = state.process();
            Mockito.verifyNoInteractions(state.sink);

            state.apply();
            state.readResult.setSuccess(Mockito.mock(Data.class));
            Mockito.verify(state.sink).reply(Mockito.eq(state.node.id()), Mockito.eq(replyContext), Mockito.eq(ReadData.CommitOrReadNack.Redundant));
        });
    }

    @Test
    public void commitObsoleteFromTracker()
    {
        // status=Commit, will listen waiting for ReadyToExecute; obsolete marked by status listener
        test(state -> {
            state.forEach(store -> check(store.execute(PreLoadContext.contextFor(state.txnId, state.keys), safe -> {
                CheckedCommands.preaccept(safe, state.txnId, state.partialTxn, state.route, state.progressKey);
                CheckedCommands.accept(safe, state.txnId, Ballot.ZERO, state.partialRoute, state.partialTxn.keys(), state.progressKey, state.executeAt, state.deps);

                SafeCommand safeCommand = safe.ifInitialised(state.txnId);
                safeCommand.stable(safe, safeCommand.current(), Ballot.ZERO, state.executeAt, Command.WaitingOn.empty(state.txnId.domain()));
            })));

            ReplyContext replyContext = state.process();

            Mockito.verifyNoInteractions(state.sink);

            state.apply();
            state.readResult.setSuccess(Mockito.mock(Data.class));

            Mockito.verify(state.sink).reply(Mockito.eq(state.node.id()), Mockito.eq(replyContext), Mockito.eq(ReadData.CommitOrReadNack.Redundant));
        });
    }

    @Test
    public void mapReduceMarksObsolete()
    {
        // status=Commit, will listen waiting for ReadyToExecute; obsolete marked by status listener
        test(state -> {
            List<CommandStore> stores = stores(state);
            // this test is a bit implementation specific... so if implementations change this may need an update
            // since mapReduceConsume walks the store in id order, by making sure the stores involved in this test
            // are in the "right" order, can make sure to hit a very specific edge case
            Collections.sort(stores, Comparator.comparingInt(CommandStore::id));
            CommandStore store = stores.get(0);

            // ack doesn't get called due to waitingOnCount not being -1, can only happen once
            // the process command completes
            state.readResult.setSuccess(Mockito.mock(Data.class));
            state.readyToExecute(store);

            store = stores.get(1);
            check(store.execute(PreLoadContext.contextFor(state.txnId, state.keys), safe -> {
                SafeCommand command = safe.get(state.txnId, state.txnId, state.route);
                command.commitInvalidated(safe);
            }));

            ReplyContext replyContext = state.process();

            Mockito.verify(state.sink).reply(Mockito.eq(state.node.id()), Mockito.eq(replyContext), Mockito.eq(ReadData.CommitOrReadNack.Redundant));
        });
    }

    @Test
    public void mapReduceAllStageMarksObsolete()
    {
        test(state -> {
            List<CommandStore> stores = stores(state);
            stores.forEach(store -> check(store.execute(PreLoadContext.contextFor(state.txnId, state.keys), safe -> {
                SafeCommand command = safe.get(state.txnId, state.txnId, state.route);
                command.commitInvalidated(safe);
            })));
            ReplyContext replyContext = state.process();

            Mockito.verify(state.sink).reply(Mockito.eq(state.node.id()), Mockito.eq(replyContext), Mockito.eq(ReadData.CommitOrReadNack.Redundant));
        });
    }

    private static List<CommandStore> stores(State state)
    {
        List<CommandStore> stores = new ArrayList<>(2);
        state.forEach(stores::add);
        Assertions.assertThat(stores).hasSize(2);
        // block duplicate stores
        Map<Integer, Long> counts = stores.stream().map(CommandStore::id).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (Map.Entry<Integer, Long> e : counts.entrySet())
        {
            if (e.getValue() == 1) continue;
            throw new AssertionError("Duplicate command store detected with id: " + e.getKey());
        }
        return stores;
    }

    private static void check(AsyncChain<Void> execute)
    {
        try
        {
            getUninterruptibly(execute);
        }
        catch (ExecutionException e)
        {
            throw new AssertionError(e.getCause());
        }
    }

    private static class State
    {
        private final Node node;
        private final MessageSink sink;
        private final TxnId txnId;
        private final PartialTxn partialTxn;
        private final Keys keys;
        private final Key key;
        private final FullRoute<?> route;
        private final PartialRoute<?> partialRoute;
        private final RoutingKey progressKey;
        private final Timestamp executeAt;
        private final PartialDeps deps;
        private final AsyncResults.SettableResult<Data> readResult;

        State(Node node, MessageSink sink, TxnId txnId, PartialTxn partialTxn, AsyncResults.SettableResult<Data> readResult)
        {
            this.node = node;
            this.sink = sink;
            this.txnId = txnId;
            this.partialTxn = partialTxn;
            this.keys = (Keys) partialTxn.keys();
            this.key = keys.get(0);
            this.route = keys.toRoute(key.toUnseekable());
            this.partialRoute = route.slice(RANGES);
            this.progressKey = key.toUnseekable();
            this.executeAt = txnId;
            this.deps = PartialDeps.builder(partialRoute).build();
            this.readResult = readResult;
        }

        void readyToExecute(CommandStore store)
        {
            check(store.execute(PreLoadContext.contextFor(txnId, keys), safe -> {
                CheckedCommands.preaccept(safe, txnId, partialTxn, route, progressKey);
                CheckedCommands.accept(safe, txnId, Ballot.ZERO, partialRoute, partialTxn.keys(), progressKey, executeAt, deps);
                CheckedCommands.commit(safe, SaveStatus.Stable, Ballot.ZERO, txnId, route, progressKey, partialTxn, executeAt, deps);
            }));
        }

        void readyToExecute()
        {
            forEach(this::readyToExecute);
        }

        private void forEach(Consumer<CommandStore> fn)
        {
            keys.stream().map(node.commandStores()::unsafeForKey).distinct().forEach(fn);
        }

        AsyncResults.SettableResult<Void> apply()
        {
            AsyncResults.SettableResult<Void> writeResult = new AsyncResults.SettableResult<>();
            Write write = Mockito.mock(Write.class);
            Mockito.when(write.apply(any(), any(), any(), any(), any())).thenReturn(writeResult);
            Writes writes = new Writes(txnId, executeAt, keys, write);

            forEach(store -> check(store.execute(PreLoadContext.contextFor(txnId, keys), safe -> {
                CheckedCommands.apply(safe, txnId, route, progressKey, executeAt, deps, partialTxn, writes, Mockito.mock(Result.class));
            })));
            return writeResult;
        }

        ReplyContext process()
        {
            ReplyContext replyContext = Mockito.mock(ReplyContext.class);
            ReadData readData = new ReadTxnData(node.id(), TOPOLOGIES, txnId, keys.toParticipants(), txnId.epoch());
            readData.process(node, node.id(), replyContext);
            return replyContext;
        }
    }
}