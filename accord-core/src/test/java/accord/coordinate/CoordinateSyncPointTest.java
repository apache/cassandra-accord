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

import accord.Utils;
import accord.api.MessageSink;
import accord.coordinate.tracking.AppliedTracker;
import accord.impl.IntKey;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.PreAccept;
import accord.messages.ReadData;
import accord.messages.Request;
import accord.primitives.PartialDeps;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.topology.Topology;
import accord.topology.TopologyUtils;
import accord.utils.async.AsyncChains;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

class CoordinateSyncPointTest
{
    static final Node.Id N1 = new Node.Id(1);
    static final Node.Id N2 = new Node.Id(2);
    static final List<Node.Id> ALL = Arrays.asList(N1, N2);

    @Test
    void rangeMovedOffNode()
    {
        Topology t1 = TopologyUtils.initialTopology(ALL, Ranges.single(IntKey.range(0, 100)), ALL.size());
        Range removed = IntKey.range(10, 100);
        Topology t2 = new Topology(t1.epoch() + 1,
                                   Utils.shard(IntKey.range(0, 10), ALL),
                                   Utils.shard(removed, Arrays.asList(N2)));

        Node n1 = Utils.createNode(N1, t1, happyPathMessaging(), new MockCluster.Clock(0), new TestAgent.RethrowAgent());
        n1.topology().onTopologyUpdate(t2, () -> null);
        for (Node.Id node : ALL)
            n1.topology().onEpochSyncComplete(node, t1.epoch());

        awaitApplied(n1, removed);
    }

    @Test
    void rangeRemovedGlobally()
    {
        Topology t1 = TopologyUtils.initialTopology(ALL, Ranges.single(IntKey.range(0, 100)), ALL.size());
        Range removed = IntKey.range(10, 100);
        Topology t2 = new Topology(t1.epoch() + 1,
                                   Utils.shard(IntKey.range(0, 10), ALL));

        Node n1 = Utils.createNode(N1, t1, happyPathMessaging(), new MockCluster.Clock(0), new TestAgent.RethrowAgent());
        n1.topology().onTopologyUpdate(t2, () -> null);
        for (Node.Id node : ALL)
            n1.topology().onEpochSyncComplete(node, t1.epoch());

        awaitApplied(n1, removed);
    }

    private static SyncPoint<Ranges> awaitApplied(Node node, Range removed)
    {
        var await = CoordinateSyncPoint.exclusive(node, Ranges.single(removed))
                                       .flatMap(syncPoint ->
                                                        // the test uses an executor that runs everything right away, so this gets called outside the CommandStore
                                                        node.commandStores().forId(0).submit(() -> {
                                                            ExecuteSyncPoint.ExecuteExclusiveSyncPoint execute = new ExecuteSyncPoint.ExecuteExclusiveSyncPoint(node, syncPoint, AppliedTracker::new);
                                                            execute.start();
                                                            return execute;
                                                        })
                                               ).flatMap(Function.identity());

        return AsyncChains.getUnchecked(await);
    }

    private static MessageSink happyPathMessaging()
    {
        MessageSink msg = Mockito.mock(MessageSink.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doAnswer(args -> {
            Node.Id to = args.getArgument(0);
            Request request = args.getArgument(1);
            AgentExecutor executor = args.getArgument(2);

            if (request instanceof PreAccept)
            {
                PreAccept preAccept = (PreAccept) request;
                PreAccept.PreAcceptReply reply = new PreAccept.PreAcceptOk(preAccept.txnId, preAccept.txnId, PartialDeps.NONE);
                Callback<PreAccept.PreAcceptReply> cb = args.getArgument(3);
                executor.execute(() -> cb.onSuccess(to, reply));
            }
            else if (request instanceof Accept)
            {
                Accept accept = (Accept) request;
                Accept.AcceptReply reply = new Accept.AcceptReply(PartialDeps.NONE);
                Callback<Accept.AcceptReply> cb = args.getArgument(3);
                executor.execute(() -> cb.onSuccess(to, reply));
            }
            else if (request instanceof Commit)
            {
                Commit commit = (Commit) request;
                ReadData.ReadOk reply = new ReadData.ReadOk(null, null);
                Callback<ReadData.ReadOk> cb = args.getArgument(3);
                executor.execute(() -> cb.onSuccess(to, reply));
            }
            else if (request instanceof Apply)
            {
                Apply apply = (Apply) request;
                Apply.ApplyReply reply = Apply.ApplyReply.Applied;
                Callback<Apply.ApplyReply> cb = args.getArgument(3);
                executor.execute(() -> cb.onSuccess(to, reply));
            }
            else
            {
                throw new AssertionError("Unexpected request: " + request);
            }
            return null;
        }).when(msg).send(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        return msg;
    }
}