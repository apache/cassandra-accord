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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.impl.IntKey.Raw;
import accord.impl.TopologyFactory;
import accord.impl.mock.MockCluster.Clock;
import accord.impl.mock.Network;
import accord.impl.mock.RecordingMessageSink;
import accord.local.CommandStore;
import accord.local.CommandSummaries.IsDep;
import accord.local.Commands;
import accord.local.ExecutionContext;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.Serialize;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.routing;
import static accord.local.cfk.CommandsForKey.InternalStatus.PREACCEPTED_COORD;
import static accord.local.cfk.CommandsForKey.InternalStatus.PREACCEPTED_COORD_RECOVERY;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.utils.async.AsyncChainUtils.getUninterruptibly;

/**
 * This file was authored by LLM
 *
 * The privileged coordinator records its own state as {@link SaveStatus#PreAcceptedPrivCoord} (whether or not it votes),
 * and once it promises a non-zero ballot {@link SaveStatus#PreAcceptedPrivCoordRecovery}; these must be visible
 * to {@link CommandsForKey} (and survive serialization), without reference to our node id.
 */
public class PrivilegedCoordinatorStatusTest
{
    private static final Id ID1 = id(1), ID2 = id(2), ID3 = id(3), ID4 = id(4), ID5 = id(5);
    private static final List<Id> IDS = List.of(ID1, ID2, ID3, ID4, ID5);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 5, IntKey.range(0, 100));
    private static final Ranges FULL_RANGE = Ranges.single(IntKey.range(routing(Integer.MIN_VALUE), routing(Integer.MAX_VALUE)));

    private static TxnId privileged(TxnId txnId)
    {
        return new TxnId(txnId.epoch(), txnId.hlc(), PrivilegedCoordinatorWithDeps.bits, Write, Key, txnId.node);
    }

    private static <T> T on(CommandStore commandStore, TxnId txnId, Txn txn, Function<SafeCommandStore, T> f)
    {
        try
        {
            return getUninterruptibly(commandStore.submit(ExecutionContext.unsequencedWrite(txnId, txn.keys().toParticipants(), "Test"), f));
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }

    private static List<IsDep> visit(CommandsForKey cfk, TxnId testTxnId)
    {
        List<IsDep> result = new ArrayList<>();
        cfk.visit(testTxnId, testTxnId.witnessedBy(), (keyOrRange, txnId, executeAt, status, dep, durability) -> {
            if (!txnId.equals(testTxnId))
                result.add(dep);
            return true;
        });
        return result;
    }

    private static PreAccept preAccept(TxnId txnId, Txn txn, FullRoute<?> route)
    {
        return PreAccept.SerializerSupport.create(txnId, route.overlapping(FULL_RANGE), txnId.epoch(), txnId.epoch(), txnId.epoch(), txn.slice(FULL_RANGE, true), null, false, route);
    }

    private static SaveStatus preacceptAsCoordinator(CommandStore commandStore, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        return on(commandStore, txnId, txn, safeStore -> {
            StoreParticipants participants = StoreParticipants.update(safeStore, route, txnId.epoch(), txnId, txnId.epoch());
            SafeCommand safeCommand = safeStore.get(txnId, participants);
            Assertions.assertEquals(Commands.AcceptOutcome.Success, Commands.preacceptAsPrivilegedCoordinator(safeStore, safeCommand, participants, txnId, txn.slice(FULL_RANGE, true)));
            Assertions.assertEquals(txnId.equals(safeCommand.current().executeAt()), BeginRecovery.hasPrivilegedVote(txnId, safeCommand.current().saveStatus(), safeCommand.current().executeAt()));
            return safeCommand.current().saveStatus();
        });
    }

    @Test
    public void test()
    {
        RecordingMessageSink sink = new RecordingMessageSink(ID3, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID3, TOPOLOGY, sink, clock);
        try
        {
            Raw key = IntKey.key(10);
            RoutingKey routingKey = key.toUnseekable();
            Txn txn = writeTxn(Keys.of(key));
            FullRoute<?> route = txn.keys().toRoute(routingKey);
            CommandStore commandStore = node.unsafeForKey(routingKey);

            TxnId earlierId = privileged(clock.idForNode(1, ID1));
            clock.increment(10);
            TxnId coordId = privileged(clock.idForNode(1, ID3));
            clock.increment(10);
            TxnId laterId = privileged(clock.idForNode(1, ID1));
            clock.increment(10);

            // pre-accept coordId as its privileged coordinator
            Assertions.assertEquals(SaveStatus.PreAcceptedPrivCoord, preacceptAsCoordinator(commandStore, coordId, txn, route));
            on(commandStore, coordId, txn, safeStore -> {
                Assertions.assertEquals(PREACCEPTED_COORD, safeStore.get(routingKey).current().get(coordId).status());
                return null;
            });

            // reported to recovery of an earlier transaction (which decides whether to report based on its own vote for the earlier transaction)
            Assertions.assertEquals(List.of(IsDep.SELF_COORD), on(commandStore, coordId, txn, safeStore -> visit(safeStore.get(routingKey).current(), earlierId)));
            Assertions.assertEquals(List.of(IsDep.NOT_ELIGIBLE), on(commandStore, coordId, txn, safeStore -> visit(safeStore.get(routingKey).current(), laterId)));

            // promising a non-zero ballot (e.g. a recovery coordinator, or CheckStatus with bumpBallot) disables our fast path
            Ballot ballot = Ballot.fromValues(coordId.epoch(), clock.now(), ID2);
            on(commandStore, coordId, txn, safeStore -> {
                SafeCommand safeCommand = safeStore.ifInitialised(coordId);
                safeCommand.updatePromised(safeStore, ballot);
                Assertions.assertEquals(SaveStatus.PreAcceptedPrivCoordRecovery, safeCommand.current().saveStatus());
                Assertions.assertEquals(SaveStatus.PreAccepted, safeCommand.current().saveStatus().report());
                Assertions.assertEquals(PREACCEPTED_COORD_RECOVERY, safeStore.get(routingKey).current().get(coordId).status());
                return null;
            });

            Assertions.assertEquals(List.of(IsDep.SELF_COORD_RECOVERY), on(commandStore, coordId, txn, safeStore -> visit(safeStore.get(routingKey).current(), earlierId)));
            Assertions.assertEquals(List.of(IsDep.NOT_ELIGIBLE), on(commandStore, coordId, txn, safeStore -> visit(safeStore.get(routingKey).current(), laterId)));

            // CommandsForKey round-trips our new status
            on(commandStore, coordId, txn, safeStore -> {
                CommandsForKey cfk = safeStore.get(routingKey).current();
                CommandsForKey reloaded = Serialize.fromBytes(routingKey, Serialize.toBytesWithoutKey(cfk));
                Assertions.assertEquals(PREACCEPTED_COORD_RECOVERY, reloaded.get(coordId).status());
                return null;
            });
        }
        finally
        {
            node.shutdown();
        }
    }

    @Test
    public void coordinatorWithoutVote()
    {
        RecordingMessageSink sink = new RecordingMessageSink(ID3, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID3, TOPOLOGY, sink, clock);
        try
        {
            Raw key = IntKey.key(10);
            RoutingKey routingKey = key.toUnseekable();
            Txn txn = writeTxn(Keys.of(key));
            FullRoute<?> route = txn.keys().toRoute(routingKey);
            CommandStore commandStore = node.unsafeForKey(routingKey);

            TxnId coordId = privileged(clock.idForNode(1, ID3));
            clock.increment(10);
            TxnId laterId = privileged(clock.idForNode(1, ID1));
            clock.increment(10);

            // witness a later conflict before we pre-accept our own transaction, so that we do not vote for its fast path
            preAccept(laterId, txn, route).process(node, ID1, Network.replyCtxFor(0));
            Assertions.assertEquals(SaveStatus.PreAcceptedPrivCoord, preacceptAsCoordinator(commandStore, coordId, txn, route));
            on(commandStore, coordId, txn, safeStore -> {
                Assertions.assertNotEquals(coordId, safeStore.ifInitialised(coordId).current().executeAt());
                Assertions.assertEquals(PREACCEPTED_COORD, safeStore.get(routingKey).current().get(coordId).status());
                CommandsForKey cfk = safeStore.get(routingKey).current();
                CommandsForKey reloaded = Serialize.fromBytes(routingKey, Serialize.toBytesWithoutKey(cfk));
                Assertions.assertEquals(PREACCEPTED_COORD, reloaded.get(coordId).status());
                return null;
            });
        }
        finally
        {
            node.shutdown();
        }
    }
}
