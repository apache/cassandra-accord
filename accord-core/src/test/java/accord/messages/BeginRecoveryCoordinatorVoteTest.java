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

import java.util.concurrent.ExecutionException;

import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.ExecutionContext;
import accord.local.SafeCommand;
import accord.local.StoreParticipants;
import accord.primitives.SaveStatus;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.impl.IntKey;
import accord.impl.IntKey.Raw;
import accord.impl.TopologyFactory;
import accord.impl.mock.MockCluster.Clock;
import accord.impl.mock.Network;
import accord.impl.mock.RecordingMessageSink;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.BeginRecovery.RecoverOk;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.routing;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.utils.async.AsyncChainUtils.getUninterruptibly;

/**
 * This file was authored by LLM
 *
 * Recovery of a transaction id must learn from the coordinator of any later conflicting transaction id'
 * that is still pre-accepted whether its vote for id' may have not witnessed id, as id' may yet commit on the fast path
 * without witnessing id. The coordinator does not record its own deps, so must infer this from its own vote for id.
 *
 * This is the scenario from the paper's discussion of e <= (n-f+1)/2 (n=5, e=f=2): p1 pre-accepts id at {p1, p2};
 * p3 pre-accepts id' (t0' > t0) at {p3, p4, p5}; p2 recovers id with {p2, p3, p4}. We test p3's reply.
 */
public class BeginRecoveryCoordinatorVoteTest
{
    private static final Id ID1 = id(1), ID2 = id(2), ID3 = id(3), ID4 = id(4), ID5 = id(5);
    private static final List<Id> IDS = List.of(ID1, ID2, ID3, ID4, ID5);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 5, IntKey.range(0, 100));
    private static final Ranges FULL_RANGE = Ranges.single(IntKey.range(routing(Integer.MIN_VALUE), routing(Integer.MAX_VALUE)));
    private static final ReplyContext REPLY_CONTEXT = Network.replyCtxFor(0);

    private static TxnId privileged(TxnId txnId)
    {
        return new TxnId(txnId.epoch(), txnId.hlc(), PrivilegedCoordinatorWithDeps.bits, Write, Key, txnId.node);
    }

    private static PreAccept preAccept(TxnId txnId, Txn txn, FullRoute<?> route, boolean hasCoordinatorVote)
    {
        return PreAccept.SerializerSupport.create(txnId, route.overlapping(FULL_RANGE), txnId.epoch(), txnId.epoch(), txnId.epoch(), txn.slice(FULL_RANGE, true), null, hasCoordinatorVote, route);
    }

    private static void preacceptAsCoordinator(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        CommandStore commandStore = node.unsafeForKey(route.homeKey());
        try
        {
            getUninterruptibly(commandStore.submit(ExecutionContext.unsequencedWrite(txnId, txn.keys().toParticipants(), "Test"), safeStore -> {
                StoreParticipants participants = StoreParticipants.update(safeStore, route, txnId.epoch(), txnId, txnId.epoch());
                SafeCommand safeCommand = safeStore.get(txnId, participants);
                Assertions.assertEquals(Commands.AcceptOutcome.Success, Commands.preacceptAsPrivilegedCoordinator(safeStore, safeCommand, participants, txnId, txn.slice(FULL_RANGE, true)));
                Assertions.assertEquals(SaveStatus.PreAcceptedPrivCoord, safeCommand.current().saveStatus());
                return null;
            }));
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }

    private static BeginRecovery beginRecovery(TxnId txnId, Txn txn, FullRoute<?> route, Ballot ballot)
    {
        return BeginRecovery.SerializationSupport.create(txnId, route.overlapping(FULL_RANGE), txnId.epoch(), txnId.epoch(), txn.slice(FULL_RANGE, true), ballot, route, txnId.epoch(), 0);
    }

    private static RecoverOk recoverOk(RecordingMessageSink sink)
    {
        Object payload = sink.responses.get(sink.responses.size() - 1).payload;
        Assertions.assertTrue(payload instanceof RecoverOk, () -> "Unexpected reply " + payload);
        return (RecoverOk) payload;
    }

    enum Scenario { UNWITNESSED, WITNESSED_FIRST, NOT_COORDINATOR }

    private static RecoverOk run(Scenario scenario)
    {
        RecordingMessageSink sink = new RecordingMessageSink(ID3, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID3, TOPOLOGY, sink, clock);
        try
        {
            Raw key = IntKey.key(10);
            Txn txn = writeTxn(Keys.of(key));
            FullRoute<?> route = txn.keys().toRoute(key.toUnseekable());

            TxnId id = privileged(clock.idForNode(1, ID1));
            clock.increment(10);
            TxnId laterId = privileged(clock.idForNode(1, scenario == Scenario.NOT_COORDINATOR ? ID4 : ID3));
            clock.increment(10);
            Assertions.assertTrue(id.compareTo(laterId) < 0);

            if (scenario == Scenario.WITNESSED_FIRST)
                preAccept(id, txn, route, true).process(node, ID1, REPLY_CONTEXT);

            // if we are the coordinator of laterId we pre-accept locally as privileged coordinator (recording no deps);
            // otherwise we are a replica that (in this case) did not receive the coordinator's deps
            if (scenario == Scenario.NOT_COORDINATOR) preAccept(laterId, txn, route, false).process(node, laterId.node, REPLY_CONTEXT);
            else preacceptAsCoordinator(node, laterId, txn, route);

            Ballot ballot = Ballot.fromValues(id.epoch(), clock.now(), ID2);
            beginRecovery(id, txn, route, ballot).process(node, ID2, REPLY_CONTEXT);
            return recoverOk(sink);
        }
        finally
        {
            node.shutdown();
        }
    }

    @Test
    public void coordinatorReportsLaterTransactionThatMayNotWitness()
    {
        RecoverOk ok = run(Scenario.UNWITNESSED);
        Assertions.assertFalse(ok.selfAcceptsFastPath);
        Assertions.assertEquals(1, ok.supersedingCoordRejects.txnIdCount(), () -> "Expected the later transaction to be reported: " + ok);
        Assertions.assertFalse(ok.supersedingRejects);
    }

    @Test
    public void coordinatorDoesNotReportLaterTransactionThatWitnessed()
    {
        RecoverOk ok = run(Scenario.WITNESSED_FIRST);
        Assertions.assertTrue(ok.selfAcceptsFastPath);
        Assertions.assertTrue(ok.supersedingCoordRejects.isEmpty(), () -> "Expected no later transactions to be reported: " + ok);
    }

    @Test
    public void replicaWithoutCoordinatorDepsDoesNotReport()
    {
        RecoverOk ok = run(Scenario.NOT_COORDINATOR);
        Assertions.assertTrue(ok.supersedingCoordRejects.isEmpty(), () -> "Expected no later transactions to be reported: " + ok);
    }
}
