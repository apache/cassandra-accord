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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Result;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.messages.Apply.ApplyReply;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;

import static accord.messages.TxnRequest.computeScope;
import static accord.utils.Invariants.illegalState;

/*
 * Used by local and global inclusive sync points to effect the sync point at each node
 * Combines commit, execute (with nothing really to execute), and apply into one request/response
 *
 * This returns when the dependencies are Applied, but doesn't wait for this transaction to be Applied.
 */
public class ApplyThenWaitUntilApplied extends WaitUntilApplied
{
    private static final Logger logger = LoggerFactory.getLogger(ReadData.class);

    @SuppressWarnings("unused")
    public static class SerializerSupport
    {
        public static ApplyThenWaitUntilApplied create(TxnId txnId, Participants<?> readScope, long minEpoch, Timestamp executeAt, FullRoute<?> route, PartialTxn txn, PartialDeps deps, Writes writes, Result result)
        {
            return new ApplyThenWaitUntilApplied(txnId, readScope, minEpoch, executeAt, route, txn, deps, writes, result);
        }
    }

    public final Timestamp executeAt;
    public final FullRoute<?> route;
    public final PartialTxn txn;
    public final PartialDeps deps;
    public final Writes writes;
    public final Result result;

    public ApplyThenWaitUntilApplied(Node.Id to, Topologies topologies, Timestamp executeAt, FullRoute<?> route, TxnId txnId, Txn txn, Deps deps, Participants<?> readScope, Writes writes, Result result)
    {
        super(to, topologies, txnId, readScope, executeAt.epoch());
        this.executeAt = executeAt;
        Route<?> scope = computeScope(to, topologies, route);
        this.route = route;
        this.txn = txn.intersecting(scope, true);
        this.deps = deps.intersecting(scope);
        this.writes = writes;
        this.result = result;
    }

    protected ApplyThenWaitUntilApplied(TxnId txnId, Participants<?> readScope, long minEpoch, Timestamp executeAt, FullRoute<?> route, PartialTxn txn, PartialDeps deps, Writes writes, Result result)
    {
        super(txnId, readScope, minEpoch, executeAt.epoch());
        this.executeAt = executeAt;
        this.route = route;
        this.txn = txn;
        this.deps = deps;
        this.writes = writes;
        this.result = result;
    }

    @Override
    public ReadType kind()
    {
        return ReadType.applyThenWaitUntilApplied;
    }

    @Override
    public CommitOrReadNack apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.update(safeStore, route, txnId.epoch(), txnId, executeAtEpoch);
        ApplyReply applyReply = Apply.apply(safeStore, participants, txn, txnId, executeAt, deps, route, writes, result);
        switch (applyReply)
        {
            default:
                throw illegalState("Unexpected ApplyReply");
            case Insufficient:
                // Ignore here, the read in super.apply will return the CommitOrReadNack.Insufficient response we need to get the maximal apply
                break;
            case Redundant:
                // TODO (required): redundant is not necessarily safe for awaitsOnlyDeps commands as might need a future epoch
            case Applied:
                // In both cases it's fine to continue to process and return a response saying
                // things were applied
                break;
        }
        return super.apply(safeStore);
    }

    @Override
    public void accept(CommitOrReadNack reply, Throwable failure)
    {
        super.accept(reply, failure);

        boolean waiting;
        synchronized (this)
        {
            waiting = waitingOnCount >= 0;
        }
        if (waiting && reply == null && failure == null)
            node.reply(replyTo, replyContext, CommitOrReadNack.Waiting, null);
    }

    @Override
    public MessageType type()
    {
        return MessageType.APPLY_THEN_WAIT_UNTIL_APPLIED_REQ;
    }

    @Override
    public String toString()
    {
        return "ApplyThenWaitUntilApplied{" +
                "txnId:" + txnId +
                '}';
    }
}
