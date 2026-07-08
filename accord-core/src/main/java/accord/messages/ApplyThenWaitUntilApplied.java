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
import accord.api.Result.PersistableResult;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.messages.Apply.ApplyReply;
import accord.primitives.Ballot;
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
import accord.utils.UnhandledEnum;

import static accord.messages.MessageType.StandardMessage.APPLY_THEN_WAIT_UNTIL_APPLIED_REQ;
import static accord.messages.RouteRequest.computeScope;

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
        public static ApplyThenWaitUntilApplied create(TxnId txnId, Participants<?> readScope, long minEpoch, Timestamp executeAt, FullRoute<?> route, PartialTxn txn, PartialDeps deps, Writes writes, PersistableResult result)
        {
            return new ApplyThenWaitUntilApplied(txnId, readScope, minEpoch, executeAt, route, txn, deps, writes, result);
        }
    }

    public final FullRoute<?> route;
    private PartialTxn txn;
    private PartialDeps deps;
    private Writes writes;
    private PersistableResult result;

    public PartialTxn   txn() { return txn; }
    public PartialDeps deps() { return deps; }
    public Writes    writes() { return writes; }
    public Result    result() { return result; }

    public ApplyThenWaitUntilApplied(Node.Id to, Topologies topologies, Timestamp executeAt, FullRoute<?> route, TxnId txnId, Txn txn, Deps deps, Participants<?> readScope, Writes writes, PersistableResult result)
    {
        this(to, topologies, executeAt, executeAt.epoch(), route, txnId, txn, deps, readScope, writes, result);
    }

    public ApplyThenWaitUntilApplied(Node.Id to, Topologies topologies, Timestamp executeAt, long executeAtEpoch, FullRoute<?> route, TxnId txnId, Txn txn, Deps deps, Participants<?> readScope, Writes writes, PersistableResult result)
    {
        super(to, topologies, txnId, readScope, executeAt, executeAtEpoch);
        Route<?> scope = computeScope(to, topologies, route);
        this.route = route;
        this.txn = txn.intersecting(scope, true);
        this.deps = deps.intersecting(scope);
        this.writes = writes;
        this.result = result;
    }

    protected ApplyThenWaitUntilApplied(TxnId txnId, Participants<?> readScope, long minEpoch, Timestamp executeAt, FullRoute<?> route, PartialTxn txn, PartialDeps deps, Writes writes, PersistableResult result)
    {
        super(txnId, readScope, minEpoch, executeAt, executeAt.epoch());
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
    public CommitOrReadNack applyInternal(SafeCommandStore safeStore)
    {
        PartialTxn txn = this.txn;
        PartialDeps deps = this.deps;
        Writes writes = this.writes;
        PersistableResult result = this.result;
        if (!isPending())
            return null; // we can't throw an exception here else we override any non-exceptional reply informing the reason

        StoreParticipants participants = StoreParticipants.execute(safeStore, route, minEpoch(), txnId, executeAtEpoch);
        ApplyReply applyReply = Apply.apply(safeStore, participants, Ballot.ZERO, txn, txnId, executeAt, deps, route, writes, result);
        switch (applyReply.kind)
        {
            default:
                throw UnhandledEnum.unknown(applyReply.kind);
            case Insufficient:
            case InsufficientEpochs:
                // Ignore here, the read in super.apply will return the CommitOrReadNack.Insufficient response we need to get the maximal apply
                break;
            case Redundant:
                // TODO (required): redundant is not necessarily safe for awaitsOnlyDeps commands as might need a future epoch
            case Applied:
            case RaceWithRecovery:
                // In both cases it's fine to continue to process and return a response saying
                // things were applied
                break;
        }
        return super.applyInternal(safeStore);
    }

    @Override
    public void accept(CommitOrReadNack reply, Throwable failure)
    {
        super.accept(reply, failure);
        txn = null;
        deps = null;
        writes = null;
        result = null;
    }

    @Override
    public MessageType type()
    {
        return APPLY_THEN_WAIT_UNTIL_APPLIED_REQ;
    }

    public ExecutionKind executionKind()
    {
        return ExecutionKind.APPLY;
    }

    @Override
    public String toString()
    {
        return "ApplyThenWaitUntilApplied{" +
                "txnId:" + txnId +
                '}';
    }
}
