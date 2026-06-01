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

import javax.annotation.Nullable;

import accord.local.Commands;
import accord.local.Commands.CommitOutcome;
import accord.local.LoadKeys;
import accord.local.LogFaultException;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
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
import accord.topology.Topologies;

import static accord.api.ProtocolModifiers.loadKeysAsyncIfPermitted;
import static accord.messages.MessageType.StandardMessage.STABLE_THEN_READ_REQ;
import static accord.messages.ReadData.CommitOrReadNack.Kind.InsufficientEpochs;
import static accord.primitives.SaveStatus.ReadyToExecute;

/**
 * Wait until the transaction has been applied locally
 */
public class StableThenRead extends ReadData
{
    public static class SerializerSupport
    {
        public static StableThenRead create(TxnId txnId, Participants<?> readScope, Commit.Kind kind, long minEpoch, Timestamp executeAt, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute)
        {
            return new StableThenRead(txnId, readScope, kind, minEpoch, executeAt, partialTxn, partialDeps, fullRoute);
        }
    }

    public static final ExecuteOn EXECUTE_ON = new ExecuteOn(ReadyToExecute, ReadyToExecute);

    public final long minEpoch;
    public final Commit.Kind kind;
    private @Nullable PartialDeps partialDeps;
    public final @Nullable FullRoute<?> route;

    public StableThenRead(Commit.Kind kind, Node.Id to, Topologies topologies, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps)
    {
        super(to, topologies, txnId, route, null, executeAt, executeAt.epoch());
        this.minEpoch = topologies.oldestEpoch();
        this.kind = kind;
        this.partialTxn = kind.withTxn.select(txn, scope, topologies, txnId, to);
        this.partialDeps = kind.withDeps.select(deps, scope);
        this.route = kind.withTxn.select(route);
    }

    protected StableThenRead(TxnId txnId, Participants<?> readScope, Commit.Kind kind, long minEpoch, Timestamp executeAt, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute)
    {
        super(txnId, readScope, partialTxn, executeAt, executeAt.epoch());
        this.minEpoch = minEpoch;
        this.kind = kind;
        this.partialDeps = partialDeps;
        this.route = fullRoute;
    }

    @Override
    public CommitOrReadNack applyInternal(SafeCommandStore safeStore)
    {
        Route<?> route = this.route == null ? (Route)scope : this.route;
        StoreParticipants participants = StoreParticipants.execute(safeStore, route, txnId, minEpoch(), executeAtEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        CommitOutcome outcome = Commands.commit(safeStore, safeCommand, participants, kind.saveStatus, Ballot.ZERO, txnId, route, partialTxn, executeAt, partialDeps, kind);
        if (outcome == CommitOutcome.Rejected)
        {
            cancel();
            return CommitOrReadNack.Rejected;
        }

        if (participants.executes().isEmpty())
            return null;

        CommitOrReadNack reply = super.applyInternal(safeStore, safeCommand, participants);
        if (outcome == CommitOutcome.InsufficientEpochs)
            return new CommitOrReadNack(InsufficientEpochs, Commit.insufficientEpoch(safeStore, safeCommand, txnId, route));
        return reply;
    }

    @Override
    protected CommitOrReadNack refuseInternal(SafeCommandStore safeStore)
    {
        // cannot commit, so should not reply implying we have done so, even with an unavailable exception (as may incorrectly infer quorum stability)
        throw new LogFaultException();
    }

    @Override
    public void accept(CommitOrReadNack reply, Throwable failure)
    {
        partialDeps = null;
        super.accept(reply, failure);
    }

    @Override
    public long minEpoch()
    {
        return minEpoch;
    }

    public @Nullable PartialDeps partialDeps()
    {
        return partialDeps;
    }

    @Override
    protected ExecuteOn executeOn()
    {
        return EXECUTE_ON;
    }

    @Override
    public LoadKeys loadKeys()
    {
        return loadKeysAsyncIfPermitted(txnId);
    }

    @Override
    public ReadType kind()
    {
        return ReadType.stableThenRead;
    }

    @Override
    public MessageType type()
    {
        return STABLE_THEN_READ_REQ;
    }

    @Override
    public String toString()
    {
        return "StableThenRead{" +
               "txnId:" + txnId +
               '}';
    }
}
