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

import javax.annotation.Nonnull;

import accord.api.Data;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Node.Id;
import accord.local.ExecutionContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.SaveStatus;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import org.agrona.collections.IntHashSet;

import static accord.local.Commands.eraseEphemeralRead;
import static accord.messages.MessageType.StandardMessage.READ_EPHEMERAL_REQ;
import static accord.messages.RouteRequest.computeScope;
import static accord.messages.RouteRequest.latestRelevantEpochIndex;

public class ReadEphemeralTxnData extends ReadData
{
    public static class SerializerSupport
    {
        public static ReadEphemeralTxnData create(TxnId txnId, Participants<?> scope, @Nonnull PartialTxn partialTxn, @Nonnull PartialDeps partialDeps, @Nonnull FullRoute<?> route, ExecuteFlags flags)
        {
            return new ReadEphemeralTxnData(txnId, scope, partialTxn, partialDeps, route, flags);
        }
    }

    private static final ExecuteOn EXECUTE_ON = new ExecuteOn(SaveStatus.ReadyToExecute, SaveStatus.Applied);

    private PartialDeps partialDeps;
    private FullRoute<?> route; // TODO (desired): should be unnecessary, only included to not breach Stable command validations

    public ReadEphemeralTxnData(Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, @Nonnull Txn txn, @Nonnull Deps deps, @Nonnull FullRoute<?> route, ExecuteFlags flags)
    {
        this(to, topologies, txnId, readScope, txn, deps, route, flags, latestRelevantEpochIndex(to, topologies, readScope));
    }

    private ReadEphemeralTxnData(Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, @Nonnull Txn txn, @Nonnull Deps deps, @Nonnull FullRoute<?> route, ExecuteFlags flags, int latestRelevantIndex)
    {
        this(txnId, readScope, computeScope(to, topologies, route, latestRelevantIndex), txnId.epoch(), txn, deps, route, flags);
    }

    private ReadEphemeralTxnData(TxnId txnId, Participants<?> readScope, Route<?> scope, long executeAtEpoch, @Nonnull Txn txn, @Nonnull Deps deps, @Nonnull FullRoute<?> route, ExecuteFlags flags)
    {
        super(txnId, readScope.overlapping(scope), txn.intersecting(scope, false), txnId, executeAtEpoch, flags);
        Invariants.require(executeAtEpoch == txnId.epoch(),
                           "Epoch for transaction %s (%d) did not match expected %d", txn, txnId.epoch(), executeAtEpoch);
        this.route = route;
        this.partialDeps = deps.intersecting(scope);
    }

    public ReadEphemeralTxnData(TxnId txnId, Participants<?> readScope, @Nonnull PartialTxn partialTxn, @Nonnull PartialDeps partialDeps, @Nonnull FullRoute<?> route, ExecuteFlags flags)
    {
        super(txnId, readScope, partialTxn, txnId, txnId.epoch(), flags);
        this.partialDeps = partialDeps;
        this.route = route;
    }

    @Override
    public CommitOrReadNack applyInternal(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.execute(safeStore, route, txnId, minEpoch(), executeAtEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        return applyInternal(safeStore, safeCommand, participants);
    }

    @Override
    protected synchronized CommitOrReadNack applyInternal(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants)
    {
        Commands.ephemeralRead(safeStore, safeCommand, participants, route, txnId, partialTxn, partialDeps);
        return super.applyInternal(safeStore, safeCommand, participants);
    }

    public final PartialDeps partialDeps()
    {
        return partialDeps;
    }

    public final FullRoute<?> route()
    {
        return route;
    }

    @Override
    public void accept(CommitOrReadNack reply, Throwable failure)
    {
        partialDeps = null;
        route = null;
        super.accept(reply, failure);
    }

    @Override
    protected boolean mayFastExecute()
    {
        return true;
    }

    @Override
    protected ExecuteOn executeOn()
    {
        return EXECUTE_ON;
    }

    @Override
    public ReadType kind()
    {
        return ReadType.readEphemeral;
    }

    @Override
    protected void read(SafeCommandStore safeStore, Command command)
    {
        long retryInLaterEpoch = retryInLaterEpoch(executeAtEpoch, safeStore, command);
        if (retryInLaterEpoch > 0)
        {
            // TODO (expected): wait for all stores' results and report only the ranges that execute later to be retried
            cancel();
            reply(new ReadOkWithFutureEpoch(null, null, retryInLaterEpoch), null);
        }
        super.read(safeStore, command);
    }

    public static long retryInLaterEpoch(long executeAtEpoch, SafeCommandStore safeStore, Command command)
    {
        TxnId txnId = command.txnId();
        if (!txnId.awaitsOnlyDeps())
            return 0;

        // TODO (required): should we disambiguate between cases where a truncated command has been executed locally
        //   versus made redundant by e.g. bootstrap, staleness etc? that *should* be handled by checking
        //   unavailable ranges
        Timestamp executesAtLeast = command.executesAtLeast();
        if (executesAtLeast != null && executesAtLeast.epoch() > executeAtEpoch)
        {
            long executeAtLeastEpoch = executesAtLeast.epoch();
            Ranges removed = safeStore.ranges().removed(executeAtEpoch, executeAtLeastEpoch);
            if (removed.intersects(command.route()))
                return executeAtLeastEpoch;
        }

        return 0;
    }

    @Override
    protected boolean cancel()
    {
        if (!super.cancel())
            return false;

        synchronized (this)
        {
            IntHashSet.IntIterator iter = waitingOn.iterator();
            while (iter.hasNext())
            {
                node.commandStores().forId(iter.nextValue())
                    .execute((ExecutionContext.Empty) () -> "Timeout Ephemeral Read", safeStore -> {
                        eraseEphemeralRead(safeStore, txnId);
                    }, node.agent());
            }
        }
        return true;
    }

    @Override
    protected void reply(Ranges unavailable, Data data, long uniqueHlc)
    {
        reply(new ReadOkWithFutureEpoch(unavailable, data, uniqueHlc), null);
    }

    @Override
    public MessageType type()
    {
        return READ_EPHEMERAL_REQ;
    }

    @Override
    public String reason()
    {
        return "Ephemeral Read";
    }
}
