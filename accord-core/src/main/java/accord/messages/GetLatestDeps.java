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

import accord.local.Command;
import accord.local.KeyHistory;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.LatestDeps;
import accord.primitives.PartialDeps;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static accord.messages.PreAccept.calculateDeps;
import static accord.primitives.EpochSupplier.constant;

public class GetLatestDeps extends TxnRequest.WithUnsynced<GetLatestDeps.GetLatestDepsOk>
{
    public static final class SerializationSupport
    {
        public static GetLatestDeps create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Timestamp executeAt)
        {
            return new GetLatestDeps(txnId, scope, waitForEpoch, minEpoch, executeAt);
        }
    }

    public final Timestamp executeAt;

    public GetLatestDeps(Id to, Topologies topologies, FullRoute<?> route, TxnId txnId, Timestamp executeAt)
    {
        super(to, topologies, txnId, route);
        this.executeAt = executeAt;
    }

    protected GetLatestDeps(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Timestamp executeAt)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.executeAt = executeAt;
    }

    @Override
    public Cancellable submit()
    {
        return node.mapReduceConsumeLocal(this, minEpoch, executeAt.epoch(), this);
    }

    @Override
    public GetLatestDepsOk apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.read(safeStore, scope, txnId, minEpoch, executeAt.epoch());
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Command command = safeCommand.current();
        PartialDeps coordinatedDeps = command.partialDeps();
        Deps localDeps = null;
        switch (command.known().deps)
        {
            default: throw new AssertionError("Unhandled KnownDeps: " + command.known().deps);
            case NoDeps:
            case DepsErased:
            case DepsKnown:
            case DepsCommitted:
                break;
            case DepsUnknown:
            case DepsProposed:
                localDeps = calculateDeps(safeStore, txnId, participants, constant(minEpoch), executeAt, false);
        }

        LatestDeps deps = LatestDeps.create(participants.owns(), command.known().deps, command.acceptedOrCommitted(), coordinatedDeps, localDeps);
        return new GetLatestDepsOk(deps);
    }

    @Override
    public GetLatestDepsOk reduce(GetLatestDepsOk ok1, GetLatestDepsOk ok2)
    {
        return new GetLatestDepsOk(LatestDeps.merge(ok1.deps, ok2.deps));
    }

    @Override
    public MessageType type()
    {
        return MessageType.GET_LATEST_DEPS_REQ;
    }

    @Override
    public String toString()
    {
        return "CalculateDeps{" +
               "txnId:" + txnId +
               ", scope:" + scope +
               ", executeAt:" + executeAt +
               '}';
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Unseekables<?> keys()
    {
        return scope;
    }

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.SYNC;
    }

    public static class GetLatestDepsOk implements Reply
    {
        public final LatestDeps deps;

        public GetLatestDepsOk(@Nonnull LatestDeps deps)
        {
            this.deps = Invariants.nonNull(deps);
        }

        @Override
        public String toString()
        {
            return "GetLatestDepsOk{" + deps + '}' ;
        }

        @Override
        public MessageType type()
        {
            return MessageType.GET_LATEST_DEPS_RSP;
        }
    }

}
