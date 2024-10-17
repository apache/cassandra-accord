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

import accord.local.KeyHistory;
import accord.local.Node.Id;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static accord.messages.PreAccept.calculateDeps;
import static accord.primitives.EpochSupplier.constant;

public class GetEphemeralReadDeps extends TxnRequest.WithUnsynced<GetEphemeralReadDeps.GetEphemeralReadDepsOk>
{
    public static final class SerializationSupport
    {
        public static GetEphemeralReadDeps create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, long executionEpoch)
        {
            return new GetEphemeralReadDeps(txnId, scope, waitForEpoch, minEpoch, executionEpoch);
        }
    }

    public final long executionEpoch;

    public GetEphemeralReadDeps(Id to, Topologies topologies, FullRoute<?> route, TxnId txnId, long executionEpoch)
    {
        super(to, topologies, txnId, route);
        this.executionEpoch = executionEpoch;
    }

    protected GetEphemeralReadDeps(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, long executionEpoch)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.executionEpoch = executionEpoch;
    }

    @Override
    public Cancellable submit()
    {
        return node.mapReduceConsumeLocal(this, minEpoch, executionEpoch, this);
    }

    @Override
    public GetEphemeralReadDepsOk apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.read(safeStore, scope, txnId, minEpoch, Long.MAX_VALUE);
        Deps deps = calculateDeps(safeStore, txnId, participants, constant(minEpoch), Timestamp.MAX);

        return new GetEphemeralReadDepsOk(deps, Math.max(safeStore.node().epoch(), node.epoch()));
    }

    @Override
    public GetEphemeralReadDepsOk reduce(GetEphemeralReadDepsOk reply1, GetEphemeralReadDepsOk reply2)
    {
        return new GetEphemeralReadDepsOk(reply1.deps.with(reply2.deps), Math.max(reply1.latestEpoch, reply2.latestEpoch));
    }

    @Override
    public MessageType type()
    {
        return MessageType.GET_EPHEMERAL_READ_DEPS_REQ;
    }

    @Override
    public String toString()
    {
        return "GetEphemeralReadDeps{" +
               "txnId:" + txnId +
               ", scope:" + scope +
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

    public static class GetEphemeralReadDepsOk implements Reply
    {
        public final Deps deps;
        public final long latestEpoch;

        public GetEphemeralReadDepsOk(@Nonnull Deps deps, long latestEpoch)
        {
            this.deps = Invariants.nonNull(deps);
            this.latestEpoch = latestEpoch;
        }

        @Override
        public String toString()
        {
            return "GetEphemeralReadDepsOk" + deps + ',' + latestEpoch + '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.GET_EPHEMERAL_READ_DEPS_RSP;
        }
    }
}

