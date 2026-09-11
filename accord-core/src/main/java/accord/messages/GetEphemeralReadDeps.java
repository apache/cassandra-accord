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
import javax.annotation.Nullable;

import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.DepsCalculator;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.Node.Id;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

public class GetEphemeralReadDeps extends RouteRequest.WithUnsynced<GetEphemeralReadDeps.GetEphemeralReadDepsOk>
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
        return node.commandStores().mapReduceConsume(minEpoch, executionEpoch, this);
    }

    @Override
    public GetEphemeralReadDepsOk applyInternal(SafeCommandStore safeStore)
    {
        long latestEpoch = Math.max(safeStore.node().epoch(), node.epoch());

        // TODO (desired): only return failure if we've participated in a sync point that could migrate coordination to the newer epoch
        StoreParticipants participants = StoreParticipants.read(safeStore, scope, txnId, minEpoch, latestEpoch);
        if (latestEpoch > executionEpoch && (safeStore.ranges().removed(executionEpoch, latestEpoch).intersects(participants.owns()) || node.topology().active().hasReplicationMaybeChanged(participants.owns(), executionEpoch)))
            return new GetEphemeralReadDepsOk(latestEpoch);

        Deps deps;
        ExecuteFlags flags;
        try (DepsCalculator calculator = new DepsCalculator(txnId))
        {
             deps = calculator.calculate(safeStore, txnId, participants, minEpoch, Timestamp.MAX, false);
             flags = calculator.executeFlags(txnId);
        }
        return new GetEphemeralReadDepsOk(deps, latestEpoch, flags);
    }

    @Override
    public GetEphemeralReadDepsOk reduce(GetEphemeralReadDepsOk r1, GetEphemeralReadDepsOk r2)
    {
        long latestEpoch = Math.max(r1.latestEpoch, r2.latestEpoch);
        if (r1.deps == null || r2.deps == null)
            return new GetEphemeralReadDepsOk(latestEpoch);
        return new GetEphemeralReadDepsOk(r1.deps.with(r2.deps), latestEpoch, r1.flags.and(r2.flags));
    }

    @Override
    public MessageType type()
    {
        return MessageType.StandardMessage.GET_EPHEMERAL_READ_DEPS_REQ;
    }

    @Override
    protected boolean abort(Refuse.MinMax refuses)
    {
        return refuses.max != Refuse.NONE;
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
    public LoadKeys loadKeys()
    {
        return LoadKeys.SYNC;
    }

    @Override
    public LoadKeysFor loadKeysFor()
    {
        return LoadKeysFor.READ_WRITE;
    }

    public static class GetEphemeralReadDepsOk implements Reply
    {
        public enum Flag { READY_TO_EXECUTE }

        public final @Nullable Deps deps; // if null, unsuccessful
        public final long latestEpoch;
        public final ExecuteFlags flags;

        public GetEphemeralReadDepsOk(@Nonnull Deps deps, long latestEpoch, ExecuteFlags flags)
        {
            this.deps = Invariants.nonNull(deps);
            this.latestEpoch = latestEpoch;
            this.flags = flags;
        }

        public GetEphemeralReadDepsOk(long latestEpoch)
        {
            this.deps = null;
            this.latestEpoch = latestEpoch;
            this.flags = ExecuteFlags.none();
        }

        @Override
        public String toString()
        {
            return "GetEphemeralReadDepsOk" + deps + ',' + latestEpoch + '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.StandardMessage.GET_EPHEMERAL_READ_DEPS_RSP;
        }
    }
}

