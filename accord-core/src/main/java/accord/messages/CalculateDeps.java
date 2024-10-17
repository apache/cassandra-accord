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

public class CalculateDeps extends TxnRequest.WithUnsynced<CalculateDeps.CalculateDepsOk>
{
    public static final class SerializationSupport
    {
        public static CalculateDeps create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Timestamp executeAt)
        {
            return new CalculateDeps(txnId, scope, waitForEpoch, minEpoch, executeAt);
        }
    }

    public final Timestamp executeAt;

    public CalculateDeps(Id to, Topologies topologies, FullRoute<?> route, TxnId txnId, Timestamp executeAt)
    {
        super(to, topologies, txnId, route);
        this.executeAt = executeAt;
    }

    protected CalculateDeps(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Timestamp executeAt)
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
    public CalculateDepsOk apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.read(safeStore, scope, txnId, minEpoch, executeAt.epoch());
        return new CalculateDepsOk(calculateDeps(safeStore, txnId, participants, constant(minEpoch), executeAt));
    }

    @Override
    public CalculateDepsOk reduce(CalculateDepsOk ok1, CalculateDepsOk ok2)
    {
        return new CalculateDepsOk(ok1.deps.with(ok2.deps));
    }

    @Override
    public MessageType type()
    {
        return MessageType.CALCULATE_DEPS_REQ;
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

    public static class CalculateDepsOk implements Reply
    {
        public final Deps deps;

        public CalculateDepsOk(@Nonnull Deps deps)
        {
            this.deps = Invariants.nonNull(deps);
        }

        @Override
        public String toString()
        {
            return "GetDepsOk{" + deps + '}' ;
        }

        @Override
        public MessageType type()
        {
            return MessageType.CALCULATE_DEPS_RSP;
        }
    }

}
