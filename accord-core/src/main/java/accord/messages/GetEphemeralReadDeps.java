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
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;

import static accord.messages.PreAccept.calculatePartialDeps;
import static accord.primitives.EpochSupplier.constant;

public class GetEphemeralReadDeps extends TxnRequest.WithUnsynced<GetEphemeralReadDeps.GetEphemeralReadDepsOk>
{
    public static final class SerializationSupport
    {
        public static GetEphemeralReadDeps create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch, Seekables<?, ?> keys, long executionEpoch)
        {
            return new GetEphemeralReadDeps(txnId, scope, waitForEpoch, minEpoch, keys, executionEpoch);
        }
    }

    public final Seekables<?, ?> keys;
    public final long executionEpoch;

    public GetEphemeralReadDeps(Id to, Topologies topologies, FullRoute<?> route, TxnId txnId, Seekables<?, ?> keys, long executionEpoch)
    {
        super(to, topologies, txnId, route);
        this.keys = keys.slice(scope.covering());
        this.executionEpoch = executionEpoch;
    }

    protected GetEphemeralReadDeps(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch,  Seekables<?, ?> keys, long executionEpoch)
    {
        super(txnId, scope, waitForEpoch, minEpoch, true);
        this.keys = keys;
        this.executionEpoch = executionEpoch;
    }

    @Override
    public void process()
    {
        node.mapReduceConsumeLocal(this, minUnsyncedEpoch, executionEpoch, this);
    }

    @Override
    public GetEphemeralReadDepsOk apply(SafeCommandStore safeStore)
    {
        Ranges ranges = safeStore.ranges().allBetween(minUnsyncedEpoch, executionEpoch);
        PartialDeps deps = calculatePartialDeps(safeStore, txnId, keys, constant(minUnsyncedEpoch), Timestamp.MAX, ranges);

        return new GetEphemeralReadDepsOk(deps, Math.max(safeStore.time().epoch(), node.epoch()));
    }

    @Override
    public GetEphemeralReadDepsOk reduce(GetEphemeralReadDepsOk reply1, GetEphemeralReadDepsOk reply2)
    {
        return new GetEphemeralReadDepsOk(reply1.deps.with(reply2.deps), Math.max(reply1.latestEpoch, reply2.latestEpoch));
    }

    @Override
    public void accept(GetEphemeralReadDepsOk result, Throwable failure)
    {
        node.reply(replyTo, replyContext, result, failure);
    }

    @Override
    public MessageType type()
    {
        return MessageType.GET_EPHEMERAL_READ_DEPS_REQ;
    }

    @Override
    public String toString()
    {
        return "GetDeps{" +
               "txnId:" + txnId +
               ", keys:" + keys +
               '}';
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return keys;
    }

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.DEPS;
    }

    public static class GetEphemeralReadDepsOk implements Reply
    {
        public final PartialDeps deps;
        public final long latestEpoch;

        public GetEphemeralReadDepsOk(@Nonnull PartialDeps deps, long latestEpoch)
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

