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
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.FullRoute;
import accord.primitives.PartialRoute;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;

public class GetMaxConflict extends TxnRequest.WithUnsynced<GetMaxConflict.GetMaxConflictOk>
{
    public static final class SerializationSupport
    {
        public static GetMaxConflict create(PartialRoute<?> scope, long waitForEpoch, long minEpoch, Seekables<?, ?> keys, long executionEpoch)
        {
            return new GetMaxConflict(scope, waitForEpoch, minEpoch, keys, executionEpoch);
        }
    }

    public final Seekables<?, ?> keys;
    public final long executionEpoch;

    public GetMaxConflict(Node.Id to, Topologies topologies, FullRoute<?> route, Seekables<?, ?> keys, long executionEpoch)
    {
        super(to, topologies, executionEpoch, route);
        this.keys = keys.slice(scope.covering());
        this.executionEpoch = executionEpoch;
    }

    protected GetMaxConflict(PartialRoute<?> scope, long waitForEpoch, long minEpoch,  Seekables<?, ?> keys, long executionEpoch)
    {
        super(TxnId.NONE, scope, waitForEpoch, minEpoch, true);
        this.keys = keys;
        this.executionEpoch = executionEpoch;
    }

    @Override
    public void process()
    {
        node.mapReduceConsumeLocal(this, minUnsyncedEpoch, executionEpoch, this);
    }

    @Override
    public GetMaxConflictOk apply(SafeCommandStore safeStore)
    {
        Ranges ranges = safeStore.ranges().allBetween(minUnsyncedEpoch, executionEpoch);
        Timestamp maxConflict = safeStore.commandStore().maxConflict(keys.slice(ranges));
        return new GetMaxConflictOk(maxConflict, Math.max(safeStore.time().epoch(), node.epoch()));
    }

    @Override
    public GetMaxConflictOk reduce(GetMaxConflictOk reply1, GetMaxConflictOk reply2)
    {
        return new GetMaxConflictOk(Timestamp.max(reply1.maxConflict, reply2.maxConflict), Math.max(reply1.latestEpoch, reply2.latestEpoch));
    }

    @Override
    public void accept(GetMaxConflictOk result, Throwable failure)
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
        return "GetMaxConflict{" +
               ", keys:" + keys +
               '}';
    }

    @Override
    public TxnId primaryTxnId()
    {
        return null;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return keys;
    }

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.NONE;
    }

    public static class GetMaxConflictOk implements Reply
    {
        public final Timestamp maxConflict;
        public final long latestEpoch;

        public GetMaxConflictOk(@Nonnull Timestamp maxConflict, long latestEpoch)
        {
            this.maxConflict = Invariants.nonNull(maxConflict);
            this.latestEpoch = latestEpoch;
        }

        @Override
        public String toString()
        {
            return "GetMaxConflictOk(" + maxConflict + ',' + latestEpoch + '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.GET_EPHEMERAL_READ_DEPS_RSP;
        }
    }
}
