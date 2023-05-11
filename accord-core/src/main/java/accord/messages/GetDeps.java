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

import accord.local.SafeCommandStore;
import accord.primitives.*;
import accord.utils.Invariants;

import accord.local.Node.Id;
import accord.topology.Topologies;

import javax.annotation.Nonnull;

import static accord.messages.PreAccept.calculatePartialDeps;

public class GetDeps extends TxnRequest.WithUnsynced<PartialDeps>
{
    public static final class SerializationSupport
    {
        public static GetDeps create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, Seekables<?, ?> keys, Timestamp executeAt)
        {
            return new GetDeps(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey, keys, executeAt);
        }
    }

    public final Seekables<?, ?> keys;
    public final Timestamp executeAt;

    public GetDeps(Id to, Topologies topologies, FullRoute<?> route, TxnId txnId, Seekables<?, ?> keys, Timestamp executeAt)
    {
        super(to, topologies, txnId, route);
        this.keys = keys.slice(scope.covering());
        this.executeAt = executeAt;
    }

    protected GetDeps(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, Seekables<?, ?> keys, Timestamp executeAt)
    {
        super(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey);
        this.keys = keys;
        this.executeAt = executeAt;
    }

    @Override
    public void process()
    {
        node.mapReduceConsumeLocal(this, minUnsyncedEpoch, executeAt.epoch(), this);
    }

    @Override
    public PartialDeps apply(SafeCommandStore instance)
    {
        Ranges ranges = instance.ranges().allBetween(minUnsyncedEpoch, executeAt);
        return calculatePartialDeps(instance, txnId, keys, executeAt, ranges);
    }

    @Override
    public PartialDeps reduce(PartialDeps deps1, PartialDeps deps2)
    {
        return deps1.with(deps2);
    }

    @Override
    public void accept(PartialDeps result, Throwable failure)
    {
        if (result == null) node.agent().onUncaughtException(failure); // TODO (expected): propagate failures to coordinator
        else node.reply(replyTo, replyContext, new GetDepsOk(result));
    }

    @Override
    public MessageType type()
    {
        return MessageType.GET_DEPS_REQ;
    }

    @Override
    public String toString()
    {
        return "GetDeps{" +
               "txnId:" + txnId +
               ", keys:" + keys +
               ", executeAt:" + executeAt +
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

    public static class GetDepsOk implements Reply
    {
        public final PartialDeps deps;

        public GetDepsOk(@Nonnull PartialDeps deps)
        {
            this.deps = Invariants.nonNull(deps);
        }

        @Override
        public String toString()
        {
            return toString("GetDepsOk");
        }

        String toString(String kind)
        {
            return kind + "{" + deps + '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.GET_DEPS_RSP;
        }
    }

}
