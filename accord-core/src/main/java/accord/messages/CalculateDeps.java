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
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;

import static accord.messages.PreAccept.calculatePartialDeps;
import static accord.primitives.EpochSupplier.constant;

public class CalculateDeps extends TxnRequest.WithUnsynced<PartialDeps>
{
    public static final class SerializationSupport
    {
        public static CalculateDeps create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Seekables<?, ?> keys, Timestamp executeAt)
        {
            return new CalculateDeps(txnId, scope, waitForEpoch, minEpoch, keys, executeAt);
        }
    }

    public final Seekables<?, ?> keys;
    public final Timestamp executeAt;

    public CalculateDeps(Id to, Topologies topologies, FullRoute<?> route, TxnId txnId, Seekables<?, ?> keys, Timestamp executeAt)
    {
        super(to, topologies, txnId, route);
        this.keys = keys.intersecting(scope);
        this.executeAt = executeAt;
    }

    protected CalculateDeps(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Seekables<?, ?> keys, Timestamp executeAt)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.keys = keys;
        this.executeAt = executeAt;
    }

    @Override
    public void process()
    {
        node.mapReduceConsumeLocal(this, minEpoch, executeAt.epoch(), this);
    }

    @Override
    public PartialDeps apply(SafeCommandStore instance)
    {
        Ranges ranges = instance.ranges().allBetween(minEpoch, executeAt);
        return calculatePartialDeps(instance, txnId, keys, scope, constant(minEpoch), executeAt, ranges);
    }

    @Override
    public PartialDeps reduce(PartialDeps deps1, PartialDeps deps2)
    {
        return deps1.with(deps2);
    }

    @Override
    public void accept(PartialDeps result, Throwable failure)
    {
        node.reply(replyTo, replyContext, result != null ? new CalculateDepsOk(result) : null, failure);
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

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.COMMANDS;
    }

    public static class CalculateDepsOk implements Reply
    {
        public final PartialDeps deps;

        public CalculateDepsOk(@Nonnull PartialDeps deps)
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
