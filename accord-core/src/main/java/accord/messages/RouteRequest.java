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

import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.Cancellable;

public abstract class RouteRequest<R extends Reply> extends ParticipantsRequest<Route<?>, R>
{
    public static abstract class WithUnsynced<R extends Reply> extends RouteRequest<R>
    {
        public final long minEpoch; // TODO (low priority, clarity): can this just always be TxnId.epoch?

        public WithUnsynced(Id to, Topologies topologies, TxnId txnId, Route<?> route)
        {
            this(to, topologies, txnId, route, latestRelevantEpochIndex(to, topologies, route));
        }

        public WithUnsynced(Id to, Topologies topologies, Route<?> route)
        {
            this(to, topologies, TxnId.NONE, route, latestRelevantEpochIndex(to, topologies, route));
        }

        protected WithUnsynced(Id to, Topologies topologies, TxnId txnId, Route<?> route, int startIndex)
        {
            super(to, topologies, route, txnId, startIndex);
            this.minEpoch = topologies.oldestEpoch();
        }

        protected WithUnsynced(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch)
        {
            super(txnId, scope, waitForEpoch);
            this.minEpoch = minEpoch;
        }
    }

    public RouteRequest(Node.Id to, Topologies topologies, Route<?> route, TxnId txnId)
    {
        this(to, topologies, route, txnId, latestRelevantEpochIndex(to, topologies, route));
    }

    public RouteRequest(Node.Id to, Topologies topologies, Route<?> route, TxnId txnId, int startIndex)
    {
        this(txnId, computeScope(to, topologies, route, startIndex), computeWaitForEpoch(to, topologies, startIndex));
    }

    public RouteRequest(TxnId txnId, Route<?> scope, long waitForEpoch)
    {
        super(txnId, scope, waitForEpoch);
    }

    @Override
    protected abstract @Nullable Cancellable submit();
}
