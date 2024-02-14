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

package accord.primitives;

import accord.api.RoutingKey;
import accord.utils.Invariants;

/**
 * Defines an inequality point in the processing of the distributed transaction log, which is to say that
 * this is able to say that the point has passed, or that it has not yet passed, but it is unable to
 * guarantee that it is processed at the precise moment given by {@code at}. This is because we do not
 * expect the whole cluster to process these, and we do not want transaction processing to be held up,
 * so while these are processed much like a transaction, they are invisible to real transactions which
 * may proceed before this is witnessed by the node processing it.
 */
public class SyncPoint<S extends Seekables<?, ?>>
{
    public static class SerializationSupport
    {
        public static SyncPoint construct(TxnId syncId, Deps waitFor, Seekables<?,?> keysOrRanges, RoutingKey homeKey)
        {
            return new SyncPoint(syncId, waitFor, keysOrRanges, homeKey);
        }
    }

    public final TxnId syncId;
    public final Deps waitFor;
    public final S keysOrRanges;
    public final RoutingKey homeKey;

    public SyncPoint(TxnId syncId, Deps waitFor, S keysOrRanges, FullRoute<?> route)
    {
        Invariants.checkArgument(keysOrRanges.toRoute(route.homeKey()).equals(route), "Expected homeKey %s from route %s to be in ranges %s", route.homeKey(), route, keysOrRanges);
        this.syncId = syncId;
        this.waitFor = waitFor;
        this.keysOrRanges = keysOrRanges;
        this.homeKey = route.homeKey();
    }

    private SyncPoint(TxnId syncId, Deps waitFor, S keysOrRanges, RoutingKey homeKey)
    {
        this.syncId = syncId;
        this.waitFor = waitFor;
        this.keysOrRanges = keysOrRanges;
        this.homeKey = homeKey;
    }

    public FullRoute route()
    {
        return keysOrRanges.toRoute(homeKey);
    }

    // TODO (required): document this and its usages; make sure call-sites make sense
    public long sourceEpoch()
    {
        TxnId maxDep = waitFor.maxTxnId();
        return TxnId.nonNullOrMax(maxDep, syncId).epoch();
    }
}
