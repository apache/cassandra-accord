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
 *
 * TODO (required): is this safe for barrier transactions? Since it is invisible to other transactions, it can be recovered
 *   to execute at its TxnId time, even if a later txn exists. We must either make it visible to other transactions
 *   for coordination (but not necessarily for execution), or else require that it has an Accept round.
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

    // TODO (required): this is not safe to use as a "sourceEpoch", as a transaction in the dependencies may execute in a future epoch
    public long sourceEpoch()
    {
        return syncId.epoch();
    }

    public long earliestEpoch()
    {
        long epoch = syncId.epoch();
        if (waitFor.keyDeps.txnIdCount() > 0)
            epoch = Math.min(waitFor.keyDeps.txnId(0).epoch(), epoch);
        if (waitFor.rangeDeps.txnIdCount() > 0)
            epoch = Math.min(waitFor.rangeDeps.txnId(0).epoch(), epoch);
        if (waitFor.directKeyDeps.txnIdCount() > 0)
            epoch = Math.min(waitFor.directKeyDeps.txnId(0).epoch(), epoch);
        return epoch;
    }

    public FullRoute route()
    {
        return keysOrRanges.toRoute(homeKey);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SyncPoint<?> syncPoint = (SyncPoint<?>) o;
        return syncId.equals(syncPoint.syncId) && waitFor.equals(syncPoint.waitFor) && keysOrRanges.equals(syncPoint.keysOrRanges) && homeKey.equals(syncPoint.homeKey);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "SyncPoint{" +
               "syncId=" + syncId +
               ", keysOrRanges=" + keysOrRanges +
               ", homeKey=" + homeKey +
               ", waitFor=" + waitFor +
               '}';
    }
}
