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

package accord.coordinate;

import java.util.EnumSet;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.Routables;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topology;

import static accord.utils.Invariants.checkState;

public class TopologyMismatch extends CoordinationFailed
{
    public enum Reason { HOME_KEY, KEYS_OR_RANGES }

    private final EnumSet<Reason> reasons;

    private TopologyMismatch(EnumSet<Reason> reasons, Topology topology, @Nullable TxnId txnId, @Nullable RoutingKey homeKey, Routables<?> keysOrRanges)
    {
        super(txnId, homeKey, buildMessage(reasons, topology, homeKey, keysOrRanges));
        this.reasons = reasons;
    }

    private TopologyMismatch(EnumSet<Reason> reasons, Topology t, Unseekables<?> select)
    {
        super(null, null, buildMessage(t, select));
        this.reasons = reasons;
    }

    private TopologyMismatch(EnumSet<Reason> reasons, TxnId txnId, @Nullable RoutingKey homeKey, TopologyMismatch cause)
    {
        super(txnId, homeKey, cause);
        this.reasons = reasons;
    }

    @Override
    public TopologyMismatch wrap()
    {
        checkState(this.getClass() == TopologyMismatch.class);
        return new TopologyMismatch(reasons, txnId(), homeKey(), this);
    }

    private static String buildMessage(EnumSet<Reason> reason, Topology topology, RoutingKey homeKey, Routables<?> keysOrRanges)
    {
        StringBuilder sb = new StringBuilder();
        if (reason.contains(Reason.KEYS_OR_RANGES))
            sb.append(String.format("Txn attempted to access keys or ranges %s that are no longer valid globally (%d -> %s)", keysOrRanges, topology.epoch(), topology.ranges()));
        if (reason.contains(Reason.HOME_KEY))
        {
            if (sb.length() != 0)
                sb.append('\n');
            sb.append(String.format("HomeKey %s exists for a range that is no longer globally valid (%d -> %s)", homeKey, topology.epoch(), topology.ranges()));
        }
        return sb.toString();
    }

    private static String buildMessage(Topology t, Unseekables<?> select)
    {
        return String.format("Attempted to access %s that are no longer valid globally (%d -> %s)", select.without(t.ranges()), t.epoch(), t.ranges());
    }

    @Nullable
    public static TopologyMismatch checkForMismatch(Topology t, Unseekables<?> select)
    {
        return t.ranges().containsAll(select) ? null : new TopologyMismatch(EnumSet.of(Reason.KEYS_OR_RANGES), t, select);
    }

    @Nullable
    public static TopologyMismatch checkForMismatch(Topology t, @Nullable TxnId txnId, RoutingKey homeKey, Routables<?> keysOrRanges)
    {
        EnumSet<TopologyMismatch.Reason> reasons = null;
        if (!t.ranges().contains(homeKey))
        {
            if (reasons == null)
                reasons = EnumSet.noneOf(TopologyMismatch.Reason.class);
            reasons.add(TopologyMismatch.Reason.HOME_KEY);
        }
        if (!t.ranges().containsAll(keysOrRanges))
        {
            if (reasons == null)
                reasons = EnumSet.noneOf(TopologyMismatch.Reason.class);
            reasons.add(TopologyMismatch.Reason.KEYS_OR_RANGES);
        }
        return reasons == null ? null : new TopologyMismatch(reasons, t, txnId, homeKey, keysOrRanges);
    }

    public boolean hasReason(Reason reason)
    {
        return reasons.contains(reason);
    }
}
