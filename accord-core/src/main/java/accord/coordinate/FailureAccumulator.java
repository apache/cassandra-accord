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

import java.util.Collection;
import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.RoutingKey;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.TxnId;

public class FailureAccumulator
{
    private FailureAccumulator() {}

    public static Throwable append(@Nullable Throwable current, @Nullable Throwable next)
    {
        if (current == null) return next;
        if (next == null) return current;
        current.addSuppressed(next);
        return current;
    }

    public static Throwable fail(Agent agent, @Nullable Throwable current, @Nullable TxnId txnId)
    {
        return fail(agent, current, txnId, (RoutingKey) null);
    }

    public static Throwable fail(Agent agent, @Nullable Throwable current, @Nullable TxnId txnId, @Nullable Route<?> route)
    {
        RoutingKey homeKey = route == null ? null : route.homeKey();
        return fail(agent, current, txnId, homeKey, null);
    }

    public static Throwable fail(Agent agent, @Nullable Throwable current, @Nullable TxnId txnId, @Nullable RoutingKey homeKey)
    {
        return fail(agent, current, txnId, homeKey, null);
    }

    public static Throwable fail(Agent agent, @Nullable Throwable current, @Nullable TxnId txnId, @Nullable RoutingKey homeKey, @Nullable Ranges unavailable)
    {
        return fail(agent, current, txnId, homeKey, unavailable, null);
    }
    public static Throwable fail(Agent agent, @Nullable Throwable current, @Nullable TxnId txnId, @Nullable RoutingKey homeKey, @Nullable Ranges unavailable, @Nullable Collection<Node.Id> failed)
    {
        if (current == null && unavailable == null && failed == null)
            return Timeout.timeout(agent, txnId, homeKey);
        if (current == null)
            return Exhausted.exhausted(agent, txnId, homeKey, unavailable, failed);
        return current;
    }
}
