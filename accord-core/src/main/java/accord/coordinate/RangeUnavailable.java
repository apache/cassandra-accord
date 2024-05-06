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

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.Ranges;
import accord.primitives.TxnId;

import static accord.utils.Invariants.checkState;

public class RangeUnavailable extends Exhausted
{
    public final Ranges unavailable;

    public RangeUnavailable(Ranges unavailable, TxnId txnId)
    {
        super(txnId, null, buildMessage(unavailable));
        this.unavailable = unavailable;
    }

    private RangeUnavailable(Ranges unavailable, TxnId txnId, @Nullable RoutingKey homeKey, RangeUnavailable cause)
    {
        super(txnId, homeKey, cause);
        this.unavailable = unavailable;
    }

    @Override
    public RangeUnavailable wrap()
    {
        checkState(this.getClass() == RangeUnavailable.class);
        return new RangeUnavailable(unavailable, txnId(), homeKey(), this);
    }

    private static String buildMessage(Ranges unavailable)
    {
        return "The following ranges are unavailable to read: " + unavailable;
    }
}
