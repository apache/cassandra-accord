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
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.utils.Invariants.checkState;

/**
 * Thrown when a coordinator encounters a phase that is ahead of the one it is attempting to progress.
 */
public class Redundant extends Preempted
{
    public final Timestamp committedExecuteAt;
    public Redundant(TxnId txnId, @Nullable RoutingKey homeKey, Timestamp committedExecuteAt)
    {
        super(txnId, homeKey);
        this.committedExecuteAt = committedExecuteAt;
    }

    Redundant(TxnId txnId, @Nullable RoutingKey homeKey, Redundant cause, Timestamp committedExecuteAt)
    {
        super(txnId, homeKey, cause);
        this.committedExecuteAt = committedExecuteAt;
    }

    @Override
    public Redundant wrap()
    {
        checkState(this.getClass() == Redundant.class);
        return new Redundant(txnId(), homeKey(), this, committedExecuteAt);
    }
}
