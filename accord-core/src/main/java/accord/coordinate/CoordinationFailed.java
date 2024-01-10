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
import accord.primitives.TxnId;

/**
 * Thrown when a transaction exceeds its specified timeout for obtaining a result for a client
 */
public class CoordinationFailed extends RuntimeException
{
    private @Nullable TxnId txnId;
    private @Nullable RoutingKey homeKey;
    public CoordinationFailed(@Nullable TxnId txnId, @Nullable RoutingKey homeKey)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
    }

    public CoordinationFailed(@Nullable TxnId txnId, @Nullable RoutingKey homeKey, String message)
    {
        super(message);
        this.txnId = txnId;
        this.homeKey = homeKey;
    }

    void set(TxnId txnId, RoutingKey homeKey)
    {
        if (this.txnId == null && txnId != null)
            this.txnId = txnId;

        if (this.homeKey == null && homeKey != null)
            this.homeKey = homeKey;
    }

    public @Nullable TxnId txnId()
    {
        return txnId;
    }

    public @Nullable RoutingKey homeKey()
    {
        return homeKey;
    }
}
