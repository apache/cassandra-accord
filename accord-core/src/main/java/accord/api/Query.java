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

package accord.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

/**
 * The computational/transformation part of a client query
 */
public interface Query
{
    /**
     * Perform some transformation on the complete {@link Data} result of a {@link Read}
     * from some {@link DataStore}, to produce a {@link Result} to return to the client.
     *
     * executeAt timestamp is provided so that the result of the transaction can be determined based
     * on configuration at that epoch. This will be deterministic even if the transaction is recovered.
     */
    Result compute(@Nonnull TxnId txnId, @Nonnull Timestamp executeAt, @Nonnull Seekables<?, ?> keys, @Nullable Data data, @Nullable Read read, @Nullable Update update);
}
