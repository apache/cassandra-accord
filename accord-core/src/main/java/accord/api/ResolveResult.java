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

import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.nonNull;

/**
 * Result of resolving UnresolvedData and fetching the full Data in DataResolver
 *
 * May contain repair writes which need to be persisted before any writes of this transaction
 * and acknowledging the txn as committed. The transaction should not be acknowledged until the repair
 * writes are applied at the requested read consistency level.
 */
public class ResolveResult
{
    @Nonnull
    public final Data data;
    @Nullable
    public final RepairWrites repairWrites;

    public ResolveResult(@Nonnull Data data, @Nullable RepairWrites repairWrites)
    {
        nonNull(data);
        checkArgument(repairWrites == null || !repairWrites.isEmpty());
        this.data = data;
        this.repairWrites = repairWrites;
    }
}
