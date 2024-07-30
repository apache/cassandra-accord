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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import accord.api.Write;
import accord.local.SafeCommandStore;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import javax.annotation.Nullable;

public class Writes
{
    public static final AsyncChain<Void> SUCCESS = AsyncChains.success(null);
    public final TxnId txnId;
    public final Timestamp executeAt;
    public final Seekables<?, ?> keys;
    @Nullable public final Write write;

    public Writes(TxnId txnId, Timestamp executeAt, Seekables<?, ?> keys, @Nullable Write write)
    {
        this.txnId = txnId;
        this.executeAt = executeAt;
        this.keys = keys;
        this.write = write;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Writes writes = (Writes) o;
        return txnId.equals(writes.txnId) && executeAt.equals(writes.executeAt) && keys.equals(writes.keys) && Objects.equals(write, writes.write);
    }

    public boolean isEmpty()
    {
        return keys.isEmpty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(txnId, executeAt, keys, write);
    }

    public AsyncChain<Void> apply(SafeCommandStore safeStore, Ranges ranges, PartialTxn txn)
    {
        if (write == null)
            return SUCCESS;

        if (ranges.isEmpty())
            return SUCCESS;

        List<AsyncChain<Void>> futures = Routables.foldl(keys, ranges, (key, accumulate, index) -> {
            accumulate.add(write.apply(key, safeStore, executeAt, safeStore.dataStore(), txn));
            return accumulate;
        }, new ArrayList<>());

        if (futures.isEmpty())
            return SUCCESS;

        return AsyncChains.reduce(futures, (l, r) -> null);
    }

    @Override
    public String toString()
    {
        return "TxnWrites{" +
               "txnId:" + txnId +
               ", executeAt:" + executeAt +
               ", keys:" + keys +
               ", write:" + write +
               '}';
    }
}
