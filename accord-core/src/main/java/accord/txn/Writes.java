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

package accord.txn;

import accord.api.Write;
import accord.local.CommandStore;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.KeyRanges;
import accord.utils.ReducingFuture;
import com.google.common.base.Preconditions;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Writes
{
    public static final Future<Void> SUCCESS = ImmediateFuture.success(null);
    public final Timestamp executeAt;
    public final Keys keys;
    public final Write write;

    public Writes(Timestamp executeAt, Keys keys, Write write)
    {
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
        return executeAt.equals(writes.executeAt) && keys.equals(writes.keys) && write.equals(writes.write);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(executeAt, keys, write);
    }

    public Future<Void> apply(CommandStore commandStore)
    {
        if (write == null)
            return SUCCESS;

        KeyRanges ranges = commandStore.ranges().since(executeAt.epoch);
        if (ranges == null)
            return SUCCESS;

        List<Future<Void>> futures = keys.foldl(ranges, (index, key, accumulate) -> {
            if (commandStore.hashIntersects(key))
                accumulate.add(write.apply(key, commandStore, executeAt, commandStore.store()));
            return accumulate;
        }, new ArrayList<>());
        return ReducingFuture.reduce(futures, (l, r) -> null);
    }

    @Override
    public String toString()
    {
        return "TxnWrites{" +
               "executeAt:" + executeAt +
               ", keys:" + keys +
               ", write:" + write +
               '}';
    }
}
