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

package accord.impl.list;

import java.util.Arrays;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import accord.impl.*;
import accord.primitives.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.DataStore;
import accord.api.Key;
import accord.api.Write;
import accord.local.CommandStore;
import accord.local.SafeCommandStore;
import accord.utils.Timestamped;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncExecutor;

public class ListWrite extends TreeMap<Key, int[]> implements Write
{
    private static final Logger logger = LoggerFactory.getLogger(ListWrite.class);

    private final Function<? super CommandStore, AsyncExecutor> executor;

    public ListWrite(Function<? super CommandStore, AsyncExecutor> executor)
    {
        this.executor = executor;
    }

    @Override
    public AsyncChain<Void> apply(Seekable key, SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt, DataStore store, PartialTxn txn)
    {
        ListStore s = (ListStore) store;
        if (!containsKey(key))
            return Writes.SUCCESS;
        TimestampsForKeys.updateLastExecutionTimestamps((AbstractSafeCommandStore<?, ?, ?>) safeStore, ((Key)key).toUnseekable(), txnId, executeAt, true);

        logger.trace("submitting WRITE on {} at {} key:{}", s.node, executeAt, key);
        return executor.apply(safeStore.commandStore()).submit(() -> {
            int[] data = get(key);
            s.data.merge((Key)key, new Timestamped<>(executeAt, data, Arrays::toString), ListStore::merge);
            logger.trace("WRITE on {} at {} key:{} -> {}", s.node, executeAt, key, data);
            return null;
        });
    }

    public void applyUnsafe(Seekable key, SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt, DataStore store, PartialTxn txn)
    {
        ListStore s = (ListStore) store;
        if (!containsKey(key))
            return;

        TimestampsForKeys.updateLastExecutionTimestamps((AbstractSafeCommandStore<?, ?, ?>) safeStore, ((Key)key).toUnseekable(), txnId, executeAt, true);
        logger.trace("unsafe applying WRITE on {} at {} key:{}", s.node, executeAt, key);
        int[] data = get(key);
        s.data.merge((Key)key, new Timestamped<>(executeAt, data, Arrays::toString), ListStore::merge);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) return true;
        if (!(o instanceof ListWrite)) return false;
        ListWrite other = (ListWrite) o;
        // Can not rely on Map.equals as our value is an array: (new int[] {2}).equals(new int[] {2}) == false!
        if (!Sets.difference(keySet(), other.keySet()).isEmpty()
            || !Sets.difference(other.keySet(), keySet()).isEmpty())
            return false;
        // keys match
        for (Key k : keySet())
        {
            if (!Arrays.equals(get(k), other.get(k)))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return entrySet().stream()
                         .map(e -> e.getKey() + ":" + Arrays.toString(e.getValue()))
                         .collect(Collectors.joining(", ", "{", "}"));
    }
}
