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

import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.Read;
import accord.local.CommandStore;
import accord.local.SafeCommandStore;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncExecutor;

public class ListRead implements Read
{
    private static final Logger logger = LoggerFactory.getLogger(ListRead.class);

    private final Function<? super CommandStore, AsyncExecutor> executor;
    public final Seekables<?, ?> readKeys;
    public final Seekables<?, ?> keys;

    public ListRead(Function<? super CommandStore, AsyncExecutor> executor, Seekables<?, ?> readKeys, Seekables<?, ?> keys)
    {
        this.executor = executor;
        this.readKeys = readKeys;
        this.keys = keys;
    }

    @Override
    public Seekables keys()
    {
        return keys;
    }

    @Override
    public AsyncChain<Data> read(Seekable key, Txn.Kind kind, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
    {
        ListStore s = (ListStore)store;
        return executor.apply(commandStore.commandStore()).submit(() -> {
            ListData result = new ListData();
            switch (key.domain())
            {
                default: throw new AssertionError();
                case Key:
                    int[] data = s.get((Key)key);
                    logger.trace("READ on {} at {} key:{} -> {}", s.node, executeAt, key, data);
                    result.put((Key)key, data);
                    break;
                case Range:
                    for (Map.Entry<Key, int[]> e : s.get((Range)key))
                        result.put(e.getKey(), e.getValue());
            }
            return result;
        });
    }

    @Override
    public Read slice(Ranges ranges)
    {
        return new ListRead(executor, readKeys.slice(ranges), keys.slice(ranges));
    }

    @Override
    public Read merge(Read other)
    {
        return new ListRead(executor, ((Seekables)readKeys).with(((ListRead)other).readKeys), ((Seekables)keys).with(((ListRead)other).keys));
    }

    @Override
    public String toString()
    {
        return keys.toString();
    }
}
