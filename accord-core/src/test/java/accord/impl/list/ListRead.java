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

import accord.local.SafeCommandStore;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.Timestamped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.Read;
import accord.local.CommandStore;
import accord.primitives.Range;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.utils.async.AsyncExecutor;

public class ListRead implements Read
{
    private static final Logger logger = LoggerFactory.getLogger(ListRead.class);

    private final Function<? super CommandStore, AsyncExecutor> executor;
    private final boolean isEphemeralRead;
    public final Seekables<?, ?> userReadKeys; // those only to be returned to user
    public final Seekables<?, ?> keys; // those including necessary for writes

    public ListRead(Function<? super CommandStore, AsyncExecutor> executor, boolean isEphemeralRead, Seekables<?, ?> userReadKeys, Seekables<?, ?> keys)
    {
        this.executor = executor;
        this.isEphemeralRead = isEphemeralRead;
        this.userReadKeys = userReadKeys;
        this.keys = keys;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return keys;
    }

    @Override
    public AsyncChain<Data> read(Seekable key, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
    {
        // read synchronously, logically taking a snapshot, so we can impose our invariant of not reading the future
        ListStore s = (ListStore)store;
        Ranges unavailable = safeStore.ranges().unsafeToReadAt(executeAt);
        // TODO (now, correctness): move the read into the executor thread to match real impl
        // There is a bug (link jira) where the stale read handle logic no longer detects and fails with the new assert below
        // There is a comment early about running synchronously, but this isn't easy for different implementations so should likely
        // be an optimization impl take rather than a foundational requirement...
        ListData result = new ListData();
        switch (key.domain())
        {
            default: throw new AssertionError();
            case Key:
                Timestamped<int[]> data = s.get(unavailable, executeAt, (Key)key);
                logger.trace("READ on {} at {} key:{} -> {}", s.node, executeAt, key, data);
                Invariants.checkState(isEphemeralRead || data.timestamp.compareTo(executeAt) < 0,
                                      "Data timestamp %s >= execute at %s", data.timestamp, executeAt);
                result.put((Key)key, data);
                break;
            case Range:
                for (Map.Entry<Key, Timestamped<int[]>> e : s.get(unavailable, executeAt, (Range)key))
                    result.put(e.getKey(), e.getValue());
        }
        return executor.apply(safeStore.commandStore()).submit(() -> result);
    }

    @Override
    public Read slice(Ranges ranges)
    {
        return new ListRead(executor, isEphemeralRead, userReadKeys.slice(ranges), keys.slice(ranges));
    }

    @Override
    public Read merge(Read other)
    {
        return new ListRead(executor, isEphemeralRead, ((Seekables) userReadKeys).with(((ListRead)other).userReadKeys), ((Seekables)keys).with(((ListRead)other).keys));
    }

    @Override
    public String toString()
    {
        return keys.toString();
    }
}
