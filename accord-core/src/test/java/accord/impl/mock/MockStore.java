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

package accord.impl.mock;

import accord.api.Data;
import accord.api.DataStore;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Update;
import accord.api.Write;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

public class MockStore implements DataStore
{
    public static final Data DATA = new Data() {
        @Override
        public Data merge(Data data)
        {
            return DATA;
        }
    };

    public static final Result RESULT = new Result() {};
    public static final Query QUERY = (txnId, executeAt, keys, data, read, update) -> RESULT;
    public static final Write WRITE = (key, commandStore, executeAt, store, command) -> Writes.SUCCESS;

    public static Read read(Seekables<?, ?> keys)
    {
        return new Read()
        {
            @Override
            public Seekables<?, ?> keys()
            {
                return keys;
            }

            @Override
            public AsyncChain<Data> read(Seekable key, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
            {
                return AsyncChains.success(DATA);
            }

            @Override
            public Read slice(Ranges ranges)
            {
                return MockStore.read(keys.slice(ranges));
            }

            @Override
            public Read intersecting(Participants<?> participants)
            {
                return MockStore.read(keys.intersecting(participants));
            }

            @Override
            public Read merge(Read other)
            {
                return MockStore.read(((Seekables)keys).with(other.keys()));
            }

            @Override
            public String toString()
            {
                return keys.toString();
            }
        };
    }

    public static Update update(Seekables<?, ?> keys)
    {
        return new Update()
        {
            @Override
            public Seekables<?, ?> keys()
            {
                return keys;
            }

            @Override
            public Write apply(Timestamp executeAt, Data data)
            {
                return WRITE;
            }

            @Override
            public Update slice(Ranges ranges)
            {
                return MockStore.update(keys.slice(ranges));
            }

            @Override
            public Update intersecting(Participants<?> participants)
            {
                return MockStore.update(keys.intersecting(participants));
            }

            @Override
            public Update merge(Update other)
            {
                return MockStore.update(((Seekables)keys).with(other.keys()));
            }

            @Override
            public String toString()
            {
                return keys.toString();
            }
        };
    }

    static class ImmediateFetchFuture extends AsyncResults.SettableResult<Ranges> implements FetchResult
    {
        ImmediateFetchFuture(Ranges ranges) { setSuccess(ranges); }
        @Override public void abort(Ranges abort) { }
    }

    @Override
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback)
    {
        callback.starting(ranges).started(Timestamp.NONE);
        callback.fetched(ranges);
        return new ImmediateFetchFuture(ranges);
    }

    @Override
    public AsyncResult<Void> snapshot(Ranges ranges, TxnId before)
    {
        return AsyncResults.success(null);
    }
}
