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

package accord.maelstrom;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import accord.api.Key;
import accord.api.DataStore;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.TxnId;
import accord.utils.Timestamped;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.AsyncResults.SettableResult;

public class MaelstromStore implements DataStore
{
    final Map<Key, Timestamped<Value>> data = new ConcurrentHashMap<>();

    public Value read(Key key)
    {
        Timestamped<Value> v = data.get(key);
        return v == null ? Value.EMPTY : v.data;
    }

    public Value get(Key key)
    {
        Timestamped<Value> v = data.get(key);
        return v == null ? Value.EMPTY : v.data;
    }

    static class ImmediateFetchResult extends SettableResult<Ranges> implements FetchResult
    {
        ImmediateFetchResult(Ranges ranges) { setSuccess(ranges); }
        @Override public void abort(Ranges abort) { }
    }

    @Override
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback)
    {
        return new ImmediateFetchResult(ranges);
    }

    @Override
    public AsyncResult<Void> snapshot(Ranges ranges, TxnId before)
    {
        return AsyncResults.success(null);
    }
}
