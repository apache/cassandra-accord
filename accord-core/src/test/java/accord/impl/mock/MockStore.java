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
import accord.api.DataResolver;
import accord.api.DataStore;
import accord.api.Query;
import accord.api.Read;
import accord.api.RepairWrites;
import accord.api.ResolveResult;
import accord.api.Result;
import accord.api.UnresolvedData;
import accord.api.Update;
import accord.api.Write;
import accord.local.SafeCommandStore;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Writes;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

public class MockStore implements DataStore
{
    public static final Data DATA = new Data() {
        @Override
        public Data merge(Data data)
        {
            return DATA;
        }
    };
    public static final UnresolvedData READ_RESULT = new UnresolvedData() {
        @Override
        public UnresolvedData merge(UnresolvedData data)
        {
            return READ_RESULT;
        }
    };
    public static final Result RESULT = new Result() {};
    public static final Query QUERY = (txnId, executeAtEpoch, keys, data, read, update) -> RESULT;
    public static final DataResolver READ_RESOLVER = (read, readResults, followupReader) -> AsyncChains.success(new ResolveResult(DATA, null));

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
            public AsyncChain<UnresolvedData> read(Seekable key, boolean digestRead, Txn.Kind kind, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
            {
                return AsyncChains.success(READ_RESULT);
            }

            @Override
            public Read slice(Ranges ranges)
            {
                return MockStore.read(keys.slice(ranges));
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
            public Write apply(Data data, RepairWrites repairWrites)
            {
                return new Write() {

                    @Override
                    public Seekables<?, ?> keys()
                    {
                        return keys;
                    }

                    @Override
                    public AsyncChain<Void> apply(Seekable key, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
                    {
                        return Writes.SUCCESS;
                    }
                };
            }

            @Override
            public Update slice(Ranges ranges)
            {
                return MockStore.update(keys.slice(ranges));
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
}
