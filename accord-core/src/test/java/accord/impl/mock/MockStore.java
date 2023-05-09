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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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
import accord.local.Node.Id;
import accord.local.SafeCommandStore;
import accord.messages.Callback;
import accord.primitives.DataConsistencyLevel;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Writes;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResults.SettableResult;

import static java.util.Collections.synchronizedList;

public class MockStore implements DataStore
{
    private final Id nodeId;

    public MockStore(Id nodeId)
    {
        this.nodeId = nodeId;
    }

    public static class MockData extends ArrayList<Id> implements Data, UnresolvedData, RepairWrites, Result
    {
        private Seekables keys;

        public List<Seekable> appliedKeys = synchronizedList(new ArrayList<>());

        public MockData()
        {
            this(null, null);
        }

        public MockData(Id id, Seekables keys)
        {
            if (id != null)
                add(id);
            if (keys == null)
                this.keys = Keys.EMPTY;
            else
                this.keys = keys;
        }

        @Override
        public Data merge(Data data)
        {
            addAll((MockData)data);
            this.keys = keys.with(((MockData)data).keys);
            return this;
        }

        @Override
        public UnresolvedData merge(UnresolvedData data)
        {
            return (MockData)merge((Data)data);
        }

        @Override
        public Seekables<?, ?> keys()
        {
            return keys;
        }

        @Override
        public Write toWrite()
        {
            return new Write() {

                @Override
                public AsyncChain<Void> apply(Seekable key, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
                {
                    appliedKeys.add(key);
                    return AsyncChains.success(null);
                }
            };
        }
    };

    public static final MockData EMPTY = new MockData();
    public static final Result RESULT = EMPTY;
    public static final Query QUERY = (txnId, executeAtEpoch, keys, data, read, update) -> RESULT;
    public static final DataResolver RESOLVER = new MockResolver(false, EMPTY, null);

    public static final Query QUERY_RETURNING_INPUT = (txnId, executeAtEpoch, keys, data, read, update) -> (MockData)data;

    public static Read read(Seekables<?, ?> keys)
    {
        return read(keys, null, null);
    }

    public static Read read(Seekables<?, ?> keys, Consumer<Boolean> digestReadListener, DataConsistencyLevel cl)
    {
        return new MockRead(keys, digestReadListener, cl);
    }

    public static class MockRead implements Read
    {
        public final Seekables keys;
        public final Consumer<Boolean> digestReadListener;
        public final DataConsistencyLevel cl;

        public MockRead(Seekables keys, Consumer<Boolean> digestReadListener, DataConsistencyLevel cl)
        {
            this.keys = keys;
            this.digestReadListener = digestReadListener;
            this.cl = cl;
        }

        @Override
        public Seekables<?, ?> keys()
        {
            return keys;
        }

        @Override
        public AsyncChain<UnresolvedData> read(Seekable key, boolean digestRead, Txn.Kind kind, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
        {
            if (digestReadListener != null)
                digestReadListener.accept(digestRead);
            return AsyncChains.success(new MockData(((MockStore)store).nodeId, Seekables.of(key)));
        }

        @Override
        public Read slice(Ranges ranges)
        {
            return MockStore.read(keys.slice(ranges), digestReadListener, cl);
        }

        @Override
        public Read merge(Read other)
        {
            return MockStore.read(((Seekables)keys).with(other.keys()), digestReadListener, cl);
        }

        @Override
        public String toString()
        {
            return keys.toString();
        }

        @Override
        public DataConsistencyLevel readDataCL()
        {
            if (cl != null)
                return cl;
            return DataConsistencyLevel.UNSPECIFIED;
        }
    }

    public static class MockFollowupRead
    {
        final Id node;
        final Seekables keys;

        public MockFollowupRead(Id node, Seekables keys)
        {
            this.node = node;
            this.keys = keys;
        }
    }

    public static class MockResolver implements DataResolver
    {
        public final boolean alwaysFollowup;
        public final RepairWrites repairWrites;
        public final MockFollowupRead mockFollowupRead;

        public UnresolvedData unresolvedData;

        public MockResolver(boolean alwaysFollowup, RepairWrites repairWrites, MockFollowupRead mockFollowupRead)
        {
            this.alwaysFollowup = alwaysFollowup;
            this.repairWrites = repairWrites != null ? repairWrites : EMPTY;
            this.mockFollowupRead = mockFollowupRead;
        }

        @Override
        public AsyncChain<ResolveResult> resolve(Timestamp executeAt, Read read, UnresolvedData unresolvedData, FollowupReader followUpReader)
        {
            this.unresolvedData = unresolvedData;
            List<AsyncChain<UnresolvedData>> followupData = new ArrayList<>();
            if (mockFollowupRead != null)
            {
                SettableResult<UnresolvedData> result = new SettableResult<>();
                followUpReader.read(new MockRead(mockFollowupRead.keys, null, DataConsistencyLevel.INVALID), mockFollowupRead.node, new Callback<UnresolvedData>(){
                    @Override
                    public void onSuccess(Id from, UnresolvedData reply)
                    {
                        result.trySuccess(reply);
                    }

                    @Override
                    public void onFailure(Id from, Throwable failure)
                    {
                        result.tryFailure(failure);
                    }

                    @Override
                    public void onCallbackFailure(Id from, Throwable failure)
                    {
                        failure.printStackTrace();
                    }
                });
                followupData.add(result);
            }
            if (alwaysFollowup)
            {
                MockData mockData = (MockData)unresolvedData;
                for (Id id : mockData)
                {
                    SettableResult<UnresolvedData> result = new SettableResult<>();
                    followUpReader.read(read, id, new Callback<UnresolvedData>(){
                        @Override
                        public void onSuccess(Id from, UnresolvedData reply)
                        {
                            result.trySuccess(reply);
                        }

                        @Override
                        public void onFailure(Id from, Throwable failure)
                        {
                            result.tryFailure(failure);
                        }

                        @Override
                        public void onCallbackFailure(Id from, Throwable failure)
                        {
                            failure.printStackTrace();
                        }
                    });
                    followupData.add(result);
                }
                return AsyncChains.reduce(followupData, UnresolvedData::merge).map(unresolve -> new ResolveResult((Data)unresolvedData, repairWrites));
            }
            return AsyncChains.success(new ResolveResult((Data)unresolvedData, repairWrites));
        }
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
