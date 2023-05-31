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
import accord.api.Key;
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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Collections.synchronizedList;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MockStore implements DataStore
{
    private final Id nodeId;

    public MockStore(Id nodeId)
    {
        this.nodeId = nodeId;
    }

    public static class MockRepairWrites implements RepairWrites
    {
        private final Seekables keys;
        private final Write write;

        public MockRepairWrites(Keys keys, Write write)
        {
            this.keys = keys;
            this.write = write;
        }

        @Override
        public Seekables<?, ?> keys()
        {
            return keys;
        }

        @Override
        public Write toWrite()
        {
            return write;
        }

        @Override
        public boolean isEmpty()
        {
            return keys().isEmpty();
        }
    }

    public static class MockData extends ArrayList<Id> implements Data, UnresolvedData, Result
    {
        private Seekables keys;

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
            MockData mockData = (MockData)data;
            addAll(mockData);
            this.keys = keys.with(mockData.keys);
            return this;
        }

        @Override
        public UnresolvedData merge(UnresolvedData data)
        {
            return (MockData)merge((Data)data);
        }
    };

    public static final MockData EMPTY_DATA = new MockData();
    public static final Result RESULT = EMPTY_DATA;
    public static final Query QUERY = (txnId, executeAtEpoch, keys, data, read, update) -> RESULT;
    public static final DataResolver RESOLVER = new MockResolver(false, null, null);

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
            this.repairWrites = repairWrites != null ? repairWrites : null;
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
                    for (Key key : (Keys)read.keys())
                    {
                        followUpReader.read(read.slice(Ranges.of(key.toUnseekable().asRange())), id, new Callback<UnresolvedData>()
                        {
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
                    }
                    followupData.add(result);
                }
                return AsyncChains.reduce(followupData, UnresolvedData::merge).map(unresolve -> new ResolveResult((Data)unresolvedData, repairWrites));
            }
            return AsyncChains.success(new ResolveResult((Data)unresolvedData, repairWrites));
        }
    }

    public static class MockWrite implements Write
    {
        public final List<Key> appliedKeys = synchronizedList(new ArrayList<>());
        
        @Override
        public synchronized AsyncChain<Void> apply(Seekable key, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
        {
            appliedKeys.add((Key)key);
            return AsyncChains.success(null);
        }
    }

    public static class MockUpdate implements Update
    {
        final Write write;
        final Seekables keys;

        DataConsistencyLevel writeDataCL;

        Data data;
        RepairWrites repairWrites;

        public MockUpdate(Seekables keys, Write write, DataConsistencyLevel writeDataCL)
        {
            this.keys = keys;
            this.write = write;
            this.writeDataCL = writeDataCL;
        }

        @Override
        public Seekables<?, ?> keys()
        {
            return keys;
        }

        @Override
        public Write apply(@Nullable Data data, @Nonnull RepairWrites repairWrites)
        {
            assertNull(this.data);
            assertNull(this.repairWrites);
            this.data = data;
            this.repairWrites = repairWrites;
            return write;
        }

        @Override
        public Update slice(Ranges ranges)
        {
            return this;
        }

        @Override
        public Update merge(Update other)
        {
            return this;
        }

        @Override
        public DataConsistencyLevel writeDataCl()
        {
            return writeDataCL;
        }
    }

    public static Update update(Seekables<?, ?> keys, DataConsistencyLevel writeDataCL)
    {
        return new MockUpdate(keys, (key, commandStore, executeAt, store) -> Writes.SUCCESS, writeDataCL);
    }

    public static Update update(Seekables<?, ?> keys)
    {
        return update(keys, DataConsistencyLevel.UNSPECIFIED);
    }
}
