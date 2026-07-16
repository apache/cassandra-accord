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

package accord.api;

import javax.annotation.Nullable;

import accord.local.CommandStore;
import accord.local.SafeCommandStore;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncChain;

/**
 * A read to be performed on potentially multiple shards, the inputs of which may be fed to a {@link Query}
 */
public interface Read
{
    Seekables<?, ?> keys();

    /**
     * Issue a potentially-asynchronous read against the underlying data store while owning the SafeCommandStore.
     * The result is expected to be non-null unless the store determined the data was stale.
     * @param key MAY BE NULL indicating a no-op result should be returned
     */
    AsyncChain<Data> read(SafeCommandStore safeStore, @Nullable Seekable key, Timestamp executeAt);

    /**
     * Issue a potentially-asynchronous read against the underlying data store, while not owning the SafeCommandStore.
     * This method is only invoked if {@link ProtocolModifiers.Toggles#fastReadsMayBypassSafeStore()} is true.
     * The result is expected to be non-null unless the store determined the data was stale.
     * @param key MAY BE NULL indicating a no-op result should be returned
     */
    default AsyncChain<Data> readDirect(CommandStore commandStore, @Nullable Seekable key, Timestamp executeAt) { throw new UnsupportedOperationException(); }
    Read slice(Ranges ranges);
    Read intersecting(Participants<?> participants);
    Read merge(Read other);
    default boolean isUsedForImport() { return false; }
}
