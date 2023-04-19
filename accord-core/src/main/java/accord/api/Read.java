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

import accord.local.SafeCommandStore;
import accord.primitives.*;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.utils.async.AsyncChain;


/**
 * A read to be performed on potentially multiple shards, the inputs of which may be fed to a {@link Query}
 */
public interface Read
{
    /**
     * Returns the scope of the transaction, that is, the keys or ranges that will be read
     * // TODO maybe this method should be renamed to readScope, or just scope, or something else which would not mention keys as it can be ranges as well?
     */
    Seekables<?, ?> keys();

    /**
     * The method is called when Accord reads data.
     *
     * @param key denotes which data to read; it is guaranteed that the key passed there is one of the items covered by {@link #keys()}.
     */
    AsyncChain<Data> read(Seekable key, Txn.Kind kind, SafeCommandStore commandStore, Timestamp executeAt, DataStore store);

    /**
     * Returns a new read operation whose scope is an intersection of {@link #keys()} and the provided ranges.
     */
    Read slice(Ranges ranges);

    /**
     * Returns a new read operation whose scope is a union of the scopes of this and the provided read operations.
     */
    Read merge(Read other);
}
