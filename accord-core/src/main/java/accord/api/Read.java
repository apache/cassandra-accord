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
    AsyncChain<Data> read(Seekable key, SafeCommandStore commandStore, Timestamp executeAt, DataStore store);
    Read slice(Ranges ranges);
    Read merge(Read other);
}
