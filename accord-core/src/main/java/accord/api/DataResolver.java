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

import accord.local.Node.Id;
import accord.messages.Callback;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncChain;

/**
 * Process the result of Accord having performed a read and merge the results
 * producing any repair writes necessary for the read to be monotonic.
 *
 * May repeat the read in order to produce the repair writes.
 */
public interface DataResolver
{
    /**
     * Allow the resolver to request additional or redundant/repeated data reads from specific nodes
     * in order to support things like read repair and short read protection.
     */
    interface FollowupReader
    {
        void read(Read read, Id id, Callback<UnresolvedData> callback);
    }

    AsyncChain<ResolveResult> resolve(Timestamp executeAt, Read read, UnresolvedData unresolvedData, FollowupReader followUpReader);
}
