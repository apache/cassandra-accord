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

import accord.primitives.Ranges;
import accord.primitives.Seekables;

import javax.annotation.Nullable;

/**
 * A client-defined update operation (the write equivalent of a query).
 */
public interface Update
{
    /**
     * Returns the scope of the transaction, that is, the keys or ranges that will be written
     */
    //TODO maybe this method should be renamed to updateScope, or just scope, or something else which would not mention keys as it can be ranges as well?
    Seekables<?, ?> keys();

    /**
     * Takes as input the data returned by {@link Read}, and returns a {@link Write} representing new information
     * to be distributed to each shard's stores. Accepts {@code null} if nothing was read.
     */
    Write apply(@Nullable Data data);

    /**
     * Returns a new update operation whose scope is an intersection of {@link #keys()} and the provided ranges.
     */
    Update slice(Ranges ranges);

    /**
     * Returns a new update operation whose scope is a union of the scopes of this and the provided update operations.
     */
    Update merge(Update other);
}
