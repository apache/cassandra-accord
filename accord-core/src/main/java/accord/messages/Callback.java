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

package accord.messages;

import accord.local.Node.Id;

/**
 * Represents some execution for handling responses from messages a node has sent.
 * TODO: associate a Callback with a CommandShard or other context for execution (for coordination, usually its home shard)
 */
public interface Callback<T>
{
    void onSuccess(Id from, T reply);
    default void onSlowResponse(Id from) {}
    void onFailure(Id from, Throwable failure);
    void onCallbackFailure(Id from, Throwable failure);
}
