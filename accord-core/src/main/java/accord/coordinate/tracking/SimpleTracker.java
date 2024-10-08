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

package accord.coordinate.tracking;

import java.util.function.Function;
import java.util.function.IntFunction;

import accord.local.Node.Id;
import accord.topology.Shard;
import accord.topology.Topologies;

import static accord.coordinate.tracking.RequestStatus.NoChange;

public abstract class SimpleTracker<ST extends ShardTracker> extends PreAcceptTracker<ST>
{
    public SimpleTracker(Topologies topologies, IntFunction<ST[]> arrayFactory, Function<Shard, ST> trackerFactory)
    {
        super(topologies, arrayFactory, trackerFactory);
    }

    public SimpleTracker(Topologies topologies, IntFunction<ST[]> arrayFactory, ShardFactory<ST> trackerFactory)
    {
        super(topologies, arrayFactory, trackerFactory);
    }

    public RequestStatus recordSuccess(Id from, boolean withFastPathTimestamp) { return recordSuccess(from); }
    public RequestStatus recordDelayed(Id from) { return NoChange; }
    public boolean hasFastPathAccepted() { return false; }

    public abstract RequestStatus recordSuccess(Id from);
    public abstract RequestStatus recordFailure(Id from);
}
