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

package accord.local;

import accord.api.*;
import accord.local.CommandStores.ShardedRanges;
import accord.primitives.AbstractKeys;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import org.apache.cassandra.utils.concurrent.Future;

import com.google.common.base.Preconditions;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore
{
    public interface Factory
    {
        CommandStore create(int id,
                            int generation,
                            int shardIndex,
                            int numShards,
                            NodeTimeService time,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            RangesForEpoch rangesForEpoch);
    }

    public interface RangesForEpoch
    {
        KeyRanges at(long epoch);
        KeyRanges between(long fromInclusive, long toInclusive);
        KeyRanges since(long epoch);
        boolean owns(long epoch, RoutingKey key);
        boolean intersects(long epoch, AbstractKeys<?, ?> keys);
    }

    private final int id; // unique id
    private final int generation;
    private final int shardIndex;
    private final int numShards;

    public CommandStore(int id,
                        int generation,
                        int shardIndex,
                        int numShards)
    {
        Preconditions.checkArgument(shardIndex < numShards);
        this.id = id;
        this.generation = generation;
        this.shardIndex = shardIndex;
        this.numShards = numShards;
    }

    public int id()
    {
        return id;
    }

    // TODO (now): rename to shardIndex
    public int index()
    {
        return shardIndex;
    }

    // TODO (now): rename to shardGeneration
    public int generation()
    {
        return generation;
    }

    public boolean hashIntersects(RoutingKey key)
    {
        return ShardedRanges.keyIndex(key, numShards) == shardIndex;
    }

    public boolean intersects(Keys keys, KeyRanges ranges)
    {
        return keys.any(ranges, this::hashIntersects);
    }

    public abstract Agent agent();
    public abstract Future<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer);
    public abstract <T> Future<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply);
    public abstract void shutdown();
}
