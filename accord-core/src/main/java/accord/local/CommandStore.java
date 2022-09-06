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

import accord.api.Agent;
import accord.api.Key;
import accord.local.CommandStores.ShardedRanges;
import accord.api.ProgressLog;
import accord.api.DataStore;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.utils.concurrent.Future;

import com.google.common.base.Preconditions;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore
{
    public interface Factory
    {
        CommandStore create(int generation,
                            int shardIndex,
                            int numShards,
                            Node node,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            RangesForEpoch rangesForEpoch);
    }

    public interface RangesForEpoch
    {
        KeyRanges at(long epoch);
        KeyRanges since(long epoch);
        boolean intersects(long epoch, Keys keys);
    }

    private final int generation;
    private final int shardIndex;
    private final int numShards;
    private final Node node;
    private final Agent agent;
    private final DataStore store;
    private final ProgressLog progressLog;
    private final RangesForEpoch rangesForEpoch;


    public CommandStore(int generation,
                        int shardIndex,
                        int numShards,
                        Node node,
                        Agent agent,
                        DataStore store,
                        ProgressLog.Factory progressLogFactory,
                        RangesForEpoch rangesForEpoch)
    {
        Preconditions.checkArgument(shardIndex < numShards);
        this.generation = generation;
        this.shardIndex = shardIndex;
        this.numShards = numShards;
        this.node = node;
        this.agent = agent;
        this.store = store;
        this.progressLog = progressLogFactory.create(this);
        this.rangesForEpoch = rangesForEpoch;
    }

    public abstract Command ifPresent(TxnId txnId);

    public abstract Command command(TxnId txnId);

    public abstract CommandsForKey commandsForKey(Key key);


    // TODO (now): is this needed?
    public abstract CommandsForKey maybeCommandsForKey(Key key);

    public DataStore store()
    {
        return store;
    }

    public Timestamp uniqueNow(Timestamp atLeast)
    {
        return node.uniqueNow(atLeast);
    }

    public Agent agent()
    {
        return agent;
    }

    public ProgressLog progressLog()
    {
        return progressLog;
    }

    public Node node()
    {
        return node;
    }

    public RangesForEpoch ranges()
    {
        return rangesForEpoch;
    }

    public long latestEpoch()
    {
        // TODO: why not inject the epoch to each command store?
        return node.epoch();
    }

    protected Timestamp maxConflict(Keys keys)
    {
        return keys.stream()
                   .map(this::maybeCommandsForKey)
                   .filter(Objects::nonNull)
                   .map(CommandsForKey::max)
                   .max(Comparator.naturalOrder())
                   .orElse(Timestamp.NONE);
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

    public boolean hashIntersects(Key key)
    {
        return ShardedRanges.keyIndex(key, numShards) == shardIndex;
    }

    public boolean intersects(Keys keys, KeyRanges ranges)
    {
        return keys.any(ranges, this::hashIntersects);
    }

    public static void onEach(TxnOperation scope, Collection<CommandStore> stores, Consumer<? super CommandStore> consumer)
    {
        for (CommandStore store : stores)
            store.process(scope, consumer);
    }

    /**
     * for interacting with the command store in a threadsafe way, without needing any txns loaded. For configuration
     */
    public abstract Future<Void> processSetup(Consumer<? super CommandStore> function);

    public abstract <T> Future<T> processSetup(Function<? super CommandStore, T> function);

    public abstract Future<Void> process(TxnOperation scope, Consumer<? super CommandStore> consumer);

    public abstract <T> Future<T> process(TxnOperation scope, Function<? super CommandStore, T> function);

    public abstract void shutdown();

}
