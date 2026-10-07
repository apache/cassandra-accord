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

package accord.topology;

import accord.api.TopologySorter;
import accord.local.Node;
import accord.primitives.Routables;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.IndexedBiFunction;

import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithoutDeps;
import static accord.primitives.TxnId.FastPath.Unoptimised;

abstract class TopologyCollector<C, K, T, E extends Exception>
{
    abstract C allocate(int size);
    abstract C update(C collector, ActiveEpoch e, K select) throws E;
    // TODO (expected): do we really need updateIfExists?
    abstract C updateIfExists(C collector, ActiveEpoch e, K select);
    T none() { throw new UnsupportedOperationException(); }
    abstract T one(ActiveEpoch e, K select) throws E;
    abstract T multi(C collector);
    abstract T retired(long requestedEpoch, long minEpoch) throws E;
    abstract T notReady(long requestedEpoch, long maxEpoch) throws E;
    Topology selects(ActiveEpoch epoch)
    {
        return epoch.all;
    }

    static class Simple extends TopologyCollector<Topologies.Builder, Routables<?>, Topologies, TopologyException>
    {
        final TopologySorter.Supplier sorter;
        final SelectShards selectShards;

        Simple(TopologySorter.Supplier sorter, SelectShards selectShards)
        {
            this.sorter = sorter;
            this.selectShards = selectShards;
        }

        @Override
        public Topologies.Builder update(Topologies.Builder collector, ActiveEpoch e, Routables<?> select) throws TopologyException
        {
            collector.add(e.get(selectShards).select(select));
            return collector;
        }

        @Override
        public Topologies.Builder updateIfExists(Topologies.Builder collector, ActiveEpoch e, Routables<?> select)
        {
            collector.add(e.get(selectShards).selectIfExists(select));
            return collector;
        }

        @Override
        public Topologies one(ActiveEpoch e, Routables<?> unseekables) throws TopologyMismatch
        {
            return new Topologies.Single(sorter, e.get(selectShards).select(unseekables));
        }

        @Override
        public Topologies multi(Topologies.Builder builder)
        {
            return builder.build(sorter);
        }

        @Override
        Topologies retired(long requestedEpoch, long minEpoch) throws TopologyException
        {
            throw new TopologyRetiredException(requestedEpoch, minEpoch);
        }

        @Override
        Topologies notReady(long requestedEpoch, long maxEpoch) throws TopologyException
        {
            throw new TopologyNotReadyException(requestedEpoch, maxEpoch);
        }

        @Override
        Topology selects(ActiveEpoch epoch)
        {
            return epoch.get(selectShards);
        }

        @Override
        public Topologies.Builder allocate(int count)
        {
            return new Topologies.Builder(count);
        }
    }

    static class BestFastPath extends TopologyCollector<TxnId.FastPath, Routables<?>, TxnId.FastPath, RuntimeException> implements IndexedBiFunction<Shard, Boolean, Boolean>
    {
        final Node.Id self;

        BestFastPath(Node.Id self)
        {
            this.self = self;
        }

        @Override
        public TxnId.FastPath update(TxnId.FastPath collector, ActiveEpoch e, Routables<?> select)
        {
            return merge(collector, one(e, select));
        }

        @Override
        public TxnId.FastPath updateIfExists(TxnId.FastPath collector, ActiveEpoch e, Routables<?> select)
        {
            return update(collector, e, select);
        }

        @Override
        public TxnId.FastPath one(ActiveEpoch e, Routables<?> routables)
        {
            if (!e.local.ranges.containsAll(routables) || !e.local.foldl(routables, this, true))
                return Unoptimised;

            return e.local.foldl(routables, (s, v, i) -> merge(v, s.bestFastPath()), null);
        }

        @Override
        public TxnId.FastPath multi(TxnId.FastPath result)
        {
            return result;
        }

        @Override
        TxnId.FastPath retired(long requestedEpoch, long minEpoch)
        {
            return Unoptimised;
        }

        @Override
        TxnId.FastPath notReady(long requestedEpoch, long maxEpoch)
        {
            return Unoptimised;
        }

        @Override
        public TxnId.FastPath allocate(int count)
        {
            return null;
        }

        private static TxnId.FastPath merge(TxnId.FastPath a, TxnId.FastPath b)
        {
            if (a == null) return b;
            // we cannot safely support fast decisions across multiple local shards until we implement the full multi shard coordinator optimisation
            return Unoptimised;
        }

        @Override
        public Boolean apply(Shard shard, Boolean prev, int index)
        {
            return prev && shard.isInFastPath(self);
        }
    }

    static class SupportsPrivilegedFastPath extends TopologyCollector<Boolean, Routables<?>, Boolean, RuntimeException> implements IndexedBiFunction<Shard, Boolean, Boolean>
    {
        final Node.Id self;

        SupportsPrivilegedFastPath(Node.Id self)
        {
            this.self = self;
        }

        @Override
        public Boolean update(Boolean collector, ActiveEpoch e, Routables<?> select)
        {
            return collector && one(e, select);
        }

        @Override
        public Boolean updateIfExists(Boolean collector, ActiveEpoch e, Routables<?> select)
        {
            return update(collector, e, select);
        }

        @Override
        public Boolean one(ActiveEpoch e, Routables<?> routables)
        {
            return e.local.ranges.containsAll(routables) && e.local.foldl(routables, this, true);
        }

        @Override
        public Boolean multi(Boolean result)
        {
            return result;
        }

        @Override
        Boolean retired(long requestedEpoch, long minEpoch) throws RuntimeException
        {
            return false;
        }

        @Override
        Boolean notReady(long requestedEpoch, long maxEpoch) throws RuntimeException
        {
            return false;
        }

        @Override
        public Boolean allocate(int count)
        {
            return true;
        }

        @Override
        public Boolean apply(Shard shard, Boolean prev, int index)
        {
            return prev && shard.isInFastPath(self);
        }
    }

    static class HasChangedReplication extends TopologyCollector<HasChangedReplication.ReplicationChangeTracker, Unseekables<?>, Boolean, RuntimeException>
    {
        static class ReplicationChangeTracker
        {
            int rf = -1;
            boolean hasChanged;
        }

        static final HasChangedReplication INSTANCE = new HasChangedReplication();

        @Override
        public ReplicationChangeTracker allocate(int size)
        {
            return new ReplicationChangeTracker();
        }

        @Override
        public Boolean none()
        {
            return false;
        }

        @Override
        public Boolean multi(ReplicationChangeTracker collector)
        {
            return collector.hasChanged;
        }

        @Override
        Boolean retired(long requestedEpoch, long minEpoch)
        {
            return true;
        }

        @Override
        Boolean notReady(long requestedEpoch, long maxEpoch)
        {
            return true;
        }

        @Override
        public Boolean one(ActiveEpoch e, Unseekables<?> select)
        {
            return false;
        }

        @Override
        public ReplicationChangeTracker update(ReplicationChangeTracker collector, ActiveEpoch e, Unseekables<?> select)
        {
            e.all.foldl(select, (shard, c, i1) -> {
                if (c.rf < 0) c.rf = shard.rf;
                else c.hasChanged |= c.rf != shard.rf;
                return c;
            }, collector);
            return collector;
        }

        @Override
        public ReplicationChangeTracker updateIfExists(ReplicationChangeTracker collector, ActiveEpoch e, Unseekables<?> select)
        {
            return update(collector, e, select);
        }
    }
}
