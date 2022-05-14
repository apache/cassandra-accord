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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import accord.coordinate.tracking.ReadTracker.ReadShardTracker;
import accord.local.Node.Id;
import accord.topology.Shard;
import accord.topology.Topologies;

import static com.google.common.collect.Sets.newHashSetWithExpectedSize;

public class ReadTracker<T extends ReadShardTracker> extends AbstractResponseTracker<T>
{
    public static class ReadShardTracker extends ShardTracker
    {
        private boolean hasData = false;
        protected int inflight;
        private int contacted;
        private int slow;

        public ReadShardTracker(Shard shard)
        {
            super(shard);
        }

        public void recordInflightRead(Id node)
        {
            ++contacted;
            ++inflight;
        }

        // TODO: this is clunky, restructure the tracker to handle this more cleanly
        // record a node as contacted, even though it isn't
        public void recordContacted(Id node)
        {
            ++contacted;
        }

        public void recordSlowRead(Id node)
        {
            ++slow;
        }

        public void unrecordSlowRead(Id node)
        {
            --slow;
        }

        public boolean recordReadSuccess(Id node)
        {
            Preconditions.checkArgument(shard.nodes.contains(node));
            Preconditions.checkState(inflight > 0);
            --inflight;
            hasData = true;
            return true;
        }

        public boolean shouldRead()
        {
            return !hasData && inflight == slow;
        }

        public boolean recordReadFailure(Id node)
        {
            Preconditions.checkState(inflight > 0);
            --inflight;
            return true;
        }

        public boolean hasCompletedRead()
        {
            return hasData;
        }

        public boolean hasFailed()
        {
            return !hasData && inflight == 0 && contacted == shard.nodes.size();
        }
    }

    // TODO: abstract the candidate selection process so the implementation may prioritise based on distance/health etc
    private final List<Id> candidates;
    private final Set<Id> inflight;
    private Set<Id> slow;

    public static ReadTracker<ReadShardTracker> create(Topologies topologies)
    {
        return new ReadTracker<>(topologies, ReadShardTracker[]::new, ReadShardTracker::new);
    }

    public ReadTracker(Topologies topologies, IntFunction<T[]> arrayFactory, Function<Shard, T> trackerFactory)
    {
        super(topologies, arrayFactory, trackerFactory);
        candidates = new ArrayList<>(topologies.nodes());
        inflight = newHashSetWithExpectedSize(trackerCount());
    }

    @VisibleForTesting
    void recordInflightRead(Id node)
    {
        if (!inflight.add(node))
            throw new IllegalStateException(node + " already in flight");

        forEachTrackerForNode(node, ReadShardTracker::recordInflightRead);
    }

    @VisibleForTesting
    private void recordContacted(Id node)
    {
        forEachTrackerForNode(node, ReadShardTracker::recordContacted);
    }

    public void recordSlowRead(Id node)
    {
        if (slow == null)
            slow = newHashSetWithExpectedSize(trackerCount());

        if (slow.add(node))
        {
            forEachTrackerForNode(node, ReadShardTracker::recordSlowRead);
        }
    }

    protected void recordResponse(Id node)
    {
        if (!inflight.remove(node))
            throw new IllegalStateException("Nothing in flight for " + node);

        if (slow != null && slow.remove(node))
            forEachTrackerForNode(node, ReadShardTracker::unrecordSlowRead);
    }

    public boolean recordReadSuccess(Id node)
    {
        recordResponse(node);
        return anyForNode(node, ReadShardTracker::recordReadSuccess);
    }

    public boolean recordReadFailure(Id node)
    {
        recordResponse(node);
        return anyForNode(node, ReadShardTracker::recordReadFailure);
    }

    public boolean hasCompletedRead()
    {
        return all(ReadShardTracker::hasCompletedRead);
    }

    public boolean hasInFlight()
    {
        return !inflight.isEmpty();
    }

    public boolean hasFailed()
    {
        return any(ReadShardTracker::hasFailed);
    }

    private int intersectionSize(Id node, Set<ReadShardTracker> target)
    {
        return matchingTrackersForNode(node, target::contains);
    }

    private int compareIntersections(Id left, Id right, Set<ReadShardTracker> target)
    {
        return Integer.compare(intersectionSize(left, target), intersectionSize(right, target));
    }

    /**
     * Return the smallest set of nodes needed to satisfy required reads.
     *
     * Returns null if the read cannot be completed.
     *
     * TODO: prioritisation of nodes should be implementation-defined
     */
    public Set<Id> computeMinimalReadSetAndMarkInflight()
    {
        Set<ReadShardTracker> toRead = foldl((tracker, accumulate) -> {
            if (!tracker.shouldRead())
                return accumulate;

            if (accumulate == null)
                accumulate = new LinkedHashSet<>(); // determinism

            accumulate.add(tracker);
            return accumulate;
        }, null);

        if (toRead == null)
            return Collections.emptySet();

        assert !toRead.isEmpty();
        Set<Id> nodes = new HashSet<>();
        while (!toRead.isEmpty())
        {
            if (candidates.isEmpty())
            {
                if (!nodes.isEmpty())
                    nodes.forEach(this::recordContacted);
                return null;
            }

            // TODO: Topology needs concept of locality/distance
            candidates.sort((a, b) -> compareIntersections(a, b, toRead));

            int i = candidates.size() - 1;
            Id node = candidates.get(i);
            nodes.add(node);
            candidates.remove(i);
            forEachTrackerForNode(node, (tracker, ignore) -> toRead.remove(tracker));
        }

        // must recordInFlightRead after loop, as we might return null if the reads are insufficient to make progress
        // but in this case we need the tracker to
        nodes.forEach(this::recordInflightRead);

        return nodes;
    }

}
