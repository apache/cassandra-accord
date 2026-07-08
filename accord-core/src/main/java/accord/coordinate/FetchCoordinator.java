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

package accord.coordinate;

import java.util.ArrayList;
import java.util.List;

import accord.api.DataStore.StartingRangeFetch;
import accord.api.DataStore.FetchRanges;
import accord.local.Node;
import accord.api.ExclusiveAsyncExecutor;
import accord.primitives.SyncPoint;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.topology.Topology;
import accord.topology.TopologyException;
import accord.utils.Invariants;
import accord.utils.SortedList;
import accord.utils.SortedListMap;

import static accord.primitives.Routables.Slice.Minimal;
import static accord.topology.SelectShards.ALL;
import static accord.utils.Invariants.illegalState;

/**
 * This abstract coordinator is to be extended by implementations.
 *
 * This coordinates both the fetching of data, and the point-in-time that data will be safe to read,
 * i.e. some point-in-time known to occur after any entry in the data that was fetched.
 */
public abstract class FetchCoordinator extends AbstractSimpleCoordination<Route<?>>
{
    /**
     * For each node, maintain what ranges have been requested, successfully retrieved or not, and whether we are
     * waiting on this replica's response or have nominated another replica to respond for a given range due to
     * lack of response.
     */
    protected static class State
    {
        final Node.Id id;

        final Ranges ownedRanges;
        Ranges uncontacted;
        Ranges contacted = Ranges.EMPTY;
        Ranges inflight = Ranges.EMPTY;
        Ranges waitingOn = Ranges.EMPTY;
        Ranges successful = Ranges.EMPTY;
        Ranges unsuccessful = Ranges.EMPTY;
        boolean failed;

        State(Node.Id id, Ranges ownedRanges, Ranges ranges)
        {
            this.id = id;
            this.ownedRanges = ownedRanges;
            this.uncontacted = ownedRanges.slice(ranges, Minimal);
        }

        public Ranges uncontacted()
        {
            return uncontacted;
        }

        public Ranges inflight()
        {
            return inflight;
        }

        void contact(Ranges ranges)
        {
            Invariants.requireArgument(uncontacted.containsAll(ranges));
            contacted = contacted.with(ranges);
            inflight = inflight.with(ranges);
            waitingOn = waitingOn.with(ranges);
            uncontacted = uncontacted.without(ranges);
        }

        void slow(Ranges ranges)
        {
            Invariants.requireArgument(waitingOn.containsAll(ranges));
            waitingOn = waitingOn.without(ranges);
        }

        void successful(Ranges ranges)
        {
            Invariants.requireArgument(inflight.containsAll(ranges));
            successful = successful.with(ranges);
            inflight = inflight.without(ranges);
            waitingOn = waitingOn.without(ranges);
        }

        Ranges waitingOn(Ranges ranges)
        {
            return ranges.slice(waitingOn, Minimal);
        }

        void unsuccessful(Ranges ranges)
        {
            Invariants.requireArgument(inflight.containsAll(ranges));
            unsuccessful = unsuccessful.with(ranges);
            inflight = inflight.without(ranges);
            waitingOn = waitingOn.without(ranges);
        }

        void fail()
        {
            if (failed)
                return;

            failed = true;
            waitingOn = Ranges.EMPTY;
            inflight = Ranges.EMPTY;
            contacted = contacted.with(uncontacted);
            unsuccessful = unsuccessful.with(uncontacted);
            uncontacted = Ranges.EMPTY;
        }
    }

    protected final Ranges ranges;
    protected final SyncPoint syncPoint;
    // provided to us, and manages the safe-to-read state
    private final FetchRanges fetchRanges;
    private Throwable failures = null;

    final SortedListMap<Node.Id, State> stateMap;
    final List<State> states = new ArrayList<>();

    private Ranges remaining;
    private Ranges success = Ranges.EMPTY;
    private Ranges needed;
    private int inflight;

    protected FetchCoordinator(Node node, ExclusiveAsyncExecutor executor, Ranges ranges, SyncPoint syncPoint, FetchRanges fetchRanges) throws TopologyException
    {
        super(node, executor, syncPoint.syncId, syncPoint.route);
        this.ranges = remaining = ranges;
        this.syncPoint = syncPoint;
        this.fetchRanges = fetchRanges;
        // TODO (expected): prioritise nodes that were members in the "prior" epoch also
        //  (by prior, we mean the prior epoch affecting ownership of this shard, not the prior numerical epoch)
        Topology topology = node.topology().active().forEpoch(ranges, syncPoint.syncId.epoch(), ALL).get(0);
        this.stateMap = new SortedListMap<>(topology.nodes(), State[]::new);
        for (int i = 0 ; i < stateMap.domainSize() ; ++i)
        {
            Node.Id id = stateMap.getKey(i);
            if (id.equals(node.id()))
                continue;

            stateMap.putAtIndex(i, new State(id, topology.rangesForNode(id), ranges));
        }
        states.addAll(stateMap.values());
        needed = ranges;
    }

    protected Ranges ownedRangesForNode(Node.Id node)
    {
        return stateMap.get(node).ownedRanges;
    }

    /**
     * Initiate fetches for the provided ranges to the specified replica.
     * NOTE: this may not be the full set of ranges owned by the replica, and more than one invocation of contact
     * may be made for each replica
     */
    protected abstract void contact(Node.Id to, Ranges ranges);

    /**
     * Try to cancel in-flight fetches for the provided ranges to the specified replica
     */
    protected void uncontact(Node.Id to, Ranges ranges) {}

    /**
     * Invoked on completion. Note that BOTH success and failure may be set, as we may
     * have succeeded on some ranges, but not all.
     *
     * NOTE: This is only the data success, which may occur before all initiated FetchRanges
     * have reported back - either successfully or not.
     */
    protected abstract void onDone(Ranges success, Throwable failure);

    @Override
    protected void start()
    {
        super.start();
        trySendMore();
    }

    private void trySendMore()
    {
        needed = trySendMore(states, needed);
        if (needed.isEmpty())
        {
            if (success.containsAll(remaining))
            {
                setDone();
                onDone(success, failures);
            }
            return;
        }

        exhausted(needed);
        if (inflight > 0)
            return;

        // some portion of the range is completely unavailable
        setDone();
        failures = FailureAccumulator.fail(node.agent(), failures, syncPoint.syncId, null, needed);
        onDone(success, failures);
    }

    protected void exhausted(Ranges exhausted)
    {
        fetchRanges.fail(exhausted, Exhausted.exhausted(node.agent(), syncPoint.syncId, null, exhausted));
    }

    protected void abort(Ranges abort)
    {
        needed = needed.without(abort);
        remaining = remaining.without(abort);
    }

    // this method can be completely overridden by an implementing class, which may simply call contact()
    // it must only ensure needed.isEmpty() (if possible)
    protected Ranges trySendMore(List<State> states, Ranges needed)
    {
        // TODO (required) : need to correctly handle the cluster having fewer nodes than replication factor
        for (State state : states)
        {
            Ranges contact = state.uncontacted.slice(needed, Minimal);
            if (contact.isEmpty())
                continue;
            if (state.inflight.isEmpty())
                ++inflight;
            state.contact(contact);
            contact(state.id, contact);
            needed = needed.without(contact);
            if (needed.isEmpty())
                break;
        }

        return needed;
    }

    /**
     * Implementations MUST call this AFTER the source node has initiated the fetch process (from some snapshot
     * that will not have future operations leak into it), and BEFORE any of this data is written to the local
     * machine.
     */
    protected StartingRangeFetch starting(Node.Id to, Ranges ranges)
    {
        if (isDone())
            throw illegalState();

        // TODO (expected): should we cancel those we have superseded?
        return fetchRanges.starting(ranges);
    }

    protected void success(Node.Id to, Ranges ranges)
    {
        if (isDone())
            return;

        State state = stateMap.get(to);
        if (state.failed)
            return;

        state.successful(ranges);
        if (state.inflight.isEmpty())
            --inflight;

        fetchRanges.fetched(ranges);
        success = success.with(ranges);
        if (success.containsAll(this.ranges))
        {
            setDone();
            onDone(success, null);
        }
    }

    protected void slow(Node.Id to, Ranges ranges)
    {
        if (isDone())
            return;

        State state = stateMap.get(to);
        if (state.failed)
            return;

        state.slow(ranges);
        retry(ranges);
    }

    protected void unavailable(Node.Id to, Ranges ranges)
    {
        if (isDone())
            return;

        State state = stateMap.get(to);
        if (state.failed)
            return;

        Ranges waitingOn = state.waitingOn(ranges);
        state.unsuccessful(ranges);
        if (state.inflight.isEmpty())
            --inflight;

        retry(waitingOn);
    }

    private void retry(Ranges ranges)
    {
        ranges = ranges.slice(remaining, Minimal);
        needed = needed.with(ranges);
        trySendMore();
    }

    protected void fail(Node.Id to, Ranges ranges, Throwable failure)
    {
        if (isDone())
            return;

        failures = FailureAccumulator.append(failures, failure);
        unavailable(to, ranges);
    }

    protected void fail(Node.Id to, Throwable failure)
    {
        if (isDone())
            return;

        failures = FailureAccumulator.append(failures, failure);

        State state = stateMap.get(to);

        if (!state.inflight.isEmpty())
            --inflight;
        Invariants.require(!state.waitingOn.intersects(success));
        needed = needed.with(state.waitingOn.slice(remaining, Minimal));
        // TODO (desired): we don't need to fail all requests to this node, just the one we have a failure response for
        state.fail();
        trySendMore();
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.Bootstrap;
    }

    @Override
    public SortedList<Node.Id> nodes()
    {
        return stateMap.keySet();
    }
    @Override
    public SortedListMap<Node.Id, ?> replies()
    {
        return stateMap;
    }
}
