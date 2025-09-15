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

import java.util.function.BiFunction;
import javax.annotation.Nullable;

import accord.local.Node.Id;
import accord.local.SafeCommandStore;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.topology.Topology.NodeInfo;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static accord.topology.Shard.Flag.MUST_WITNESS;
import static accord.utils.Invariants.illegalArgument;

public abstract class ParticipantsRequest<P extends Participants<?>, R extends Reply> extends NoWaitRequest<P, R>
{
    public final long waitForEpoch;

    public ParticipantsRequest(TxnId txnId, P scope, long waitForEpoch)
    {
        super(txnId, scope);
        Invariants.require(!scope.isEmpty());
        this.waitForEpoch = waitForEpoch;
    }

    /**
     * The portion of the complete Route that this TxnRequest applies to. Should represent the complete
     * range owned by the target node for the involved epochs.
     */
    public P scope()
    {
        return scope;
    }

    /**
     * The minimum epoch the recipient needs to know in order to process the request. This is computed by the sender
     * to permit a recipient to process a request before knowing of a topology change if the sender determines it is
     * safe to do so.
     */
    @Override
    public long waitForEpoch()
    {
        return waitForEpoch;
    }

    protected abstract @Nullable Cancellable submit();

    protected abstract R applyInternal(SafeCommandStore safeStore);

    // finds the first topology index that intersects with the node
    protected static int latestRelevantEpochIndex(Id node, Topologies topologies, Unseekables<?> route)
    {
        if (topologies.size() == 1)
            return 0;

        Ranges latest = topologies.current().rangesForNode(node);
        if (route.intersects(latest))
            return 0;

        int i = 0;
        int mi = topologies.size();

        // find first non-null for node
        while (latest.isEmpty())
        {
            if (++i == mi)
                return mi;

            latest = topologies.get(i).rangesForNode(node);
        }

        if (route.intersects(latest))
            return i;

        // find first non-empty intersection for node
        while (++i < mi)
        {
            Ranges next = topologies.get(i).rangesForNode(node);
            if (!next.equals(latest))
            {
                if (route.intersects(next))
                    return i;
                latest = next;
            }
        }
        return mi;
    }

    /**
     * Compute the minimum epoch the recipient must know in order to safely process the request.
     *
     * For now use a simple heuristic of whether the node's ownership ranges have changed,
     * on the assumption that this might also mean some local shard rearrangement
     * (ignoring the case where the latest epochs do not intersect the keys at all)
     */
    public static long computeWaitForEpoch(Id node, Topologies topologies, Unseekables<?> scope)
    {
        return computeWaitForEpoch(node, topologies, latestRelevantEpochIndex(node, topologies, scope));
    }

    public static long computeWaitForEpoch(Id node, Topologies topologies, int startIndex)
    {
        int i = Math.max(1, startIndex);
        int mi = topologies.size();
        if (i == mi)
            return topologies.oldestEpoch();


        Ranges latest;
        {
            Topology mostRecent = topologies.get(i - 1);
            NodeInfo nodeInfo = mostRecent.nodeInfo(node);
            if (nodeInfo == null)
                return mostRecent.epoch();
            latest = nodeInfo.ranges;
            if (nodeInfo.anyMatch(mostRecent, shard -> shard.is(MUST_WITNESS)))
                return mostRecent.epoch();
        }
        while (i < mi)
        {
            Topology topology = topologies.get(i);
            NodeInfo nodeInfo = topology.nodeInfo(node);
            if (nodeInfo == null)
                break;

            Ranges ranges = nodeInfo.ranges;
            if (!ranges.equals(latest))
                break;

            ++i;
            if (nodeInfo.anyMatch(topology, shard -> shard.is(MUST_WITNESS)))
                break;
        }
        return topologies.get(i - 1).epoch();
    }

    public static Route<?> computeScope(Id node, Topologies topologies, FullRoute<?> fullRoute)
    {
        return computeScope(node, topologies, fullRoute, latestRelevantEpochIndex(node, topologies, fullRoute));
    }

    public static Participants<?> computeScope(Id node, Topologies topologies, Participants<?> participants)
    {
        return computeScope(node, topologies, participants, latestRelevantEpochIndex(node, topologies, participants));
    }

    public static Route<?> computeScope(Id node, Topologies topologies, Route<?> route)
    {
        return computeScope(node, topologies, route, latestRelevantEpochIndex(node, topologies, route));
    }

    public static Route<?> computeScope(Id node, Topologies topologies, Route<?> route, int startIndex)
    {
        return computeScope(node, topologies, route, startIndex, Route::slice, Route::with);
    }

    public static Participants<?> computeScope(Id node, Topologies topologies, Participants<?> route, int startIndex)
    {
        return computeScope(node, topologies, route, startIndex, Participants::slice, Participants::with);
    }

    // TODO (low priority, clarity): move to Topologies
    public static <I, O> O computeScope(Id node, Topologies topologies, I keys, int startIndex, BiFunction<I, Ranges, O> slice, BiFunction<O, O, O> merge)
    {
        Ranges last = null;
        O scope = null;
        for (int i = startIndex, mi = topologies.size() ; i < mi ; ++i)
        {
            Topology topology = topologies.get(i).global();
            Ranges ranges = topology.rangesForNode(node);
            if (ranges != last && !ranges.equals(last))
            {
                O add = slice.apply(keys, ranges);
                scope = scope == null ? add : merge.apply(scope, add);
            }

            last = ranges;
        }
        if (scope == null)
            throw illegalArgument("No intersection between " + topologies + " and " + keys + " on " + node);
        return scope;
    }
}
