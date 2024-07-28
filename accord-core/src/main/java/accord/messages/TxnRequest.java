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

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.FullRoute;
import accord.primitives.PartialRoute;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import static java.lang.Long.min;

public abstract class TxnRequest<R> implements Request, PreLoadContext, MapReduceConsume<SafeCommandStore, R>
{
    public static abstract class WithUnsynced<R> extends TxnRequest<R>
    {
        public final long minUnsyncedEpoch; // TODO (low priority, clarity): can this just always be TxnId.epoch?
        public final boolean doNotComputeProgressKey;

        public WithUnsynced(Id to, Topologies topologies, TxnId txnId, FullRoute<?> route)
        {
            this(to, topologies, txnId, route, latestRelevantEpochIndex(to, topologies, route));
        }

        protected WithUnsynced(Id to, Topologies topologies, TxnId txnId, FullRoute<?> route, int startIndex)
        {
            this(to, topologies, txnId, txnId.epoch(), route, startIndex);
        }

        public WithUnsynced(Id to, Topologies topologies, long epoch, FullRoute<?> route)
        {
            this(to, topologies, TxnId.NONE, epoch, route, latestRelevantEpochIndex(to, topologies, route), true);
        }

        protected WithUnsynced(Id to, Topologies topologies, TxnId txnId, long epoch, FullRoute<?> route, int startIndex)
        {
            this(to, topologies, txnId, epoch, route, startIndex, false);
        }

        protected WithUnsynced(Id to, Topologies topologies, TxnId txnId, long epoch, FullRoute<?> route, int startIndex, boolean doNotComputeProgressKey)
        {
            super(to, topologies, route, txnId, startIndex);
            this.minUnsyncedEpoch = topologies.oldestEpoch();
            this.doNotComputeProgressKey = doNotComputeProgressKey || doNotComputeProgressKey(topologies, startIndex, epoch, waitForEpoch());

            Ranges ranges = topologies.forEpoch(epoch).rangesForNode(to);
            if (doNotComputeProgressKey)
            {
                Invariants.checkState(txnId.equals(TxnId.NONE) || !route.intersects(ranges)); // confirm dest is not a replica on txnId.epoch
            }
            else if (Invariants.isParanoid())
            {
                long progressEpoch = Math.min(waitForEpoch(), epoch);
                Ranges computesRangesOn = topologies.forEpoch(progressEpoch).rangesForNode(to);
                if (computesRangesOn == null)
                    Invariants.checkState(!route.intersects(ranges));
                else
                    Invariants.checkState(route.slice(computesRangesOn).equals(route.slice(ranges)));
            }
        }

        protected WithUnsynced(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minUnsyncedEpoch, boolean doNotComputeProgressKey)
        {
            super(txnId, scope, waitForEpoch);
            this.minUnsyncedEpoch = minUnsyncedEpoch;
            this.doNotComputeProgressKey = doNotComputeProgressKey;
        }

        @Override
        RoutingKey progressKey()
        {
            if (doNotComputeProgressKey)
                return null;
            return super.progressKey();
        }
    }

    public final TxnId txnId;
    public final PartialRoute<?> scope;
    public final long waitForEpoch;
    // set on receive only
    protected transient RoutingKey progressKey;
    protected transient Node node;
    protected transient Id replyTo;
    protected transient ReplyContext replyContext;

    public ReplyContext replyContext()
    {
        return replyContext;
    }

    public TxnRequest(Node.Id to, Topologies topologies, Route<?> route, TxnId txnId)
    {
        this(to, topologies, route, txnId, latestRelevantEpochIndex(to, topologies, route));
    }

    public TxnRequest(Node.Id to, Topologies topologies, Route<?> route, TxnId txnId, int startIndex)
    {
        this(txnId, computeScope(to, topologies, route, startIndex), computeWaitForEpoch(to, topologies, startIndex));
    }

    public TxnRequest(TxnId txnId, PartialRoute<?> scope, long waitForEpoch)
    {
        Invariants.checkState(!scope.isEmpty());
        this.txnId = txnId;
        this.scope = scope;
        this.waitForEpoch = waitForEpoch;
    }

    /**
     * The portion of the complete Route that this TxnRequest applies to. Should represent the complete
     * range owned by the target node for the involved epochs.
     */
    public PartialRoute<?> scope()
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

    @Override
    public void preProcess(Node on, Id replyTo, ReplyContext replyContext)
    {
        this.node = on;
        this.replyTo = replyTo;
        this.replyContext = replyContext;
        this.progressKey = progressKey(); // TODO (low priority, clarity): not every class that extends TxnRequest needs this set
    }

    @Override
    public void process(Node on, Id replyTo, ReplyContext replyContext)
    {
        preProcess(on, replyTo, replyContext);
        process();
    }

    RoutingKey progressKey()
    {
        return progressKey(node, waitForEpoch, txnId, scope);
    }

    public static RoutingKey progressKey(Node node, long waitForEpoch, TxnId txnId, Route<?> scope)
    {
        if (txnId == null)
            return null;

        // if waitForEpoch < txnId.epoch, then this replica's ownership is unchanged
        long progressEpoch = min(waitForEpoch, txnId.epoch());
        return node.trySelectProgressKey(progressEpoch, scope, scope.homeKey());
    }

    protected abstract void process();

    // finds the first topology index that intersects with the node
    protected static int latestRelevantEpochIndex(Node.Id node, Topologies topologies, Routables<?> route)
    {
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
    public static long computeWaitForEpoch(Node.Id node, Topologies topologies, Unseekables<?> scope)
    {
        return computeWaitForEpoch(node, topologies, latestRelevantEpochIndex(node, topologies, scope));
    }

    public static long computeWaitForEpoch(Node.Id node, Topologies topologies, int startIndex)
    {
        int i = Math.max(1, startIndex);
        int mi = topologies.size();
        if (i == mi)
            return topologies.oldestEpoch();

        Ranges latest = topologies.get(i - 1).rangesForNode(node);
        while (i < mi)
        {
            Topology topology = topologies.get(i);
            Ranges ranges = topology.rangesForNode(node);
            if (!ranges.equals(latest))
                break;
            ++i;
        }
        return topologies.get(i - 1).epoch();
    }

    public static PartialRoute<?> computeScope(Node.Id node, Topologies topologies, FullRoute<?> fullRoute)
    {
        return computeScope(node, topologies, fullRoute, latestRelevantEpochIndex(node, topologies, fullRoute));
    }

    public static PartialRoute<?> computeScope(Node.Id node, Topologies topologies, Route<?> route, int startIndex)
    {
        return computeScope(node, topologies, route, startIndex, Route::slice, PartialRoute::union);
    }

    // TODO (low priority, clarity): move to Topologies
    public static <I, O> O computeScope(Node.Id node, Topologies topologies, I keys, int startIndex, BiFunction<I, Ranges, O> slice, BiFunction<O, O, O> merge)
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
            throw new IllegalArgumentException("No intersection");
        return scope;
    }

    private static boolean doNotComputeProgressKey(Topologies topologies, int startIndex, long epoch, long waitForEpoch)
    {
        // to understand this calculation we must bear in mind the following:
        //  - startIndex is the "latest relevant" which means we skip over recent epochs where we are not owners at all,
        //    i.e. if this node does not participate in the most recent epoch, startIndex > 0
        //  - waitForEpoch gives us the most recent epoch with differing ownership information, starting from startIndex
        // So, we can have some surprising situations arise where a *prior* owner must be contacted for its vote,
        // and does not need to wait for the latest ring information because from the point of view of its contribution
        // the stale ring information is sufficient, however we do not want it to compute a progress key with this stale
        // ring information and mistakenly believe that it is a home shard for the transaction, as it will not receive
        // updates for the transaction going forward.
        // So in these cases we send a special flag indicating that the progress key should not be computed
        // (as it might be done so with stale ring information)

        // TODO (low priority, clarity): this would be better defined as "hasProgressKey"
        return waitForEpoch < epoch && startIndex > 0
                && topologies.get(startIndex).epoch() < epoch;
    }
}
