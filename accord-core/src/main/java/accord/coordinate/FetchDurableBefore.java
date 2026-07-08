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

import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.messages.GetDurableBefore;
import accord.messages.GetDurableBefore.DurableBeforeReply;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.SortedListMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

public class FetchDurableBefore extends AbstractCoordination<Ranges, DurableBefore, DurableBeforeReply, DurableBefore>
{
    final QuorumTracker tracker;

    public FetchDurableBefore(Node node, Topology topology, BiConsumer<? super DurableBefore, Throwable> callback)
    {
        super(node, node.someExclusiveExecutor(), TxnId.NONE, topology.ranges(), topology.nodes(), callback);
        this.tracker = new QuorumTracker(new Topologies.Single(node.topology().sorter(), topology));
    }

    void start()
    {
        super.start();
        contact(ignore -> new GetDurableBefore(), id -> !node.id().equals(id));
        executor.executeMaybeImmediately(() -> {
            markSelfContacted();
            onSuccess(node.id(), new DurableBeforeReply(node.durableBefore()));
        });
    }

    public static AsyncChain<DurableBefore> catchup(Node node)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            public @Nullable Cancellable start(BiConsumer<? super DurableBefore, Throwable> callback)
            {
                catchup(node, callback);
                return null;
            }
        };
    }

    public static void catchup(Node node, BiConsumer<? super DurableBefore, Throwable> callback)
    {
        Topology topology = node.topology().currentLocal();
        if (topology.ranges().isEmpty())
            callback.accept(DurableBefore.EMPTY, null);
        else
            new FetchDurableBefore(node, topology, callback).start();
    }

    @Override
    void onSuccessInternal(Node.Id from, int fromIndex, DurableBeforeReply reply)
    {
        recordOk(fromIndex, reply.durableBefore);
        handle(tracker.recordSuccess(from));
    }

    @Override
    void onFailureInternal(Node.Id from, int fromIndex, Throwable fail)
    {
        recordFailure(fail);
        handle(tracker.recordFailure(from));
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.FetchDurableBefore;
    }

    @Nonnull
    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }

    private void handle(RequestStatus status)
    {
        switch (status)
        {
            case Success:
                SortedListMap<Node.Id, DurableBefore> oks = finishOks();
                DurableBefore durableBefore = oks.foldlNonNullValues(DurableBefore::merge, DurableBefore.EMPTY);
                finishWithSuccess(durableBefore);
                break;
            case Failed:
                finishOnFailure();
                break;
            case NoChange:
                break;
        }
    }
}
