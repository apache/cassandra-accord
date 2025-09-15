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

import javax.annotation.Nullable;

import accord.api.VisibleForImplementation;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.SequentialAsyncExecutor;
import accord.messages.GetMaxConflict;
import accord.messages.GetMaxConflict.GetMaxConflictOk;
import accord.primitives.FullRoute;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;

/**
 * Calculate the maximum TxnId that could have been agreed before this operation started
 */
@VisibleForImplementation
public class CoordinateMaxConflict extends AbstractCoordinatePreAccept<Timestamp, GetMaxConflictOk, Void>
{
    final QuorumTracker tracker;
    Timestamp maxConflict;
    long executionEpoch;

    private CoordinateMaxConflict(Node node, SequentialAsyncExecutor executor, FullRoute<?> route, long executionEpoch, BiConsumer<? super Timestamp, Throwable> callback)
    {
        this(node, executor, route, executionEpoch, node.topology().withUnsyncedEpochs(route, executionEpoch, executionEpoch), callback);
    }

    private CoordinateMaxConflict(Node node, SequentialAsyncExecutor executor, FullRoute<?> route, long executionEpoch, Topologies topologies, BiConsumer<? super Timestamp, Throwable> callback)
    {
        super(node, executor, route, TxnId.NONE, topologies, callback);
        this.maxConflict = Timestamp.NONE;
        this.executionEpoch = executionEpoch;
        this.tracker = new QuorumTracker(topologies);
    }

    public static AsyncChain<Timestamp> maxConflict(Node node, Routables<?> keysOrRanges)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super Timestamp, Throwable> callback)
            {
                maxConflict(node, keysOrRanges, callback);
                return null;
            }
        };
    }

    public static void maxConflict(Node node, Routables<?> keysOrRanges, BiConsumer<? super Timestamp, Throwable> callback)
    {
        long epoch = node.epoch();
        FullRoute<?> route = node.computeRoute(epoch, keysOrRanges);
        TopologyMismatch mismatch = TopologyMismatch.checkForMismatchOrPendingRemoval(node.topology().globalForEpoch(epoch), null, route.homeKey(), keysOrRanges);
        if (mismatch != null)
        {
            callback.accept(null, mismatch);
            return;
        }

        CoordinateMaxConflict coordinate;
        try
        {
            coordinate = new CoordinateMaxConflict(node, node.someSequentialExecutor(), route, epoch, callback);
        }
        catch (Throwable t)
        {
            callback.accept(null, t);
            return;
        }
        coordinate.start();
    }

    @Override
    void start()
    {
        super.start();
        contact(to -> new GetMaxConflict(to, topologies, scope, executionEpoch));
    }

    @Override
    void onSuccessInternal(Node.Id from, int fromIndex, GetMaxConflictOk reply)
    {
        maxConflict = Timestamp.max(reply.maxConflict, maxConflict);
        executionEpoch = Math.max(executionEpoch, reply.latestEpoch);

        if (tracker.recordSuccess(from) == Success)
            onPreAcceptedOrNewEpoch();
    }

    @Override
    void onFailureInternal(Node.Id from, int fromIndex, Throwable failure)
    {
        recordFailure(failure);
        if (tracker.recordFailure(from) == Failed)
            finishOnFailure();
    }

    @Override
    void onNewEpochTopologyMismatch(TopologyMismatch mismatch)
    {
        finishWithFailureOverride(mismatch);
    }

    @Override
    long executeAtEpoch()
    {
        return executionEpoch;
    }

    @Override
    void onPreAccepted(Topologies topologies)
    {
        finishAndInvokeCallback(maxConflict, null);
    }

    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }
}
