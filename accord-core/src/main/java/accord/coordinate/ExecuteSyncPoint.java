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

import java.util.function.Function;

import accord.api.Result;
import accord.coordinate.tracking.AbstractSimpleTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.messages.ApplyThenWaitUntilApplied;
import accord.messages.Callback;
import accord.messages.ReadData;
import accord.messages.ReadData.ReadReply;
import accord.messages.WaitUntilApplied;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.topology.TopologyManager.EpochSufficiencyMode.AT_MOST;

public abstract class ExecuteSyncPoint<S extends Seekables<?, ?>> extends SettableResult<SyncPoint<S>> implements Callback<ReadReply>
{
    public static class SyncPointErased extends Throwable {}

    public static class ExecuteBlocking<S extends Seekables<?, ?>> extends ExecuteSyncPoint<S>
    {
        private final Timestamp executeAt;
        public ExecuteBlocking(Node node, SyncPoint<S> syncPoint, AbstractSimpleTracker<?> tracker, Timestamp executeAt)
        {
            super(node, syncPoint, tracker);
            Invariants.checkArgument(!syncPoint.syncId.kind().awaitsOnlyDeps());
            this.executeAt = executeAt;
        }

        public static <S extends Seekables<?, ?>> ExecuteBlocking<S> atQuorum(Node node, Topologies topologies, SyncPoint<S> syncPoint, Timestamp executeAt)
        {
            return new ExecuteBlocking<>(node, syncPoint, new QuorumTracker(topologies), executeAt);
        }

        @Override
        public void start()
        {
            Txn txn = node.agent().emptyTxn(syncPoint.syncId.kind(), syncPoint.keysOrRanges);
            Writes writes = txn.execute(syncPoint.syncId, syncPoint.syncId, null);
            Result result = txn.result(syncPoint.syncId, syncPoint.syncId, null);
            node.send(tracker.topologies().nodes(), to -> {
                Seekables<?, ?> notify = to.equals(node.id()) ? null : syncPoint.keysOrRanges;
                Participants<?> participants = syncPoint.keysOrRanges.toParticipants();
                return new ApplyThenWaitUntilApplied(to, tracker.topologies(), executeAt, syncPoint.route(), syncPoint.syncId, txn, syncPoint.waitFor, participants, writes, result, notify);
            }, this);
        }
    }

    public static class ExecuteExclusiveSyncPoint extends ExecuteSyncPoint<Ranges>
    {
        private long retryInFutureEpoch;
        public ExecuteExclusiveSyncPoint(Node node, SyncPoint<Ranges> syncPoint, Function<Topologies, AbstractSimpleTracker<?>> trackerSupplier)
        {
            super(node, syncPoint, trackerSupplier);
            Invariants.checkArgument(syncPoint.syncId.kind() == ExclusiveSyncPoint);
        }

        public ExecuteExclusiveSyncPoint(Node node, SyncPoint<Ranges> syncPoint, Function<Topologies, AbstractSimpleTracker<?>> trackerSupplier, AbstractSimpleTracker<?> tracker)
        {
            super(node, syncPoint, trackerSupplier, tracker);
            Invariants.checkArgument(syncPoint.syncId.kind() == ExclusiveSyncPoint);
        }

        @Override
        protected void start()
        {
            node.send(tracker.nodes(), to -> new WaitUntilApplied(to, tracker.topologies(), syncPoint.syncId, syncPoint.keysOrRanges, syncPoint.syncId.epoch()), this);
        }

        @Override
        public synchronized void onSuccess(Node.Id from, ReadReply reply)
        {
            if (reply instanceof ReadData.ReadOkWithFutureEpoch)
                retryInFutureEpoch = Math.max(retryInFutureEpoch, ((ReadData.ReadOkWithFutureEpoch) reply).futureEpoch);

            super.onSuccess(from, reply);
        }

        @Override
        protected void onSuccess()
        {
            if (retryInFutureEpoch > tracker.topologies().currentEpoch())
            {
                ExecuteExclusiveSyncPoint continuation = new ExecuteExclusiveSyncPoint(node, syncPoint, trackerSupplier, trackerSupplier.apply(node.topology().preciseEpochs(syncPoint.route(), tracker.topologies().currentEpoch(), retryInFutureEpoch)));
                continuation.addCallback((success, failure) -> {
                    if (failure == null) trySuccess(success);
                    else tryFailure(failure);
                });
                continuation.start();
            }
            else
            {
                super.onSuccess();
            }
        }
    }

    final Node node;
    final SyncPoint<S> syncPoint;

    final Function<Topologies, AbstractSimpleTracker<?>> trackerSupplier;
    final AbstractSimpleTracker<?> tracker;
    private Throwable failures = null;

    ExecuteSyncPoint(Node node, SyncPoint<S> syncPoint, AbstractSimpleTracker<?> tracker)
    {
        this.node = node;
        this.syncPoint = syncPoint;
        this.trackerSupplier = null;
        this.tracker = tracker;
    }

    ExecuteSyncPoint(Node node, SyncPoint<S> syncPoint, Function<Topologies, AbstractSimpleTracker<?>> trackerSupplier)
    {
        this(node, syncPoint, trackerSupplier, trackerSupplier.apply(node.topology().withUncompletedEpochs(syncPoint.route(), EpochSupplier.constant(syncPoint.earliestEpoch()), syncPoint.syncId, AT_MOST)));
    }

    ExecuteSyncPoint(Node node, SyncPoint<S> syncPoint, Function<Topologies, AbstractSimpleTracker<?>> trackerSupplier, AbstractSimpleTracker<?> tracker)
    {
        this.node = node;
        this.syncPoint = syncPoint;
        this.trackerSupplier = trackerSupplier;
        this.tracker = tracker;
    }

    protected abstract void start();

    @Override
    public synchronized void onSuccess(Node.Id from, ReadReply reply)
    {
        if (isDone()) return;

        if (!reply.isOk())
        {
            switch ((ReadData.CommitOrReadNack)reply)
            {
                default: throw new AssertionError("Unhandled: " + reply);

                case Insufficient:
                    CoordinateSyncPoint.sendApply(node, from, syncPoint);
                    return;

                case Redundant:
                    tryFailure(new SyncPointErased());
                    return;

                case Invalid:
                    tryFailure(new Invalidated(syncPoint.syncId, syncPoint.homeKey));
                    return;
            }
        }
        else
        {
            // TODO (required, consider): do we need to handle ranges not being safe to read
            if (tracker.recordSuccess(from) == RequestStatus.Success)
                onSuccess();
        }
    }

    protected void onSuccess()
    {
        trySuccess(syncPoint);
    }

    @Override
    public synchronized void onFailure(Node.Id from, Throwable failure)
    {
        if (isDone()) return;
        failures = FailureAccumulator.append(failures, failure);
        if (tracker.recordFailure(from) == RequestStatus.Failed)
            tryFailure(FailureAccumulator.createFailure(failures, syncPoint.syncId, syncPoint.homeKey));
    }

    @Override
    public void onCallbackFailure(Node.Id from, Throwable failure)
    {
        tryFailure(failure);
    }
}
