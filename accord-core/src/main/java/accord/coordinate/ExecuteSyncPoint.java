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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import accord.api.Result;
import accord.coordinate.CoordinationAdapter.Adapters;
import accord.coordinate.tracking.QuorumIdTracker;
import accord.coordinate.tracking.SimpleTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.messages.Apply;
import accord.messages.ApplyThenWaitUntilApplied;
import accord.messages.Callback;
import accord.messages.InformDurable;
import accord.messages.ReadData;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadReply;
import accord.messages.WaitUntilApplied;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Status;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Unseekable;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.messages.Apply.ApplyReply.Insufficient;
import static accord.messages.ReadData.CommitOrReadNack.Waiting;
import static accord.primitives.Status.Durability.Majority;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

public abstract class ExecuteSyncPoint<U extends Unseekable> extends SettableResult<SyncPoint<U>> implements Callback<ReadReply>
{
    public static class SyncPointErased extends Throwable {}

    public static class ExecuteInclusive<U extends Unseekable> extends ExecuteSyncPoint<U>
    {
        private final Timestamp executeAt;
        private final QuorumIdTracker durableTracker;
        private Callback<Apply.ApplyReply> insufficientCallback;

        public ExecuteInclusive(Node node, SyncPoint<U> syncPoint, SimpleTracker<?> tracker, Timestamp executeAt)
        {
            super(node, syncPoint, tracker);
            Invariants.checkArgument(!syncPoint.syncId.awaitsOnlyDeps());
            this.executeAt = executeAt;
            this.durableTracker = new QuorumIdTracker(tracker.topologies());
        }

        public static <U extends Unseekable> ExecuteInclusive<U> atQuorum(Node node, Topologies topologies, SyncPoint<U> syncPoint, Timestamp executeAt)
        {
            return new ExecuteInclusive<>(node, syncPoint, new QuorumTracker(topologies), executeAt);
        }

        @Override
        public void onSuccess(Node.Id from, ReadReply reply)
        {
            if (isDurableReply(reply))
                onDurableSuccess(from);

            super.onSuccess(from, reply);
        }

        private void onDurableSuccess(Node.Id from)
        {
            if (durableTracker.recordSuccess(from) == RequestStatus.Success)
                InformDurable.informHome(node, tracker.topologies(), syncPoint.syncId, syncPoint.route, executeAt, Majority);
        }

        private static boolean isDurableReply(ReadReply reply)
        {
            if (reply.isOk())
                return true;

            switch ((CommitOrReadNack) reply)
            {
                case Waiting:
                case Invalid:
                case Redundant:
                    return true;
                case Insufficient:
                case Rejected:
                    return false;
            }
            return false;
        }

        protected void sendApply(Node.Id to)
        {
            if (insufficientCallback == null)
            {
                insufficientCallback = new Callback<>()
                {
                    @Override
                    public void onSuccess(Node.Id from, Apply.ApplyReply reply)
                    {
                        if (reply != Insufficient)
                            onDurableSuccess(from);
                    }
                    @Override public void onFailure(Node.Id from, Throwable failure) {}
                    @Override public void onCallbackFailure(Node.Id from, Throwable failure) {}
                };
            }
            CoordinateSyncPoint.sendApply(node, to, syncPoint, insufficientCallback);
        }

        @Override
        public void start()
        {
            Txn txn = node.agent().emptySystemTxn(syncPoint.syncId.kind(), syncPoint.syncId.domain());
            Writes writes = txn.execute(syncPoint.syncId, syncPoint.syncId, null);
            Result result = txn.result(syncPoint.syncId, syncPoint.syncId, null);
            node.send(tracker.topologies().nodes(), to -> {
                Participants<?> participants = syncPoint.route.participants();
                return new ApplyThenWaitUntilApplied(to, tracker.topologies(), executeAt, syncPoint.route(), syncPoint.syncId, txn, syncPoint.waitFor, participants, writes, result);
            }, this);
        }
    }

    public static class ExecuteExclusive extends ExecuteSyncPoint<Range>
    {
        private long retryInFutureEpoch;
        public ExecuteExclusive(Node node, SyncPoint<Range> syncPoint, Function<Topologies, SimpleTracker<?>> trackerSupplier)
        {
            super(node, syncPoint, Adapters.exclusiveSyncPoint().forExecution(node, syncPoint.route(), syncPoint.syncId, syncPoint.syncId, syncPoint.waitFor), trackerSupplier);
            Invariants.checkArgument(syncPoint.syncId.kind() == ExclusiveSyncPoint);
        }

        public ExecuteExclusive(Node node, SyncPoint<Range> syncPoint, Function<Topologies, SimpleTracker<?>> trackerSupplier, SimpleTracker<?> tracker)
        {
            super(node, syncPoint, trackerSupplier, tracker);
            Invariants.checkArgument(syncPoint.syncId.kind() == ExclusiveSyncPoint);
        }

        @Override
        protected void start()
        {
            SortedArrayList<Node.Id> contact = tracker.filterAndRecordFaulty();
            if (contact == null) tryFailure(new Exhausted(syncPoint.syncId, syncPoint.route.homeKey(), null));
            else node.send(contact, to -> new WaitUntilApplied(to, tracker.topologies(), syncPoint.syncId, syncPoint.route, syncPoint.syncId.epoch()), this);
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
                ExecuteExclusive continuation = new ExecuteExclusive(node, syncPoint, trackerSupplier, trackerSupplier.apply(node.topology().preciseEpochs(syncPoint.route(), tracker.topologies().currentEpoch(), retryInFutureEpoch)));
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
    final SyncPoint<U> syncPoint;

    final Function<Topologies, SimpleTracker<?>> trackerSupplier;
    final SimpleTracker<?> tracker;
    private Throwable failures = null;
    final Map<Node.Id, Object> debug = Invariants.debug() ? new LinkedHashMap<>() : null;

    ExecuteSyncPoint(Node node, SyncPoint<U> syncPoint, SimpleTracker<?> tracker)
    {
        this.node = node;
        this.syncPoint = syncPoint;
        this.trackerSupplier = null;
        this.tracker = tracker;
    }

    ExecuteSyncPoint(Node node, SyncPoint<U> syncPoint, Topologies topologies, Function<Topologies, SimpleTracker<?>> trackerSupplier)
    {
        this(node, syncPoint, trackerSupplier, trackerSupplier.apply(topologies));
    }

    ExecuteSyncPoint(Node node, SyncPoint<U> syncPoint, Function<Topologies, SimpleTracker<?>> trackerSupplier, SimpleTracker<?> tracker)
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
        if (debug != null)
            debug.put(from, reply);

        if (!reply.isOk())
        {
            switch ((CommitOrReadNack)reply)
            {
                default: throw new AssertionError("Unhandled: " + reply);

                case Insufficient:
                    sendApply(from);
                    return;

                case Redundant:
                    tryFailure(new SyncPointErased());
                    return;

                case Invalid:
                    tryFailure(new Invalidated(syncPoint.syncId, syncPoint.route.homeKey()));
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

    protected void sendApply(Node.Id to)
    {
        CoordinateSyncPoint.sendApply(node, to, syncPoint);
    }

    @Override
    public synchronized void onFailure(Node.Id from, Throwable failure)
    {
        if (isDone()) return;
        if (debug != null)
            debug.put(from, failure);

        failures = FailureAccumulator.append(failures, failure);
        if (tracker.recordFailure(from) == RequestStatus.Failed)
            tryFailure(FailureAccumulator.createFailure(failures, syncPoint.syncId, syncPoint.route.homeKey()));
    }

    @Override
    public void onCallbackFailure(Node.Id from, Throwable failure)
    {
        tryFailure(failure);
    }
}
