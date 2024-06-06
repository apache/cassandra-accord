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

import accord.api.Result;
import accord.coordinate.tracking.AbstractSimpleTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.messages.ApplyThenWaitUntilApplied;
import accord.messages.Callback;
import accord.messages.ReadData;
import accord.messages.ReadData.ReadReply;
import accord.primitives.Participants;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.async.AsyncResults.SettableResult;

public abstract class ExecuteSyncPoint<S extends Seekables<?, ?>> extends SettableResult<SyncPoint<S>> implements Callback<ReadReply>
{
    public static class SyncPointErased extends Throwable {}

    public static class ExecuteBlocking<S extends Seekables<?, ?>> extends ExecuteSyncPoint<S>
    {
        private final Timestamp executeAt;
        public ExecuteBlocking(Node node, AbstractSimpleTracker<?> tracker, SyncPoint<S> syncPoint, Timestamp executeAt)
        {
            super(node, tracker, syncPoint);
            this.executeAt = executeAt;
        }

        public static <S extends Seekables<?, ?>> ExecuteBlocking<S> atQuorum(Node node, Topologies topologies, SyncPoint<S> syncPoint, Timestamp executeAt)
        {
            return  new ExecuteBlocking<>(node, new QuorumTracker(topologies), syncPoint, executeAt);
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

    final Node node;
    final AbstractSimpleTracker<?> tracker;
    final SyncPoint<S> syncPoint;

    ExecuteSyncPoint(Node node, AbstractSimpleTracker<?> tracker, SyncPoint<S> syncPoint)
    {
        // TODO (required): this isn't correct, we need to potentially perform a second round if a dependency executes in a future epoch and we have lost ownership of that epoch
        this.node = node;
        this.tracker = tracker;
        this.syncPoint = syncPoint;
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
            // TODO (required): we also need to handle ranges not being safe to read
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
        if (tracker.recordFailure(from) == RequestStatus.Failed)
            tryFailure(new Exhausted(syncPoint.syncId, syncPoint.homeKey));
    }

    @Override
    public void onCallbackFailure(Node.Id from, Throwable failure)
    {
        tryFailure(failure);
    }
}
