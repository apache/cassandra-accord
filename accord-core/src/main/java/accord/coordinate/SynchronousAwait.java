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

import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.coordinate.tracking.SimpleTracker;
import accord.local.Node;
import accord.api.ExclusiveAsyncExecutor;
import accord.messages.Await;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import static accord.topology.SelectShards.ALL;

/**
 * Synchronously await some set of replicas reaching a given wait condition.
 * This may or may not be a condition we expect to reach promptly, but we will wait only until the timeout passes
 * at which point we will report failure.
 */
public class SynchronousAwait extends AbstractCoordination<Participants<?>, Boolean, Await.AwaitOk, Void>
{
    final SimpleTracker<?> tracker;
    final Await.Until until;
    final boolean notifyProgressLog;

    public SynchronousAwait(Node node, ExclusiveAsyncExecutor executor, TxnId txnId, Participants<?> participants, SimpleTracker<?> tracker, Await.Until until, boolean notifyProgressLog, BiConsumer<? super Boolean, Throwable> callback)
    {
        super(node, executor, txnId, participants, tracker.nodes(), callback);
        this.until = until;
        this.notifyProgressLog = notifyProgressLog;
        this.tracker = tracker;
    }

    @Override
    void start()
    {
        super.start();
        contact(to -> new Await(to, tracker.topologies(), txnId, scope, until, notifyProgressLog));
    }

    public static AsyncChain<Boolean> awaitQuorum(Node node, ExclusiveAsyncExecutor executor, TxnId txnId, Participants<?> participants, Await.Until until, boolean notifyProgressLog)
    {
        // TODO (expected): copy this pattern elsewhere; should also make it easier to share exception handling logic etc
        return new AsyncChains.Head<>()
        {
            protected @Nullable @Override Cancellable start(BiConsumer<? super Boolean, Throwable> callback)
            {
                SynchronousAwait await;
                try
                {
                    Topologies topologies = node.topology().active().forEpoch(participants, txnId.epoch(), ALL);
                    await = new SynchronousAwait(node, executor, txnId, participants, new QuorumTracker(topologies), until, notifyProgressLog, callback);
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                    return null;
                }
                await.start();
                return null;
            }
        };
    }

    @Override
    public void onSuccessInternal(Node.Id from, int fromIndex, Await.AwaitOk reply)
    {
        RequestStatus status = tracker.recordSuccess(from);
        if (status != RequestStatus.NoChange)
            onDone(status);
    }

    @Override
    public void onFailureInternal(Node.Id from, int fromIndex, Throwable failure)
    {
        recordFailure(failure);
        RequestStatus status = tracker.recordFailure(from);
        if (status != RequestStatus.NoChange)
            onDone(status);
    }

    private void onDone(RequestStatus status)
    {
        if (status == RequestStatus.Success) finishWithSuccess(true);
        else finishOnFailure();
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.SyncAwait;
    }

    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }

    @Override
    public String describe()
    {
        return "blockedUntil=" + until +
               ", notifyProgressLog=" + notifyProgressLog;
    }
}

