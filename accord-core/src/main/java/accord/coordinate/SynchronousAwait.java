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

import accord.api.ProgressLog.BlockedUntil;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.coordinate.tracking.SimpleTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Await;
import accord.messages.Callback;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

/**
 * Synchronously await some set of replicas reaching a given wait condition.
 * This may or may not be a condition we expect to reach promptly, but we will wait only until the timeout passes
 * at which point we will report failure.
 */
public class SynchronousAwait implements Callback<Await.AwaitOk>
{
    final SimpleTracker<?> tracker;
    BiConsumer<? super Boolean, Throwable> callback;
    Throwable failure;

    public SynchronousAwait(SimpleTracker<?> tracker, BiConsumer<? super Boolean, Throwable> callback)
    {
        this.callback = callback;
        this.tracker = tracker;
    }

    public static AsyncChain<Boolean> awaitQuorum(Node node, Topologies topologies, TxnId txnId, BlockedUntil blockedUntil, boolean notifyProgressLog, Participants<?> participants)
    {
        // TODO (expected): copy this pattern elsewhere; should also make it easier to share exception handling logic etc
        AsyncChains.Head<Boolean> head = new AsyncChains.Head<>()
        {
            protected @Nullable @Override Cancellable start(BiConsumer<? super Boolean, Throwable> callback)
            {
                SynchronousAwait await = new SynchronousAwait(new QuorumTracker(topologies), callback);
                node.send(topologies.nodes(), to -> new Await(to, topologies, txnId, participants, blockedUntil, notifyProgressLog), await);
                return null;
            }
        };
        return head.maybeWrapDebug();
    }

    @Override
    public void onSuccess(Id from, Await.AwaitOk reply)
    {
        RequestStatus status = tracker.recordSuccess(from);
        if (status != RequestStatus.NoChange)
            onDone(status);
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        RequestStatus status = tracker.recordFailure(from);
        if (status != RequestStatus.NoChange)
            onDone(status);
    }

    private void onDone(RequestStatus status)
    {
        Invariants.require(callback != null);
        BiConsumer<? super Boolean, Throwable> callback = this.callback;
        this.callback = null;
        if (status == RequestStatus.Success) callback.accept(true, null);
        else callback.accept(null, this.failure != null ? this.failure : new Timeout(null, null));
    }
}

