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
import accord.coordinate.tracking.AwaitTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Commands;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Await;
import accord.messages.Await.AwaitOk;
import accord.messages.Callback;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.Invariants;

import static accord.coordinate.tracking.RequestStatus.Success;

/**
 * Perform a distributed wait operation: each commandStore that is contacted either reports that the relevant key(s)
 * meet the required wait condition, or else registers a callback that will be notified once the wait condition is met
 * on the replica for those keys. The response informs the initiating operation which keys met the wait condition
 * before replying, and which are awaiting a distributed response.
 *
 * Note that remote listeners may have been registered regardless, as replicas may not be in agreement for each key.
 * Any listener must be resilient to redundant callbacks.
 *
 * Asynchronous awaits will not time out if the wait is longer than message/request timeouts.
 */
public class AsynchronousAwait implements Callback<AwaitOk>
{
    // TODO (desired, efficiency): this should collect the executeAt of any commit, and terminate as soon as one is found
    //                             that is earlier than TxnId for the Txn we are recovering; if all commits we wait for
    //                             are given earlier timestamps we can retry without restarting.

    public static class SynchronousResult
    {
        public final Unseekables<?> ready;
        public final @Nullable Unseekables<?> notReady;

        public SynchronousResult(Unseekables<?> ready, @Nullable Unseekables<?> notReady)
        {
            this.ready = ready;
            this.notReady = notReady;
        }
    }

    final TxnId txnId;
    final Participants<?> contact;
    final AwaitTracker tracker;
    final int asynchronousCallbackId;
    final BiConsumer<SynchronousResult, Throwable> synchronousCallback;
    private boolean isDone;
    private Throwable failure;

    public AsynchronousAwait(TxnId txnId, Participants<?> contact, AwaitTracker tracker, int asynchronousCallbackId, BiConsumer<SynchronousResult, Throwable> synchronousCallback)
    {
        this.txnId = txnId;
        this.contact = contact;
        this.tracker = tracker;
        this.asynchronousCallbackId = asynchronousCallbackId;
        this.synchronousCallback = synchronousCallback;
    }

    /**
     * we require a Route to contact so we can be sure a home shard recipient invokes {@link Commands#updateParticipants},
     * notifying the progress log of a Route to determine it is the home shard.
     */
    public static AsynchronousAwait awaitAny(Node node, Topologies topologies, TxnId txnId, Route<?> contact, BlockedUntil awaiting, int asynchronousCallbackId, BiConsumer<SynchronousResult, Throwable> synchronousCallback)
    {
        Invariants.checkArgument(topologies.size() == 1);
        AwaitTracker tracker = new AwaitTracker(topologies);
        AsynchronousAwait result = new AsynchronousAwait(txnId, contact, tracker, asynchronousCallbackId, synchronousCallback);
        result.start(node, topologies, contact, awaiting);
        return result;
    }

    private void start(Node node, Topologies topologies, Route<?> route, BlockedUntil blockedUntil)
    {
        node.send(topologies.nodes(), to -> new Await(to, topologies, txnId, route, blockedUntil, asynchronousCallbackId), this);
    }

    @Override
    public synchronized void onSuccess(Id from, AwaitOk reply)
    {
        if (isDone) return;

        if (tracker.recordSuccess(from, reply == AwaitOk.Ready) == Success)
            onSuccess();
    }

    @Override
    public synchronized void onFailure(Id from, Throwable failure)
    {
        if (isDone) return;

        this.failure = FailureAccumulator.append(this.failure, failure);
        RequestStatus status = tracker.recordFailure(from);
        switch (status)
        {
            default: throw new AssertionError("Unhandled RequestStatus: " + status);
            case NoChange: break;
            case Success:
                onSuccess();
                break;
            case Failed:
                isDone = true;
                synchronousCallback.accept(null, this.failure);
        }
    }

    private void onSuccess()
    {
        Unseekables<?> ready = tracker.ready(contact);
        Unseekables<?> notReady = tracker.notReady(contact);
        if (notReady.isEmpty())
            notReady = null;
        isDone = true;
        synchronousCallback.accept(new SynchronousResult(ready, notReady), null);
    }

    @Override
    public synchronized void onCallbackFailure(Id from, Throwable failure)
    {
        if (isDone) return;

        isDone = true;
        synchronousCallback.accept(null, failure);
    }
}

