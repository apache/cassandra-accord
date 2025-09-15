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
import accord.coordinate.tracking.AwaitTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Commands;
import accord.local.Node;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Await;
import accord.messages.Await.AwaitOk;
import accord.messages.Callback;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

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
public class AsynchronousAwait extends AbstractCoordination<Participants<?>, AsynchronousAwait.SynchronousResult, AwaitOk, Void> implements Callback<AwaitOk>
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

    final AwaitTracker tracker;
    final Await.Until until;
    final int asynchronousCallbackId;
    final boolean notifyProgressLog;

    public AsynchronousAwait(Node node, SequentialAsyncExecutor executor, Participants<?> contact, TxnId txnId, AwaitTracker tracker, Await.Until until, boolean notifyProgressLog, int asynchronousCallbackId, BiConsumer<SynchronousResult, Throwable> synchronousCallback)
    {
        super(node, executor, txnId, contact, tracker.nodes(), synchronousCallback);
        this.tracker = tracker;
        this.until = until;
        this.asynchronousCallbackId = asynchronousCallbackId;
        this.notifyProgressLog = notifyProgressLog;
    }

    public static AsynchronousAwait awaitAny(Node node, Topologies topologies, TxnId txnId, Route<?> contact, Await.Until until, int asynchronousCallbackId, BiConsumer<SynchronousResult, Throwable> synchronousCallback)
    {
        return awaitAny(node, node.someSequentialExecutor(), topologies, txnId, contact, until, true, asynchronousCallbackId, synchronousCallback);
    }

    /**
     * we require a Route to contact so we can be sure a home shard recipient invokes {@link Commands#supplementParticipants},
     * notifying the progress log of a Route to determine it is the home shard.
     */
    public static AsynchronousAwait awaitAny(Node node, SequentialAsyncExecutor executor, Topologies topologies, TxnId txnId, Route<?> contact, Await.Until until, boolean notifyProgressLog, int asynchronousCallbackId, BiConsumer<SynchronousResult, Throwable> synchronousCallback)
    {
        Invariants.requireArgument(topologies.size() == 1);
        AwaitTracker tracker = new AwaitTracker(topologies);
        AsynchronousAwait result = new AsynchronousAwait(node, executor, contact, txnId, tracker, until, notifyProgressLog, asynchronousCallbackId, synchronousCallback);
        result.start();
        return result;
    }

    @Override
    void start()
    {
        super.start();
        contact(to -> new Await(to, tracker.topologies(), txnId, scope, until, asynchronousCallbackId, notifyProgressLog));
    }

    @Override
    public void onSuccessInternal(Node.Id from, int fromIndex, AwaitOk reply)
    {
        if (tracker.recordSuccess(from, reply == AwaitOk.Ready) == Success)
            onSuccess();
    }

    @Override
    public void onFailureInternal(Node.Id from, int fromIndex, Throwable failure)
    {
        recordFailure(failure);
        RequestStatus status = tracker.recordFailure(from);
        switch (status)
        {
            default: throw new UnhandledEnum(status);
            case NoChange: break;
            case Success:
                onSuccess();
                break;
            case Failed:
                finishOnFailure();
        }
    }

    private void onSuccess()
    {
        Unseekables<?> ready = tracker.ready(scope);
        Unseekables<?> notReady = tracker.notReady(scope);
        if (notReady.isEmpty())
            notReady = null;

        finishWithSuccess(new SynchronousResult(ready, notReady));
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.AsyncAwait;
    }

    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }
}

