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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.tracking.FastPathTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptOk;
import accord.messages.PreAccept.PreAcceptReply;
import accord.primitives.FullRoute;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncResults;

import static accord.utils.Invariants.checkState;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
abstract class CoordinatePreAccept<T> extends AsyncResults.SettableResult<T> implements Callback<PreAcceptReply>, BiConsumer<T, Throwable>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CoordinatePreAccept.class);

    final Node node;
    final TxnId txnId;
    final Txn txn;
    final FullRoute<?> route;

    final FastPathTracker tracker;
    private boolean preAcceptIsDone;
    private final List<PreAcceptOk> successes;

    CoordinatePreAccept(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        Topologies topologies = node.topology().withUnsyncedEpochs(route, txnId, txnId);
        this.tracker = new FastPathTracker(topologies);
        this.successes = new ArrayList<>(tracker.topologies().estimateUniqueNodes());
    }

    void start()
    {
        // TODO (desired, efficiency): consider sending only to electorate of most recent topology (as only these PreAccept votes matter)
        // note that we must send to all replicas of old topology, as electorate may not be reachable
        node.send(tracker.nodes(), to -> new PreAccept(to, tracker.topologies(), txnId, txn, route), this);
    }

    @Override
    public synchronized void onFailure(Id from, Throwable failure)
    {
        if (preAcceptIsDone)
            return;

        switch (tracker.recordFailure(from))
        {
            default: throw new AssertionError();
            case NoChange:
                break;
            case Failed:
                checkState(!preAcceptIsDone);
                preAcceptIsDone = true;
                tryFailure(new Timeout(txnId, route.homeKey()));
                break;
            case Success:
                checkState(!preAcceptIsDone);
                preAcceptIsDone = true;
                onPreAccepted(successes);
        }
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        tryFailure(failure);
    }

    public synchronized void onSuccess(Id from, PreAcceptReply reply)
    {
        if (preAcceptIsDone)
            return;

        if (!reply.isOk())
        {
            // we've been preempted by a recovery coordinator; defer to it, and wait to hear any result
            tryFailure(new Preempted(txnId, route.homeKey()));
            return;
        }

        PreAcceptOk ok = (PreAcceptOk) reply;
        successes.add(ok);

        boolean fastPath = ok.witnessedAt.compareTo(txnId) == 0;
        // TODO (desired, safety): update formalisation (and proof), as we do not seek additional pre-accepts from later epochs.
        //                         instead we rely on accept to do our work: a quorum of accept in the later epoch
        //                         and its effect on preaccepted timestamps and the deps it returns create our sync point.
        if (tracker.recordSuccess(from, fastPath) == RequestStatus.Success)
        {
            checkState(!preAcceptIsDone);
            preAcceptIsDone = true;
            onPreAccepted(successes);
        }
    }

    abstract void onPreAccepted(List<PreAcceptOk> successes);

    @Override
    public void accept(T success, Throwable failure)
    {
        if (failure instanceof CoordinationFailed)
            ((CoordinationFailed) failure).set(txnId, route.homeKey());

        if (success != null) trySuccess(success);
        else tryFailure(failure);
    }
}
