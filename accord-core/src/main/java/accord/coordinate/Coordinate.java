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

import java.util.*;
import java.util.function.BiConsumer;

import accord.api.Result;
import accord.coordinate.tracking.FastPathTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.primitives.*;
import accord.topology.Topologies;
import accord.messages.Callback;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptOk;
import accord.messages.PreAccept.PreAcceptReply;
import com.google.common.base.Preconditions;

import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.messages.Commit.Invalidate.commitInvalidate;

import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class Coordinate extends AsyncResults.Settable<Result> implements Callback<PreAcceptReply>, BiConsumer<Result, Throwable>
{
    final Node node;
    final TxnId txnId;
    final Txn txn;
    final FullRoute<?> route;

    private final FastPathTracker tracker;
    private boolean preAcceptIsDone;
    private final List<PreAcceptOk> successes;

    private Coordinate(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        Topologies topologies = node.topology().withUnsyncedEpochs(route, txnId, txnId);
        this.tracker = new FastPathTracker(topologies);
        this.successes = new ArrayList<>(tracker.topologies().estimateUniqueNodes());
    }

    private void start()
    {
        // TODO (desired, efficiency): consider sending only to electorate of most recent topology (as only these PreAccept votes matter)
        // note that we must send to all replicas of old topology, as electorate may not be reachable
        node.send(tracker.nodes(), to -> new PreAccept(to, tracker.topologies(), txnId, txn, route), this);
    }

    public static AsyncResult<Result> coordinate(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        Coordinate coordinate = new Coordinate(node, txnId, txn, route);
        coordinate.start();
        return coordinate;
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
                preAcceptIsDone = true;
                tryFailure(new Timeout(txnId, route.homeKey()));
                break;
            case Success:
                onPreAccepted();
        }
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        tryFailure(failure);
    }

    @Override
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
            onPreAccepted();
    }

    private synchronized void onPreAccepted()
    {
        Preconditions.checkState(!preAcceptIsDone);
        preAcceptIsDone = true;

        if (tracker.hasFastPathAccepted())
        {
            Deps deps = Deps.merge(successes, ok -> ok.witnessedAt.equals(txnId) ? ok.deps : null);
            Execute.execute(node, txnId, txn, route, txnId, deps, this);
        }
        else
        {
            Deps deps = Deps.merge(successes, ok -> ok.deps);
            Timestamp executeAt; {
                Timestamp accumulate = Timestamp.NONE;
                for (PreAcceptOk preAcceptOk : successes)
                    accumulate = Timestamp.max(accumulate, preAcceptOk.witnessedAt);
                executeAt = accumulate;
            }

            // TODO (low priority, efficiency): perhaps don't submit Accept immediately if we almost have enough for fast-path,
            //                                  but by sending accept we rule out hybrid fast-path
            // TODO (low priority, efficiency): if we receive an expired response, perhaps defer to permit at least one other
            //                                  node to respond before invalidating
            if (node.agent().isExpired(txnId, executeAt.hlc()))
            {
                proposeInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), (success, fail) -> {
                    if (fail != null)
                    {
                        accept(null, fail);
                    }
                    else
                    {
                        node.withEpoch(executeAt.epoch(), () -> {
                            commitInvalidate(node, txnId, route, executeAt);
                            // TODO (required, API consistency): this should be Invalidated rather than Timeout?
                            accept(null, new Timeout(txnId, route.homeKey()));
                        });
                    }
                });
            }
            else
            {
                node.withEpoch(executeAt.epoch(), () -> {
                    Topologies topologies = tracker.topologies();
                    if (executeAt.epoch() > txnId.epoch())
                        topologies = node.topology().withUnsyncedEpochs(route, txnId.epoch(), executeAt.epoch());
                    Propose.propose(node, topologies, Ballot.ZERO, txnId, txn, route, executeAt, deps, this);
                });
            }
        }
    }

    @Override
    public void accept(Result success, Throwable failure)
    {
        if (failure instanceof CoordinateFailed)
            ((CoordinateFailed) failure).set(txnId, route.homeKey());

        if (success != null) trySuccess(success);
        else tryFailure(failure);
    }
}
