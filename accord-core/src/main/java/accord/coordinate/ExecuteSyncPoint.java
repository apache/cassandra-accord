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

import java.util.Collection;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import accord.api.Result.PersistableResult;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.DurabilityTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.api.ExclusiveAsyncExecutor;
import accord.local.durability.DurabilityResult;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.messages.ApplyThenWaitUntilApplied;
import accord.messages.Callback;
import accord.messages.ReadData;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadReply;
import accord.messages.SetShardDurable;
import accord.primitives.PartialSyncPoint;
import accord.primitives.SyncPoint;
import accord.primitives.Range;
import accord.primitives.Route;
import accord.primitives.Txn;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.Rethrowable;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.coordinate.CoordinationAdapter.Adapters.exclusiveSyncPoint;
import static accord.primitives.Status.Durability.HasOutcome.Quorum;
import static accord.primitives.Status.Durability.HasOutcome.Universal;
import static accord.topology.SelectShards.ALL;

public class ExecuteSyncPoint extends AbstractCoordination<Route<Range>, DurabilityResult, ReadReply, Void> implements Callback<ReadReply>
{
    public static class SyncPointErased extends Throwable implements Rethrowable<SyncPointErased>
    {
        public SyncPointErased() {}
        public SyncPointErased(Throwable cause) { super(cause); }
        @Override public SyncPointErased rethrowable() { return new SyncPointErased(this); }
    }

    public static class DurabilityResults implements BiConsumer<DurabilityResult, Throwable>
    {
        private final SettableResult<DurabilityResult> onDone = new SettableResult<>();
        private final SettableResult<DurabilityResult> onQuorumOrDone = new SettableResult<>();

        public AsyncResult<DurabilityResult> onDone() { return onDone; }
        public AsyncResult<DurabilityResult> onQuorumOrDone() { return onQuorumOrDone; }

        @Override
        public void accept(DurabilityResult success, Throwable failure)
        {
            if (failure != null)
            {
                onQuorumOrDone.tryFailure(failure);
                onDone.tryFailure(failure);
            }
            else
            {
                onQuorumOrDone.trySuccess(success);
                onDone.trySuccess(success);
            }
        }
    }

    final PartialSyncPoint syncPoint;

    final DurabilityResults results;
    // propagated from an earlier epoch's execution; our result is the minimum of this and the current execution
    final DurabilityResult partialResult;
    final DurabilityTracker tracker;
    final int attempt;
    boolean reportedQuorum, reportedMinorityQuorum, knownToSelf;
    long retryInFutureEpoch;

    protected ExecuteSyncPoint(Node node, ExclusiveAsyncExecutor executor, Topologies topologies, PartialSyncPoint syncPoint, int attempt, DurabilityResults callback)
    {
        this(node, executor, topologies, syncPoint, syncPoint.route, attempt, null, callback);
    }

    ExecuteSyncPoint(Node node, ExclusiveAsyncExecutor executor, Topologies topologies, PartialSyncPoint syncPoint, Route<Range> route, int attempt, DurabilityResult partialResult, DurabilityResults callback)
    {
        super(node, executor, syncPoint.syncId, route, topologies.nodes(), callback);
        this.syncPoint = syncPoint;
        this.partialResult = partialResult;
        this.attempt = attempt;
        this.tracker = new DurabilityTracker(topologies);
        this.results = callback;
    }

    public AsyncResult<DurabilityResult> onDone()
    {
        return results.onDone;
    }

    @Override
    void start()
    {
        node.agent().coordinatorEvents().onExecuting(syncPoint.syncId, null, syncPoint.waitFor, null);
        // TODO (desired): special Apply message that doesn't resend deps if path=MEDIUM
        Txn txn = node.agent().emptySystemTxn(syncPoint.syncId.kind(), syncPoint.syncId.domain());
        PersistableResult result = txn.result(syncPoint.syncId, syncPoint.executeAt, null).toPersistable();
        super.start();
        contact(to -> new ApplyThenWaitUntilApplied(to, tracker.topologies(), syncPoint.executeAt, tracker.topologies().currentEpoch(), syncPoint.fullRoute, syncPoint.syncId, txn, syncPoint.waitFor, scope, null, result));
    }

    @Override
    public void onSuccessInternal(Node.Id from, int fromIndex, ReadReply reply)
    {
        if (reply instanceof ReadData.ReadOkWithFutureEpoch)
            retryInFutureEpoch = Math.max(retryInFutureEpoch, ((ReadData.ReadOkWithFutureEpoch) reply).futureEpoch);

        if (!reply.isOk())
        {
            Invariants.require(reply instanceof CommitOrReadNack);
            CommitOrReadNack nack = (CommitOrReadNack) reply;
            switch (nack.kind)
            {
                default: throw new UnhandledEnum(nack.kind);

                case InsufficientEpochs:
                    sendApply(from, nack.minEpoch());
                    return;

                case InsufficientAndWaiting:
                    sendApply(from);
                    return;

                case Redundant:
                    finishOnFailure(new SyncPointErased());
                    return;

                case Waiting:
                    if (from.equals(node.id()))
                        knownToSelf = true;
            }
        }
        else
        {
            ReadData.ReadOk ok = (ReadData.ReadOk) reply;
            // TODO (expected): handle partial successes to achieve durability quorums
            update(ok.unavailable != null && !ok.unavailable.isEmpty()
                   ? tracker.recordFailure(from)
                   : tracker.recordSuccess(from)
            );
        }
    }

    protected void sendApply(Node.Id to)
    {
        CoordinateSyncPoint.sendApply(node, to, syncPoint, tracing);
    }

    protected void sendApply(Node.Id to, long minEpoch)
    {
        CoordinateSyncPoint.sendApply(node, to, syncPoint, minEpoch, tracker.topologies().currentEpoch(), tracing);
    }

    @Override
    public void onFailureInternal(Node.Id from, int fromIndex, Throwable failure)
    {
        recordFailure(failure);
        update(tracker.recordFailure(from));
    }

    private void maybeReportQuorums()
    {
        if (!reportedQuorum)
        {
            if (tracker.hasQuorumSuccess())
            {
                DurabilityResult current = current();
                results.onQuorumOrDone.trySuccess(current);
                node.durability().report(current);
                reportedQuorum = true;
            }
            else if (!reportedMinorityQuorum && tracker.hasMinorityQuorumSuccess())
            {
                DurabilityResult current = current();
                node.durability().report(current);
                reportedMinorityQuorum = true;
            }
        }
    }

    private void update(RequestStatus status)
    {
        if (status == RequestStatus.NoChange)
        {
            maybeReportQuorums();
            return;
        }

        Collection<Node.Id> failedNodes = tracker.excluding();
        if (status == RequestStatus.Failed)
            recordFailure(Exhausted.exhausted(node.agent(), txnId, syncPoint.route.homeKey(), scope.toRanges(), failedNodes));

        if (retryInFutureEpoch > tracker.topologies().currentEpoch())
        {
            awaitEpochAtLeastToFinish(retryInFutureEpoch, () -> {
                DurabilityResults results = (DurabilityResults) finishAndTakeCallback();
                ExecuteSyncPoint continuation;
                try
                {
                    Topologies topologies = node.topology().active().preciseEpochs(scope, tracker.topologies().currentEpoch(), retryInFutureEpoch, ALL);
                    continuation = new ExecuteSyncPoint(node, executor, topologies, syncPoint, scope, attempt, current(), results);
                }
                catch (Throwable t)
                {
                    results.accept(null, t);
                    return;
                }
                continuation.start();
            });
        }
        else
        {
            DurabilityResult result = current();
            if (result.min.remote == SyncRemote.All)
            {
                node.topology().onEpochRetired(scope.toRanges(), syncPoint.syncId);
                node.send(tracker.topologies(), new SetShardDurable(syncPoint, Universal), tracing);
            }
            else if (result.min.remote == SyncRemote.Quorum)
            {
                node.send(tracker.topologies(), new SetShardDurable(syncPoint, Quorum), tracing);
            }
            else
            {
                maybeReportQuorums();
            }
            node.durability().report(result);
            finishWithSuccess(result);
        }
    }

    DurabilityResult current()
    {
        DurabilityResult cur = new DurabilityResult(syncPoint, tracker.results(node.id(), knownToSelf), failure());
        if (partialResult == null)
            return cur;
        return partialResult.min(cur);
    }

    public static DurabilityResults coordinateIncluding(Node node, PartialSyncPoint syncPoint, ExclusiveAsyncExecutor executor, int attempt)
    {
        return coordinate(node, syncPoint, executor, attempt);
    }

    public static DurabilityResults coordinate(Node node, SyncPoint syncPoint, int attempt)
    {
        return coordinate(node, syncPoint, node.someExclusiveExecutor(), attempt);
    }

    public static DurabilityResults coordinate(Node node, PartialSyncPoint syncPoint, ExclusiveAsyncExecutor executor, int attempt)
    {
        DurabilityResults result = new DurabilityResults();
        try
        {
            Topologies topologies = exclusiveSyncPoint().forExecution(node, syncPoint.route, syncPoint.syncId, syncPoint.syncId, syncPoint.waitFor);
            ExecuteSyncPoint coordinate = new ExecuteSyncPoint(node, executor, topologies, syncPoint, attempt, result);
            coordinate.start();
        }
        catch (Throwable t)
        {
            result.accept(null, t);
        }
        return result;
    }

    @Override
    public CoordinationKind kind()
    {
        // TODO (desired): better name? to not confuse with normal execution, as this execution implies durability
        return CoordinationKind.ExecuteSyncPoint;
    }

    @Nonnull
    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }

    @Override
    public String describe()
    {
        // TODO (expected): report hard removed / stale nodes we're excluding, not only those requested by the caller
        return "exclude=" + tracker.topologies().staleOrRemovedIds().intersecting(tracker.nodes());
    }
}
