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
import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import accord.api.Result;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.DurabilityTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.local.SequentialAsyncExecutor;
import accord.local.durability.DurabilityResult;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.messages.ApplyThenWaitUntilApplied;
import accord.messages.Callback;
import accord.messages.ReadData;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadReply;
import accord.messages.SetShardDurable;
import accord.primitives.FullRoute;
import accord.primitives.Range;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
import accord.topology.Topologies;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.UnhandledEnum;
import accord.utils.WrappableException;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.coordinate.CoordinationAdapter.Adapters.exclusiveSyncPoint;
import static accord.primitives.Status.Durability.HasOutcome.Quorum;
import static accord.primitives.Status.Durability.HasOutcome.Universal;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;

public class ExecuteSyncPoint extends AbstractCoordination<FullRoute<?>, DurabilityResult, ReadReply, Void> implements Callback<ReadReply>
{
    public static class SyncPointErased extends Throwable implements WrappableException<SyncPointErased>
    {
        public SyncPointErased() {}
        public SyncPointErased(Throwable cause) { super(cause); }
        @Override public SyncPointErased wrap() { return new SyncPointErased(this); }
    }

    public static class DurabilityResults implements BiConsumer<DurabilityResult, Throwable>
    {
        private final SettableResult<DurabilityResult> onDone = new SettableResult<>();
        private final SettableResult<DurabilityResult> onQuorum = new SettableResult<>();

        public AsyncResult<DurabilityResult> onDone() { return onDone; }
        public AsyncResult<DurabilityResult> onQuorum() { return onQuorum; }

        @Override
        public void accept(DurabilityResult success, Throwable failure)
        {
            if (failure != null)
            {
                onQuorum.tryFailure(failure);
                onDone.tryFailure(failure);
            }
            else
            {
                onQuorum.trySuccess(success);
                onDone.trySuccess(success);
            }
        }
    }

    final SyncPoint<Range> syncPoint;

    final DurabilityResults results;
    final DurabilityResult partialResult;
    final Set<Node.Id> excludeSuccess;
    final DurabilityTracker tracker;
    final int attempt;
    boolean reportedQuorum, reportedMinorityQuorum;
    long retryInFutureEpoch;

    protected ExecuteSyncPoint(Node node, SyncPoint<Range> syncPoint, Set<Node.Id> excludeSuccess, SequentialAsyncExecutor executor, int attempt, DurabilityResults callback)
    {
        this(node, syncPoint, exclusiveSyncPoint().forExecution(node, syncPoint.route(), SHARE, syncPoint.syncId, syncPoint.syncId, syncPoint.waitFor), excludeSuccess, executor, attempt, null, callback);
    }

    ExecuteSyncPoint(Node node, SyncPoint<Range> syncPoint, Function<Topologies, Set<Node.Id>> excludeSuccess, SequentialAsyncExecutor executor, int attempt, DurabilityResults callback)
    {
        this(node, syncPoint, exclusiveSyncPoint().forExecution(node, syncPoint.route(), SHARE, syncPoint.syncId, syncPoint.syncId, syncPoint.waitFor), excludeSuccess, executor, attempt, callback);
    }

    ExecuteSyncPoint(Node node, SyncPoint<Range> syncPoint, Topologies topologies, Function<Topologies, Set<Node.Id>> excludeSuccess, SequentialAsyncExecutor executor, int attempt, DurabilityResults callback)
    {
        this(node, syncPoint, topologies, excludeSuccess.apply(topologies), executor, attempt, null, callback);
    }

    ExecuteSyncPoint(Node node, SyncPoint<Range> syncPoint, Topologies topologies, Set<Node.Id> excludeSuccess, SequentialAsyncExecutor executor, int attempt, DurabilityResult partialResult, DurabilityResults callback)
    {
        super(node, executor, syncPoint.syncId, syncPoint.route, topologies.nodes(), callback);
        this.syncPoint = syncPoint;
        this.partialResult = partialResult;
        this.excludeSuccess = excludeSuccess;
        this.attempt = attempt;
        this.tracker = new DurabilityTracker(topologies, excludeSuccess);
        this.results = callback;
    }

    public AsyncResult<DurabilityResult> onQuorum()
    {
        return results.onQuorum;
    }

    public AsyncResult<DurabilityResult> onDone()
    {
        return results.onDone;
    }

    @Override
    void start()
    {
        node.agent().coordinatorEvents().onExecuting(syncPoint.syncId, null, syncPoint.waitFor, null);
        SortedArrayList<Node.Id> contact = tracker.filterAndRecordFaulty();
        // TODO (desired): special Apply message that doesn't resend deps if path=MEDIUM
        Txn txn = node.agent().emptySystemTxn(syncPoint.syncId.kind(), syncPoint.syncId.domain());
        Result result = txn.result(syncPoint.syncId, syncPoint.executeAt, null);
        if (contact == null) finishOnExaustion();
        else
        {
            super.start();
            contact(to -> new ApplyThenWaitUntilApplied(to, tracker.topologies(), syncPoint.executeAt, tracker.topologies().currentEpoch(), syncPoint.route, syncPoint.syncId, txn, syncPoint.waitFor, syncPoint.route, null, result));
        }
    }

    @Override
    public void onSuccessInternal(Node.Id from, int fromIndex, ReadReply reply)
    {
        if (reply instanceof ReadData.ReadOkWithFutureEpoch)
            retryInFutureEpoch = Math.max(retryInFutureEpoch, ((ReadData.ReadOkWithFutureEpoch) reply).futureEpoch);

        if (!reply.isOk())
        {
            CommitOrReadNack nack = (CommitOrReadNack) reply;
            switch (nack.kind)
            {
                default: throw new UnhandledEnum(nack.kind);

                case InsufficientEpochs:
                    sendApply(from, nack.minEpoch());
                    return;

                case Insufficient:
                    sendApply(from);
                    return;

                case Redundant:
                    finishWithFailureOverride(new SyncPointErased());
                    return;

                case Waiting:
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
        CoordinateSyncPoint.sendApply(node, to, syncPoint);
    }

    protected void sendApply(Node.Id to, long minEpoch)
    {
        CoordinateSyncPoint.sendApply(node, to, syncPoint, minEpoch, tracker.topologies().currentEpoch());
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
                results.onQuorum.trySuccess(current);
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

        Collection<Node.Id> failedNodes = tracker.failures();
        if (status == RequestStatus.Failed)
            recordFailure(Exhausted.exhausted(node.agent(), txnId, syncPoint.route.homeKey(), syncPoint.route.toRanges(), failedNodes));

        if (retryInFutureEpoch > tracker.topologies().currentEpoch())
        {
            awaitEpochAtLeastToFinish(retryInFutureEpoch, () -> {
                ExecuteSyncPoint continuation = new ExecuteSyncPoint(node, syncPoint, node.topology().preciseEpochs(syncPoint.route(), tracker.topologies().currentEpoch(), retryInFutureEpoch, SHARE), excludeSuccess, executor, attempt, current(), (DurabilityResults) finishAndTakeCallback());
                continuation.start();
            });
        }
        else
        {
            DurabilityResult result = current();
            if (result.achievedRemote == SyncRemote.All)
            {
                node.configService().reportEpochRetired(syncPoint.route.toRanges(), syncPoint.syncId.epoch() - 1);
                node.send(tracker.nodes(), new SetShardDurable(syncPoint, Universal));
            }
            else if (result.achievedRemote == SyncRemote.Quorum)
            {
                node.send(tracker.nodes(), new SetShardDurable(syncPoint, Quorum));
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
        DurabilityResult cur = new DurabilityResult(syncPoint, tracker.achievedLocal(node.id()), tracker.achievedRemote(), tracker.failures(), failure());
        if (partialResult == null)
            return cur;
        return partialResult.merge(cur);
    }

    public static DurabilityResults coordinate(Node node, SyncPoint<Range> exclusiveSyncPoint, int attempt)
    {
        return coordinate(node, ignore -> Collections.emptySet(), exclusiveSyncPoint, attempt);
    }

    public static DurabilityResults coordinateIncluding(Node node, SyncPoint<Range> exclusiveSyncPoint, @Nullable Collection<Node.Id> including, int attempt)
    {
        return coordinateIncluding(node, exclusiveSyncPoint, including, node.someSequentialExecutor(), attempt);
    }

    public static DurabilityResults coordinateIncluding(Node node, SyncPoint<Range> exclusiveSyncPoint, @Nullable Collection<Node.Id> including, SequentialAsyncExecutor executor, int attempt)
    {
        return coordinate(node, including == null ? ignore -> Collections.emptySet() : topologies -> topologies.nodes().without(including::contains), exclusiveSyncPoint, executor, attempt);
    }

    public static DurabilityResults coordinate(Node node, Function<Topologies, Set<Node.Id>> excludeSuccess, SyncPoint<Range> exclusiveSyncPoint, int attempt)
    {
        return coordinate(node, excludeSuccess, exclusiveSyncPoint, null, attempt);
    }

    public static DurabilityResults coordinate(Node node, Function<Topologies, Set<Node.Id>> excludeSuccess, SyncPoint<Range> syncPoint, SequentialAsyncExecutor executor, int attempt)
    {
        DurabilityResults result = new DurabilityResults();
        try
        {
            ExecuteSyncPoint coordinate = new ExecuteSyncPoint(node, syncPoint, excludeSuccess, executor, attempt, result);
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

    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }

    @Override
    public String describe()
    {
        return "exclude=" + excludeSuccess;
    }
}
