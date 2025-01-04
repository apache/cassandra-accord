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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import accord.api.ProgressLog.BlockedUntil;
import accord.api.Result;
import accord.coordinate.tracking.RecoveryTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Known.KnownDeps;
import accord.primitives.Status;
import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.LatestDeps;
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.SortedListMap;
import accord.utils.WrappableException;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections;
import static accord.coordinate.CoordinationAdapter.Factory.Kind.Recovery;
import static accord.coordinate.ExecutePath.RECOVER;
import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;
import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.messages.BeginRecovery.RecoverOk.maxAccepted;
import static accord.messages.BeginRecovery.RecoverOk.maxAcceptedNotTruncated;
import static accord.primitives.Known.KnownDeps.DepsCommitted;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.ProgressToken.TRUNCATED_DURABLE_OR_INVALIDATED;
import static accord.utils.Invariants.illegalState;

// TODO (low priority, cleanup): rename to Recover (verb); rename Recover message to not clash
// TODO (expected): do not wait for a quorum if we find enough information to execute
public class Recover implements Callback<RecoverReply>, BiConsumer<Result, Throwable>
{
    // TODO (required): make sure we contact and wait (at least until timeout)
    //  for an ACK from any replica that witnessed a txn as something to wait for
    AsyncResult<Object> awaitCommits(Node node, Deps waitOn)
    {
        AtomicInteger remaining = new AtomicInteger(waitOn.txnIdCount());
        AsyncResult.Settable<Object> result = AsyncResults.settable();
        for (int i = 0 ; i < waitOn.txnIdCount() ; ++i)
        {
            TxnId txnId = waitOn.txnId(i);
            Participants<?> participants = waitOn.participants(txnId);
            long sinceEpoch = Math.min(txnId.epoch(), Recover.this.txnId.epoch());
            Topologies topologies = this.tracker.topologies().select(participants, sinceEpoch);
            SynchronousAwait.awaitQuorum(node, topologies, txnId, route.homeKey(), BlockedUntil.HasCommittedDeps, participants).addCallback((success, failure) -> {
                if (result.isDone())
                    return;
                if (failure == null && remaining.decrementAndGet() == 0)
                    result.setSuccess(success);
                else
                    result.tryFailure(failure);
            });
        }
        return result;
    }

    private final CoordinationAdapter<Result> adapter;
    private final Node node;
    private final Ballot ballot;
    private final TxnId txnId;
    private final Txn txn;
    private final FullRoute<?> route;
    private final @Nullable Timestamp committedExecuteAt;
    private final BiConsumer<Outcome, Throwable> callback;
    private boolean isDone;

    private SortedListMap<Id, RecoverOk> recoverOks;
    private final RecoveryTracker tracker;
    private boolean isBallotPromised;

    private Recover(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, @Nullable Timestamp committedExecuteAt, BiConsumer<Outcome, Throwable> callback)
    {
        this.committedExecuteAt = committedExecuteAt;
        Invariants.checkState(txnId.isVisible());
        this.adapter = node.coordinationAdapter(txnId, Recovery);
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        this.callback = callback;
        this.tracker = new RecoveryTracker(topologies);
        this.recoverOks = new SortedListMap<>(topologies.nodes(), RecoverOk[]::new);
    }

    @Override
    public void accept(Result result, Throwable failure)
    {
        Invariants.checkState(!isDone);
        isDone = true;
        if (failure == null)
        {
            callback.accept(ProgressToken.APPLIED, null);
            node.agent().metricsEventsListener().onRecover(txnId, ballot);
        }
        else if (failure instanceof Redundant)
        {
            retry(((Redundant) failure).committedExecuteAt);
        }
        else
        {
            callback.accept(null, WrappableException.wrap(failure));
            if (failure instanceof Preempted)
                node.agent().metricsEventsListener().onPreempted(txnId);
            else if (failure instanceof Timeout)
                node.agent().metricsEventsListener().onTimeout(txnId);
            else if (failure instanceof Invalidated) // TODO (expected): should we tick this counter? we haven't invalidated anything
                node.agent().metricsEventsListener().onInvalidated(txnId);
        }

        node.agent().onRecover(node, result, failure);
    }

    public static Recover recover(Node node, TxnId txnId, Txn txn, FullRoute<?> route, BiConsumer<Outcome, Throwable> callback)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        return recover(node, ballot, txnId, txn, route, callback);
    }

    private static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, ballot, txnId, txn, route, null, callback);
    }

    public static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, BiConsumer<Outcome, Throwable> callback)
    {
        Topologies topologies = node.topology().select(route, txnId, executeAt == null ? txnId : executeAt, QuorumEpochIntersections.recover);
        return recover(node, topologies, ballot, txnId, txn, route, executeAt, callback);
    }

    private static Recover recover(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, BiConsumer<Outcome, Throwable> callback)
    {
        Recover recover = new Recover(node, topologies, ballot, txnId, txn, route, executeAt, callback);
        recover.start(topologies.nodes());
        return recover;
    }

    void start(Collection<Id> nodes)
    {
        node.send(nodes, to -> new BeginRecovery(to, tracker.topologies(), txnId, committedExecuteAt, txn, route, ballot), this);
    }

    @Override
    public synchronized void onSuccess(Id from, RecoverReply reply)
    {
        if (isDone || isBallotPromised)
            return;

        boolean acceptsFastPath;
        switch (reply.kind())
        {
            default: throw new AssertionError("Unhandled RecoverReply.Kind: " + reply.kind());
            case Reject:
            case Truncated:
                // TODO (expected): handle partial truncation
                accept(null, new Preempted(txnId, route.homeKey()));
                return;

            case Ok:
                RecoverOk ok = (RecoverOk) reply;
                recoverOks.put(from, ok);
                acceptsFastPath = ok.selfAcceptsFastPath;
                break;

            case Retired:
                acceptsFastPath = true;
        }

        if (tracker.recordSuccess(from, acceptsFastPath) == Success)
            recover();
    }

    private void recover()
    {
        Invariants.checkState(!isBallotPromised);
        isBallotPromised = true;

        SortedListMap<Id, RecoverOk> recoverOks = this.recoverOks;
        if (!Invariants.debug()) this.recoverOks = null;
        List<RecoverOk> recoverOkList = recoverOks.valuesAsNullableList();
        RecoverOk acceptOrCommit = maxAccepted(recoverOkList);
        RecoverOk acceptOrCommitNotTruncated = acceptOrCommit == null || acceptOrCommit.status != Status.Truncated
                                               ? acceptOrCommit : maxAcceptedNotTruncated(recoverOkList);

        if (acceptOrCommitNotTruncated != null)
        {
            Status status = acceptOrCommitNotTruncated.status;
            Timestamp executeAt = acceptOrCommitNotTruncated.executeAt;
            if (committedExecuteAt != null)
            {
                Invariants.checkState(acceptOrCommitNotTruncated.status.compareTo(Status.PreCommitted) < 0 || executeAt.equals(committedExecuteAt));
                // if we know from a prior Accept attempt that this is committed we can go straight to the commit phase
                if (status == Status.Accepted)
                    status = Status.Committed;
            }
            switch (status)
            {
                default: throw new AssertionError("Unhandled Status: " + status);
                case Truncated: throw illegalState("Truncate should be filtered");
                case Invalidated:
                {
                    commitInvalidate(invalidateUntil(recoverOks));
                    return;
                }

                case Applied:
                case PreApplied:
                {
                    withStableDeps(recoverOkList, executeAt, (i, t) -> node.agent().acceptAndWrap(i, t), stableDeps -> {
                        // TODO (required): if we have to calculate deps for any shard, should we first ensure they are stable?
                        //   it should only be possible for a fast path decision, but we might have Stable in only one shard.
                        // TODO (future development correctness): when writes/result are partially replicated, need to confirm we have quorum of these
                        adapter.persist(node, tracker.topologies(), route, txnId, txn, executeAt, stableDeps, acceptOrCommitNotTruncated.writes, acceptOrCommitNotTruncated.result, (i, t) -> node.agent().acceptAndWrap(i, t));
                    });
                    accept(acceptOrCommitNotTruncated.result, null);
                    return;
                }

                case Stable:
                {
                    // TODO (required): if we have to calculate deps for any shard, should we first ensure they are stable?
                    //   it should only be possible for a fast path decision, but we might have Stable in only one shard.
                    withStableDeps(recoverOkList, executeAt, this, stableDeps -> {
                        adapter.execute(node, tracker.topologies(), route, RECOVER, txnId, txn, executeAt, stableDeps, this);
                    });
                    return;
                }

                case PreCommitted:
                case Committed:
                {
                    withCommittedDeps(recoverOkList, executeAt, this, committedDeps -> {
                        adapter.stabilise(node, tracker.topologies(), route, ballot, txnId, txn, executeAt, committedDeps, this);
                    });
                    return;
                }

                case Accepted:
                {
                    // TODO (desired): if we have a quorum of Accept with matching ballot or proposal we can go straight to Commit
                    // TODO (desired, behaviour): if we didn't find Accepted in *every* shard, consider invalidating for consistency of behaviour
                    //     however, note that we may have taken the fast path and recovered, so we can only do this if acceptedOrCommitted=Ballot.ZERO
                    //     (otherwise recovery was attempted and did not invalidate, so it must have determined it needed to complete)
                    Deps proposeDeps = LatestDeps.mergeProposal(recoverOkList, ok -> ok == null ? null : ok.deps);
                    propose(acceptOrCommitNotTruncated.executeAt, proposeDeps);
                    return;
                }

                case AcceptedInvalidate:
                {
                    invalidate(recoverOks);
                    return;
                }

                case NotDefined:
                case PreAccepted:
                    throw illegalState("Should only be possible to have Accepted or later commands");
            }
        }

        if (acceptOrCommit != null)
        {
            // TODO (required): match logic in Invalidate; we need to know which keys have been invalidated
            Topologies topologies = tracker.topologies();
            boolean allShardsTruncated = true;
            for (int topologyIndex = 0 ; topologyIndex < topologies.size() ; ++topologyIndex)
            {
                Topology topology = topologies.get(topologyIndex);
                for (Shard shard : topology.shards())
                {
                    RecoverOk maxReply = maxAccepted(recoverOks.select(shard.nodes));
                    allShardsTruncated &= maxReply.status == Status.Truncated;
                }
                if (allShardsTruncated)
                {
                    // TODO (required, correctness): this is not a safe inference in the case of an ErasedOrInvalidOrVestigial response.
                    //   We need to tighten up the inference and spread of truncation/invalid outcomes.
                    //   In this case, at minimum this can lead to liveness violations as the home shard stops coordinating
                    //   a command that it hasn't invalidated, but nor is it possible to recover. This happens because
                    //   when the home shard shares all of its replicas with another shard that has autonomously invalidated
                    //   the transaction, so that all received InvalidateReply show truncation (when in fact this is only partial).
                    //   We could paper over this, but better to revisit and provide stronger invariants we can rely on.
                    isDone = true;
                    callback.accept(TRUNCATED_DURABLE_OR_INVALIDATED, null);
                    return;
                }
            }
        }

        if (tracker.rejectsFastPath() || supersedingRejects(route, recoverOkList))
        {
            invalidate(recoverOks);
            return;
        }

        Invariants.checkState(committedExecuteAt == null || committedExecuteAt.equals(txnId));
        // should all be PreAccept
        Deps proposeDeps = LatestDeps.mergeProposal(recoverOkList, ok -> ok == null ? null : ok.deps);
        Deps earlierWait = Deps.merge(recoverOkList, recoverOkList.size(), List::get, ok -> ok.earlierWait);
        Deps earlierNoWait = Deps.merge(recoverOkList, recoverOkList.size(), List::get, ok -> ok.earlierNoWait);
        earlierWait = earlierWait.without(earlierNoWait);
        if (!earlierWait.isEmpty())
        {
            // If there exist commands that were proposed an earlier execution time than us that have not witnessed us,
            // we have to be certain these commands have not successfully committed without witnessing us (thereby
            // ruling out a fast path decision for us and changing our recovery decision).
            // So, we wait for these commands to finish committing before retrying recovery.
            // See whitepaper for more details
            awaitCommits(node, earlierWait).addCallback((success, failure) -> {
                if (failure != null) accept(null, failure);
                else retry();
            });
            return;
        }

        propose(txnId, proposeDeps);
    }

    private static boolean supersedingRejects(FullRoute<?> route, List<RecoverOk> oks)
    {
        for (RecoverOk ok : oks)
        {
            if (ok != null && ok.supersedingRejects)
                return true;
        }
        return false;
    }

    private void withCommittedDeps(List<RecoverOk> nullableRecoverOkList, Timestamp executeAt, BiConsumer<?, Throwable> failureCallback, Consumer<Deps> withDeps)
    {
        withCommittedOrStableDeps(DepsCommitted, nullableRecoverOkList, executeAt, failureCallback, withDeps);
    }

    private void withStableDeps(List<RecoverOk> nullableRecoverOkList, Timestamp executeAt, BiConsumer<?, Throwable> failureCallback, Consumer<Deps> withDeps)
    {
        withCommittedOrStableDeps(DepsKnown, nullableRecoverOkList, executeAt, failureCallback, withDeps);
    }

    private void withCommittedOrStableDeps(KnownDeps atLeast, List<RecoverOk> nullableRecoverOkList, Timestamp executeAt, BiConsumer<?, Throwable> failureCallback, Consumer<Deps> withDeps)
    {
        LatestDeps.MergedCommitResult merged = LatestDeps.mergeCommit(atLeast, txnId, executeAt, nullableRecoverOkList, txnId, ok -> ok == null ? null : ok.deps);
        node.withEpoch(executeAt.epoch(), failureCallback, () -> {
            Unseekables<?> missing = route.without(merged.sufficientFor);
            if (missing.isEmpty())
            {
                withDeps.accept(merged.deps);
            }
            else
            {
                CollectLatestDeps.withLatestDeps(node, txnId, route, missing, executeAt, (extraDeps, fail) -> {
                    if (fail != null)
                    {
                        failureCallback.accept(null, fail);
                    }
                    else
                    {
                        withDeps.accept(merged.deps.with(LatestDeps.mergeCommit(DepsUnknown, txnId, executeAt, extraDeps, executeAt, i -> i).deps));
                    }
                });
            }
        });
    }

    private void invalidate(SortedListMap<Id, RecoverOk> recoverOks)
    {
        Timestamp invalidateUntil = invalidateUntil(recoverOks);
        proposeInvalidate(node, ballot, txnId, route.homeKey(), (success, fail) -> {
            if (fail != null) accept(null, fail);
            else commitInvalidate(invalidateUntil);
        });
    }

    private Timestamp invalidateUntil(SortedListMap<Id, RecoverOk> recoverOks)
    {
        // If not accepted then the executeAt is not consistent cross the peers and likely different on every node.  There is also an edge case
        // when ranges are removed from the topology, during this case the executeAt won't know the ranges and the invalidate commit will fail.
        return recoverOks.valuesAsNullableStream()
                         .map(ok -> ok == null ? null : ok.status.hasBeen(Status.Accepted) ? ok.executeAt : ok.txnId)
                         .reduce(txnId, Timestamp::nonNullOrMax);
    }

    private void commitInvalidate(Timestamp invalidateUntil)
    {
        node.withEpoch(invalidateUntil.epoch(), node.agent(), () -> {
            Commit.Invalidate.commitInvalidate(node, txnId, route, invalidateUntil);
        });
        isDone = true;
        locallyInvalidateAndCallback(node, txnId, txnId, invalidateUntil, route, ProgressToken.INVALIDATED, callback);
    }

    private void propose(Timestamp executeAt, Deps deps)
    {
        node.withEpoch(executeAt.epoch(), this, () -> {
            adapter.propose(node, null, route, ballot, txnId, txn, executeAt, deps, this);
        });
    }

    private void retry()
    {
        retry(committedExecuteAt);
    }

    private void retry(Timestamp executeAt)
    {
        Topologies topologies = tracker.topologies();
        if (executeAt != null && executeAt.epoch() != (this.committedExecuteAt == null ? txnId : this.committedExecuteAt).epoch())
            topologies = node.topology().select(route, txnId, executeAt, QuorumEpochIntersections.recover);
        Recover.recover(node, topologies, ballot, txnId, txn, route, executeAt, callback);
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        if (tracker.recordFailure(from) == Failed)
            accept(null, new Timeout(txnId, route.homeKey()));
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        accept(null, failure);
        node.agent().onUncaughtException(failure);
    }
}
