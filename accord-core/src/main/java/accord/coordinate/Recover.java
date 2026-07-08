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
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

import accord.api.ExclusiveAsyncExecutor;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.RecoveryTracker;
import accord.local.CommandStores.LatentStoreSelector;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Accept;
import accord.messages.Await;
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
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.topology.TopologyException;
import accord.topology.TopologyMismatch;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.Invariants;
import accord.utils.Rethrowable;
import accord.utils.SortedListMap;
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections;
import static accord.api.ProtocolModifiers.recoverPartialAcceptPhaseIfNoFastPath;
import static accord.coordinate.CoordinationAdapter.Factory.Kind.Recovery;
import static accord.coordinate.ExecutePath.RECOVER;
import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;
import static accord.coordinate.Propose.NotAccept.proposeInvalidate;
import static accord.coordinate.Recover.InferredFastPath.Reject;
import static accord.coordinate.Recover.InferredFastPath.Unknown;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.messages.Accept.Kind.SLOW;
import static accord.messages.Await.Until.CommittedOrNotFastPathCommit;
import static accord.messages.Await.Until.HasDecidedExecuteAt;
import static accord.messages.Await.Until.HasStableDeps;
import static accord.messages.BeginRecovery.RecoverOk.maxAccepted;
import static accord.messages.BeginRecovery.RecoverOk.maxAcceptedNotTruncated;
import static accord.messages.BeginRecovery.RecoveryFlags.FAST_PATH_DECIDED;
import static accord.messages.BeginRecovery.RecoveryFlags.FORCE_RECOVER_FAST_PATH;
import static accord.messages.BeginRecovery.RecoveryFlags.NO_CALCULATE_DEPS;
import static accord.messages.BeginRecovery.RecoveryFlags.calculateDeps;
import static accord.messages.BeginRecovery.RecoveryFlags.forceRecoverFastPath;
import static accord.messages.BeginRecovery.RecoveryFlags.isFastPathDecided;
import static accord.primitives.ProgressToken.TRUNCATED_DURABLE_OR_INVALIDATED;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Status.AcceptedMedium;
import static accord.primitives.Status.AcceptedSlow;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.topology.SelectShards.ALL;
import static accord.utils.Invariants.illegalState;
import static accord.utils.SortedArrays.Search.CEIL;
import static accord.utils.SortedArrays.Search.FLOOR;

// TODO (expected): lifetime of Recovery currently overlaps with follow-up work (and we callback to the Recovery).
//   this is suboptimal - should setDone() and takeCallback() before passing onto next step
public class Recover extends AbstractCoordination<FullRoute<?>, Outcome, RecoverReply, RecoverOk> implements Callback<RecoverReply>
{
    public enum InferredFastPath
    {
        Accept, Unknown, Reject;

        public InferredFastPath merge(InferredFastPath that)
        {
            return compareTo(that) >= 0 ? this : that;
        }
    }

    private final CoordinationAdapter<Result> adapter;
    private final Ballot ballot;
    private final Txn txn;
    private final @Nullable Timestamp committedExecuteAt;
    private final int flags;
    private final LatentStoreSelector reportTo;

    private final RecoveryTracker tracker;

    private Recover(Node node, ExclusiveAsyncExecutor executor, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route,
                    @Nullable Timestamp committedExecuteAt, boolean isFastPathDecided, LatentStoreSelector reportTo,
                    BiConsumer<? super Outcome, Throwable> callback)
    {
        super(node, executor, txnId, route, topologies.nodes(), callback);
        this.flags = computeFlags(txnId, isFastPathDecided, topologies);
        Invariants.require(txnId.isVisible());
        this.adapter = node.coordinationAdapter(txnId, Recovery);
        this.ballot = ballot;
        this.txn = txn;
        this.committedExecuteAt = committedExecuteAt;
        this.reportTo = reportTo;
        this.tracker = new RecoveryTracker(topologies);
    }

    private static int computeFlags(TxnId txnId, boolean isFastPathDecided, Topologies topologies)
    {
        int flags = (isFastPathDecided ? TinyEnumSet.encode(FAST_PATH_DECIDED) : 0)
                    | (!txnId.hasFastPath() && !recoverPartialAcceptPhaseIfNoFastPath() ? TinyEnumSet.encode(NO_CALCULATE_DEPS) : 0);

        for (int i = 0 ; i < topologies.size() ; ++i)
        {
            Topology topology = topologies.get(i);
            if (topology.hardRemovedIds().isEmpty())
                continue;

            for (Shard shard : topology.shards())
            {
                if (shard.hasLostQuorum())
                    return flags | TinyEnumSet.encode(FORCE_RECOVER_FAST_PATH);
            }
        }

        return flags;
    }

    public static void recover(Node node, TxnId txnId, Txn txn, FullRoute<?> route, boolean isFastPathDecided, BiConsumer<? super Outcome, Throwable> callback)
    {
        recover(node, txnId, txn, route, isFastPathDecided, LatentStoreSelector.standard(), callback);
    }

    public static void recover(Node node, TxnId txnId, Txn txn, FullRoute<?> route, boolean isFastPathDecided, LatentStoreSelector reportTo, BiConsumer<? super Outcome, Throwable> callback)
    {
        Ballot ballot = node.uniqueTimestamp(Ballot::fromValues);
        recover(node, ballot, txnId, txn, route, isFastPathDecided, reportTo, callback);
    }

    private static void recover(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, boolean isFastPathDecided, LatentStoreSelector reportTo, BiConsumer<? super Outcome, Throwable> callback)
    {
        recover(node, ballot, txnId, txn, route, null, isFastPathDecided, reportTo, callback);
    }

    public static void recover(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, @Nullable Timestamp committedExecuteAt, boolean isFastPathDecided, LatentStoreSelector reportTo, BiConsumer<? super Outcome, Throwable> callback)
    {
        recover(node, null, ballot, txnId, txn, route, committedExecuteAt, isFastPathDecided, reportTo, callback);
    }

    private static void recover(Node node, @Nullable Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp committedExecuteAt, boolean isFastPathDecided, LatentStoreSelector reportTo, BiConsumer<? super Outcome, Throwable> callback)
    {
        Recover recover;
        try
        {
            if (topologies == null || (committedExecuteAt != null && topologies.currentEpoch() != committedExecuteAt.epoch()))
                topologies = node.topology().active().select(route, txnId, committedExecuteAt == null ? txnId : committedExecuteAt, ALL, QuorumEpochIntersections.recover);
            recover = new Recover(node, node.someExclusiveExecutor(), topologies, ballot, txnId, txn, route, committedExecuteAt, isFastPathDecided, reportTo, callback);
        }
        catch (Throwable t)
        {
            callback.accept(null, t);
            return;
        }
        recover.start();
    }

    @Override
    void start()
    {
        super.start();
        node.agent().coordinatorEvents().onRecoveryStarted(txnId, ballot);
        contact(to -> new BeginRecovery(to, tracker.topologies(), txnId, committedExecuteAt, flags, txn, scope, ballot));
    }

    @Override
    public void onSuccessInternal(Id from, int fromIndex, RecoverReply reply)
    {
        boolean acceptsFastPath;
        switch (reply.kind())
        {
            default: throw new AssertionError("Unhandled RecoverReply.Kind: " + reply.kind());
            case Reject:
            case Truncated:
                // TODO (required): handle partial truncations (both within a shard e.g. pre-bootstrap, and for some shards)
                finishOnFailure(Preempted.preempted(node.agent(), txnId, scope.homeKey()));
                return;

            case Ok:
                RecoverOk ok = (RecoverOk) reply;
                recordOk(fromIndex, ok);
                acceptsFastPath = ok.selfAcceptsFastPath;
                break;

            case Retired:
                acceptsFastPath = true;
        }

        // TODO (expected): don't trigger recovery unless we have Q+1 responses, or 1 or more responses have
        //  no entries in WP where it is itself the coordinator. This means we can always recover immediately.
        if (tracker.recordSuccess(from, acceptsFastPath) == Success)
            recover();
    }

    BiConsumer<? super Outcome, Throwable> finishAndTakeCallback()
    {
        return finishAndWrapCallback();
    }

    private BiConsumer<? super Outcome, Throwable> finishAndWrapCallback()
    {
        BiConsumer<? super Outcome, Throwable> callback = super.finishAndTakeCallback();
        return (success, failure) -> {
            callback.accept(success, failure);
            node.agent().coordinatorEvents().onRecoveryStopped(node, txnId, ballot, null, failure);
        };
    }

    private BiConsumer<? super Outcome, Throwable> finishAndUnwrapCallback()
    {
        return super.finishAndTakeCallback();
    }

    private BiConsumer<Result, Throwable> finishAndTakeResultCallback()
    {
        BiConsumer<? super Outcome, Throwable> callback = finishAndUnwrapCallback();
        return (success, failure) -> {
            if (failure == null)
            {
                callback.accept(ProgressToken.APPLIED, null);
                node.agent().coordinatorEvents().onRecoveryStopped(node, txnId, ballot, success, failure);
            }
            else if (failure instanceof Redundant)
            {
                Timestamp committedExecuteAt = ((Redundant) failure).committedExecuteAt;
                if (tracing != null)
                    tracing.trace(null, "found Redundant; retrying with known committedExecuteAt " + committedExecuteAt);
                retry(committedExecuteAt, callback);
            }
            else
            {
                callback.accept(null, Rethrowable.rethrowable(failure));
                node.agent().coordinatorEvents().onRecoveryStopped(node, txnId, ballot, success, failure);
            }
        };
    }

    private void recover()
    {
        SortedListMap<Id, RecoverOk> oks = finishOks();

        List<RecoverOk> okList = oks.valuesAsNullableList();
        RecoverOk acceptOrCommit = maxAccepted(okList);
        RecoverOk acceptOrCommitNotTruncated = acceptOrCommit == null || acceptOrCommit.status != Status.Truncated
                                               ? acceptOrCommit : maxAcceptedNotTruncated(okList);

        if (acceptOrCommitNotTruncated != null)
        {
            Timestamp executeAt = acceptOrCommitNotTruncated.executeAt;
            Status status; {
            Status tmp = acceptOrCommitNotTruncated.status;
            if (committedExecuteAt != null)
            {
                Invariants.require(acceptOrCommitNotTruncated.status.compareTo(Status.PreCommitted) < 0 || executeAt.equals(committedExecuteAt));
                // if we know from a prior Accept attempt that this is committed we can go straight to the commit phase
                if (tmp == AcceptedMedium || tmp == AcceptedSlow)
                    tmp = Status.Committed;
            }
            status = tmp;
        }

            switch (status)
            {
                case Truncated: throw illegalState("Truncate should be filtered");
                case Invalidated:
                {
                    if (tracing != null)
                        tracing.trace(null, "found Invalidated: committing to all shards.");

                    commitInvalidate(invalidateUntil(oks), finishAndTakeCallback());
                    return;
                }

                case AcceptedInvalidate:
                {
                    if (tracing != null)
                        tracing.trace(null, "found AcceptedInvalidate: continuing Invalidate.");

                    finishAndInvalidate(oks);
                    return;
                }

                case NotDefined:
                case PreAccepted:
                    throw illegalState("Should only be possible to have Accepted or later commands");
            }

            LatestDeps.Merge merge = mergeDeps(okList);
            Participants<?> await = merge.notAccepted(scope);

            if (!calculateDeps(flags))
            {
                Invariants.require(!txnId.hasFastPath());
                switch (status)
                {
                    case AcceptedMedium:
                    case AcceptedSlow:
                        if (!await.isEmpty())
                        {
                            finishAndInvalidate(oks);
                            return;
                        }
                }
            }

            awaitPartialSimple(okList, await, () -> {
                BiConsumer<Result, Throwable> callback = finishAndTakeResultCallback();
                switch (status)
                {
                    default: throw new UnhandledEnum(status);
                    case Applied:
                    case PreApplied:
                    {
                        if (tracing != null)
                            tracing.trace(null, "found Applied; persisting.");

                        withStableDeps(merge, executeAt, callback, stableDeps -> {
                            adapter.persist(node, executor, tracker.topologies(), scope, ballot, CoordinationFlags.none(), txnId, txn, executeAt, stableDeps, acceptOrCommitNotTruncated.writes, acceptOrCommitNotTruncated.result, callback);
                        });
                        return;
                    }

                    case Stable:
                    {
                        if (tracing != null)
                            tracing.trace(null, "found Stable; executing.");

                        withStableDeps(merge, executeAt, callback, stableDeps -> {
                            adapter.execute(node, executor, tracker.topologies(), scope, ballot, RECOVER, CoordinationFlags.none(), txnId, txn, executeAt, stableDeps, stableDeps, callback);
                        });
                        return;
                    }

                    case PreCommitted:
                    case Committed:
                    {
                        if (tracing != null)
                            tracing.trace(null, "found Committed; stabilising.");

                        withCommittedDeps(merge, executeAt, callback, committedDeps -> {
                            adapter.stabilise(node, executor, tracker.topologies(), scope, ballot, txnId, txn, executeAt, committedDeps, callback);
                        });
                        return;
                    }

                    case AcceptedSlow:
                    case AcceptedMedium:
                    {
                        if (tracing != null)
                            tracing.trace(null, "found Accepted; re-proposing.");

                        // TODO (desired): if we have a quorum of Accept with matching ballot or proposal we can go straight to Commit
                        // TODO (desired): if we didn't find Accepted in *every* shard, consider invalidating for consistency of behaviour
                        //     however, note that we may have taken the fast path and recovered, so we can only do this if acceptedOrCommitted=Ballot.ZERO
                        //     (otherwise recovery was attempted and did not invalidate, so it must have determined it needed to complete)
                        propose(SLOW, acceptOrCommitNotTruncated.executeAt, merge, callback);
                    }
                }
            });
            return;
        }

        if (acceptOrCommit != null && acceptOrCommit != acceptOrCommitNotTruncated)
        {
            // TODO (required): match logic in Invalidate; we need to know which keys have been invalidated
            Topologies topologies = tracker.topologies();
            boolean allShardsTruncated = true;
            for (int topologyIndex = 0 ; topologyIndex < topologies.size() ; ++topologyIndex)
            {
                Topology topology = topologies.get(topologyIndex);
                for (Shard shard : topology.shards())
                {
                    RecoverOk maxReply = maxAccepted(oks.lazySelect(shard.nodes));
                    allShardsTruncated &= maxReply.status == Status.Truncated;
                }
                if (allShardsTruncated)
                {
                    if (tracing != null)
                        tracing.trace(null, "found all shards truncated; terminating.");
                    // TODO (required, correctness): this is not a safe inference in the case of an ErasedOrInvalidOrVestigial response.
                    //   We need to tighten up the inference and spread of truncation/invalid outcomes.
                    //   In this case, at minimum this can lead to liveness violations as the home shard stops coordinating
                    //   a command that it hasn't invalidated, but nor is it possible to recover. This happens because
                    //   when the home shard shares all of its replicas with another shard that has autonomously invalidated
                    //   the transaction, so that all received InvalidateReply show truncation (when in fact this is only partial).
                    //   We could paper over this, but better to revisit and provide stronger invariants we can rely on.
                    finishWithSuccess(TRUNCATED_DURABLE_OR_INVALIDATED);
                    return;
                }
            }
        }

        Invariants.require(committedExecuteAt == null || committedExecuteAt.equals(txnId));
        Invariants.require(!isFastPathDecided(flags) || forceRecoverFastPath(flags));

        boolean coordinatorInRecoveryQuorum = oks.get(txnId.node) != null;
        Participants<?> extraCoordVotes = extraCoordinatorVotes(txnId, coordinatorInRecoveryQuorum, okList);
        Participants<?> extraRejects = Deps.merge(okList, okList.size(), List::get, ok -> ok.supersedingCoordRejects)
                                           .intersecting(scope, id -> !oks.containsKey(id.node));
        InferredFastPath fastPath;
        if (txnId.hasPrivilegedCoordinator() && coordinatorInRecoveryQuorum) fastPath = Reject;
        else if (txnId.isSyncPoint()) fastPath = Reject;
        else fastPath = merge(
                supersedingRejects(okList) ? Reject : Unknown,
                tracker.inferFastPathDecision(txnId, extraCoordVotes, extraRejects)
            );

        switch (fastPath)
        {
            case Reject:
            {
                if (tracing != null)
                    tracing.trace(null, "found fast path rejection; invoking Invalidate.");

                finishAndInvalidate(oks);
                return;
            }
            case Accept:
            {
                // we still have to wait for earlier transactions to decide themselves so we don't accidentally include
                // a non-fastpath transaction in our dependencies and permit it to conclude it is safe to execute.
                // So, we fall-through to Unknown condition - though we don't in principle need to wait for any future transactions
            }
            case Unknown:
            {
                // should all be PreAccept
                Deps simpleWait = Deps.merge(okList, okList.size(), List::get, ok -> ok.simpleWait);
                Deps simpleNoWait = Deps.merge(okList, okList.size(), List::get, ok -> ok.simpleNoWait);
                simpleWait = simpleWait.without(simpleNoWait);
                Deps laterWitnessedCoordRejects = Deps.merge(oks, oks.domainSize(), (map, i) -> selectCoordinatorReplies(map.getKey(i), map.getValue(i)), Function.identity());

                if (!simpleWait.isEmpty() || !laterWitnessedCoordRejects.isEmpty())
                {
                    // If there exist commands that were proposed a later execution time than us that have not witnessed us,
                    // we have to be certain these commands have not successfully committed without witnessing us (thereby
                    // ruling out a fast path decision for us and changing our recovery decision).
                    // So, we wait for these commands to commit and recompute supersedingRejects for them.
                    // TODO (required): we diverge from the paper by preferring phase over ballot, so that we do not move phase backwards;
                    //  Fedor points out we should therefore wait for Stable rather than Committed deps here (TBC by Lean)
                    awaitToFinish(AsyncChains.reduce(awaitSimple(node, simpleWait, HasStableDeps),
                                                     awaitSupersedingCoord(node, laterWitnessedCoordRejects, CommittedOrNotFastPathCommit, extraCoordVotes),
                                                     InferredFastPath::merge)
                                             .invokeIfSuccess((inferred) -> {
                                                 switch (inferred)
                                                 {
                                                     default: throw new UnhandledEnum(inferred);
                                                     case Accept:
                                                     {
                                                         if (tracing != null)
                                                             tracing.trace(null, "found accepted fast path; proposing.");
                                                         propose(SLOW, txnId, okList);
                                                         break;
                                                     }
                                                     case Unknown:
                                                     {
                                                         if (tracing != null)
                                                             tracing.trace(null, "found unknown fast path decision; retrying.");
                                                         retry(committedExecuteAt, finishAndUnwrapCallback());
                                                         break;
                                                     }
                                                     case Reject:
                                                     {
                                                         if (tracing != null)
                                                             tracing.trace(null, "found fast path rejection; invoking Invalidate.");

                                                         finishAndInvalidate(oks);
                                                         break;
                                                     }
                                                 }
                                             }));
                }
                else
                {
                    if (tracing != null)
                        tracing.trace(null, "found unknown fast path decision, but no preceding or superseding transactions awaiting decisions; proposing.");
                    propose(SLOW, txnId, okList);
                }
            }
        }
    }

    private static LatestDeps.Merge mergeDeps(List<RecoverOk> nullableRecoverOkList)
    {
        return LatestDeps.merge(nullableRecoverOkList, ok -> ok == null ? null : ok.deps);
    }

    private void awaitPartialSimple(List<RecoverOk> nullableRecoverOkList, Participants<?> participants, Runnable whenReady)
    {
        Deps earlierWait = Deps.merge(nullableRecoverOkList, nullableRecoverOkList.size(), List::get, ok -> ok.simpleWait);
        Deps earlierNoWait = Deps.merge(nullableRecoverOkList, nullableRecoverOkList.size(), List::get, ok -> ok.simpleNoWait);
        earlierWait = earlierWait.without(earlierNoWait);
        earlierWait = earlierWait.intersecting(participants);
        awaitToFinish(awaitSimple(node, earlierWait, HasDecidedExecuteAt).invokeIfSuccess(whenReady));
    }

    private static boolean supersedingRejects(List<RecoverOk> oks)
    {
        for (RecoverOk ok : oks)
        {
            if (ok != null && ok.supersedingRejects)
                return true;
        }
        return false;
    }

    private static InferredFastPath merge(InferredFastPath a, InferredFastPath b)
    {
        if (a == Unknown || b == Unknown)
            return a == Unknown ? b : a;

        // we CAN encounter both Reject AND Accept in the event we are a stale recovery attempt, and a "later" recovery
        // has already invalidated us, due to witnessing a different quorum
        // (e.g. witnessing the privileged coordinator so knew we did not take fast path, even if we could have done).
        // So, we just take the Reject unless both are Accept
        return a == b ? a : Reject;
    }

    private static Participants<?> extraCoordinatorVotes(TxnId txnId, boolean coordinatorInQuorum, List<RecoverOk> oks)
    {
        if (!txnId.hasPrivilegedCoordinator())
            return null;

        Participants<?> result = Participants.empty(txnId);
        if (coordinatorInQuorum)
            return result;

        for (RecoverOk ok : oks)
        {
            if (ok != null && ok.coordinatorAcceptsFastPath != null)
                result = Participants.merge(result, (Participants) ok.coordinatorAcceptsFastPath);
        }
        return result;
    }

    private static Deps selectCoordinatorReplies(Id from, RecoverOk ok)
    {
        if (ok == null)
            return null;

        return ok.supersedingCoordRejects.with(id -> from.equals(id.node));
    }

    private void withCommittedDeps(LatestDeps.Merge merge, Timestamp executeAt, BiConsumer<?, Throwable> failureCallback, Consumer<Deps> withDeps)
    {
        LatestDeps.withCommitted(adapter, node, executor, merge, scope, ballot, txnId, executeAt, txn, failureCallback, withDeps);
    }

    private void withStableDeps(LatestDeps.Merge merge, Timestamp executeAt, BiConsumer<?, Throwable> failureCallback, Consumer<Deps> withDeps)
    {
        LatestDeps.withStable(adapter, node, executor, merge, Deps.NONE, scope, null, null, scope, ballot, txnId, executeAt, txn, failureCallback, withDeps);
    }

    private void finishAndInvalidate(SortedListMap<Id, RecoverOk> recoverOks)
    {
        invalidate(recoverOks, finishAndTakeCallback());
    }

    private void invalidate(SortedListMap<Id, RecoverOk> recoverOks, BiConsumer<? super Outcome, Throwable> callback)
    {
        Timestamp invalidateUntil = invalidateUntil(recoverOks);
        proposeInvalidate(node, executor, ballot, txnId, scope.homeKey(), (success, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else commitInvalidate(invalidateUntil, callback);
        });
    }

    private Timestamp invalidateUntil(SortedListMap<Id, RecoverOk> recoverOks)
    {
        // If not accepted then the executeAt is not consistent cross the peers and likely different on every node.  There is also an edge case
        // when ranges are removed from the topology, during this case the executeAt won't know the ranges and the invalidate commit will fail.
        return recoverOks.valuesAsNullableStream()
                         .map(ok -> ok == null ? null : ok.status.hasBeen(AcceptedMedium) ? ok.executeAt : ok.txnId)
                         .reduce(txnId, Timestamp::nonNullOrMax);
    }

    private void commitInvalidate(Timestamp invalidateUntil, BiConsumer<? super Outcome, Throwable> callback)
    {
        locallyInvalidateAndCallback(node, txnId, reportTo.refine(txnId, null, scope), scope, ProgressToken.INVALIDATED, callback, tracing);
        node.withEpochAtLeast(invalidateUntil.epoch(), executor, node.agent(), () -> {
            Commit.Invalidate.commitInvalidate(node, txnId, scope, invalidateUntil, tracing);
        });
    }

    private void propose(Accept.Kind kind, Timestamp executeAt, List<RecoverOk> recoverOkList)
    {
        LatestDeps.Merge mergeDeps = mergeDeps(recoverOkList);
        propose(kind, executeAt, mergeDeps, finishAndTakeResultCallback());
    }

    private void propose(Accept.Kind kind, Timestamp executeAt, LatestDeps.Merge merge, BiConsumer<Result, Throwable> callback)
    {
        Deps proposeDeps;
        try { proposeDeps = merge.mergeProposal(); }
        catch (Throwable t)
        {
            callback.accept(null, t);
            return;
        }
        node.withEpochAtLeast(executeAt.epoch(), executor, callback, () -> {
            adapter.propose(node, executor, null, scope, kind, ballot, txnId, txn, executeAt, proposeDeps, callback);
        });
    }

    private void retry(Timestamp executeAt, BiConsumer<? super Outcome, Throwable> callback)
    {
        Ballot ballot = node.uniqueTimestamp(Ballot::fromValues);
        Recover.recover(node, tracker.topologies(), ballot, txnId, txn, scope, executeAt, isFastPathDecided(flags), reportTo, callback);
    }

    AsyncChain<InferredFastPath> awaitSimple(Node node, Deps waitOn, Await.Until awaitUntil)
    {
        if (tracing != null)
            tracing.trace(null, "awaiting decisions: " + waitOn.txnIds());

        long requireEpoch = waitOn.maxTxnId(txnId).epoch();
        return node.withEpochAtLeast(requireEpoch, executor, () -> {
            TxnId recoverId = this.txnId;
            List<AsyncChain<InferredFastPath>> requests = new ArrayList<>();
            for (int i = 0 ; i < waitOn.txnIdCount() ; ++i)
            {
                TxnId awaitId = waitOn.txnId(i);
                Invariants.require(awaitId.compareTo(recoverId) < 0 || !awaitId.hasFastPath());
                Participants<?> participants = waitOn.participants(awaitId);

                Topologies topologies;
                if (tracker.topologies().containsEpoch(awaitId.epoch())) topologies = tracker.topologies().selectIfExists(participants, awaitId.epoch());
                else
                {
                    try { topologies = node.topology().active().forEpochAtLeast(participants, awaitId.epoch(), ALL); }
                    catch (TopologyMismatch e) { throw new RuntimeException(e); }
                }
                requests.add(SynchronousRecoverAwait.awaitAny(node, executor, topologies, awaitId, awaitUntil, true, participants, recoverId));
            }
            if (requests.isEmpty())
                return AsyncChains.success(InferredFastPath.Accept);
            return AsyncChains.reduce(requests, InferredFastPath::merge);
        });
    }

    AsyncChain<InferredFastPath> awaitSupersedingCoord(Node node, Deps waitOn, Await.Until awaitUntil, @Nullable Participants<?> selfCoordVotes)
    {
        if (tracing != null)
            tracing.trace(null, "awaiting later decisions or recoveries: " + waitOn.txnIds());

        if (waitOn.isEmpty())
            return AsyncChains.success(InferredFastPath.Accept);

        Participants<?> reliesOnAwaitIdCoordVote;
        Topology topology = tracker.topologies().current();
        switch (scope.domain())
        {
            default: throw new UnhandledEnum(scope.domain());
            case Key:
                try (BufferList<RoutingKey> tmp = new BufferList<>())
                {
                    for (int j = 0; j < scope.size() ; ++j)
                    {
                        RoutingKey key = (RoutingKey) scope.get(j);
                        RecoveryTracker.RecoveryShardTracker shardTracker = tracker.get(0, topology.indexForKey(key));
                        if (shardTracker.fastPathReliesOnUnwitnessedCoordinatorVote(txnId, selfCoordVotes))
                            tmp.add(key);
                    }
                    reliesOnAwaitIdCoordVote = RoutingKeys.ofSortedUnique(tmp);
                }
                break;
            case Range:
                try (BufferList<Range> tmp = new BufferList<>())
                {
                    for (int j = 0; j < scope.size() ; ++j)
                    {
                        Range range = (Range) scope.get(j);
                        for (int k = topology.indexForRange(range, CEIL), maxk = topology.indexForRange(range, FLOOR); k <= maxk ; k++)
                        {
                            RecoveryTracker.RecoveryShardTracker shardTracker = tracker.get(0, k);
                            if (shardTracker.fastPathReliesOnUnwitnessedCoordinatorVote(txnId, selfCoordVotes))
                                tmp.add(range.slice(shardTracker.shard.range));
                        }
                    }
                    reliesOnAwaitIdCoordVote = Ranges.ofSortedAndDeoverlapped(tmp.toArray(Range[]::new));
                }
        }

        if (reliesOnAwaitIdCoordVote.isEmpty())
            return AsyncChains.success(InferredFastPath.Accept);

        long requireEpoch = waitOn.maxTxnId(txnId).epoch();
        return node.withEpochExact(requireEpoch, executor, () -> {
            TxnId recoverId = this.txnId;
            List<AsyncChain<InferredFastPath>> requests = new ArrayList<>();
            for (int i = 0 ; i < waitOn.txnIdCount() ; ++i)
            {
                TxnId awaitId = waitOn.txnId(i);
                Invariants.require(awaitId.is(PrivilegedCoordinatorWithDeps));
                Invariants.require(awaitId.compareTo(recoverId) > 0);
                Participants<?> participants = waitOn.participants(awaitId)
                                                     .intersecting(reliesOnAwaitIdCoordVote, Minimal);
                if (participants.isEmpty())
                    continue;

                Topologies topologies;
                if (tracker.topologies().containsEpoch(awaitId.epoch())) topologies = tracker.topologies().selectIfExists(participants, awaitId.epoch());
                else
                {
                    try { topologies = node.topology().active().forEpoch(participants, awaitId.epoch(), ALL); }
                    catch (TopologyException t)
                    {
                        // this is a future transaction, and we cannot find its topology despite waiting for the relevant epoch;
                        // most likely the transaction we are recovering is already resolved
                        return AsyncChains.failure(t);
                    }
                }
                requests.add(SynchronousRecoverAwait.awaitAny(node, executor, topologies, awaitId, awaitUntil, true, participants, recoverId));
            }
            if (requests.isEmpty())
                return AsyncChains.success(InferredFastPath.Accept);
            return AsyncChains.reduce(requests, InferredFastPath::merge);
        });
    }

    @Override
    public void onFailureInternal(Id from, int fromIndex, Throwable failure)
    {
        recordFailure(failure);
        if (tracker.recordFailure(from) == Failed)
            finishOnFailure();
    }

    public CoordinationKind kind()
    {
        return CoordinationKind.BeginRecovery;
    }

    @Override
    public Ballot ballot()
    {
        return ballot;
    }

    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }

    @Override
    public String describe()
    {
        return "ballot=" + ballot + ", flags=" + Integer.toHexString(flags) + ", committedExecuteAt=" + committedExecuteAt;
    }
}
