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

import java.util.List;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import accord.api.ProtocolModifiers;
import accord.api.Result;
import accord.coordinate.CoordinationAdapter.Adapters;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Commands;
import accord.local.DepsCalculator;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.api.ExclusiveAsyncExecutor;
import accord.local.StoreParticipants;
import accord.messages.PreAccept.PreAcceptNack;
import accord.messages.PreAccept.PreAcceptReply;
import accord.topology.Topologies;
import accord.local.Node;
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.SortedListMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import static accord.coordinate.CoordinationAdapter.Factory.Kind.Standard;
import static accord.coordinate.ExecuteFlag.CoordinationFlags.empty;
import static accord.coordinate.ExecutePath.FAST;
import static accord.local.Commands.AcceptOutcome.Success;
import static accord.messages.Accept.Kind.MEDIUM;
import static accord.messages.Accept.Kind.SLOW;
import static accord.messages.MessageType.StandardMessage.PRE_ACCEPT_REQ;
import static accord.primitives.Timestamp.Flag.REJECTED;
import static accord.primitives.Timestamp.mergeMaxAndFlags;
import static accord.primitives.Timestamp.Flag.SOFT_REJECT;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.topology.SelectShards.LIVE;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class CoordinateTransaction extends CoordinatePreAccept<Result>
{
    private CoordinateTransaction(Node node, ExclusiveAsyncExecutor executor, Topologies topologies, FullRoute<?> route, TxnId txnId, Txn txn, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, executor, topologies, route, txnId, txn, callback);
    }

    public static AsyncChain<Result> coordinate(Node node, TxnId txnId, Txn txn)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            public @Nullable Cancellable start(BiConsumer<? super Result, Throwable> callback)
            {
                coordinate(node, txnId, txn, callback);
                return null;
            }
        };
    }

    public static void coordinate(Node node, TxnId txnId, Txn txn, BiConsumer<? super Result, Throwable> callback)
    {
        CoordinateTransaction coordinate;
        try
        {
            FullRoute<?> route = node.computeRoute(txnId, txn.keys());
            Topologies topologies = node.topology().active().select(route, txnId, txnId, LIVE, ProtocolModifiers.QuorumEpochIntersections.preaccept.include);
            coordinate = new CoordinateTransaction(node, node.someExclusiveExecutor(), topologies, route, txnId, txn, callback);
        }
        catch (Throwable t)
        {
            callback.accept(null, t);
            return;
        }
        coordinate.start();
    }

    @Override
    void start()
    {
        super.start();
        if (txnId != null && txnId.hasPrivilegedCoordinator())
            new LocalExecute().start();
        else
            contact(null, false);
    }

    @Override
    void onPreAccepted(Topologies topologies, SortedListMap<Node.Id, PreAcceptOk> oks)
    {
        Timestamp executeAt = oks.foldlNonNullValues((ok, prev) -> mergeMaxAndFlags(ok.witnessedAt, prev), Timestamp.NONE);
        if (executeAt.is(REJECTED) && !(topologies.size() == 1 && (tracker.hasFastPathAccepted() || tracker.hasMediumPathAccepted())))
        {
            // we special case having fast or medium path accepted with only the latest topology because this is compatible with
            // the behaviour on PreAccept that does not calculate Deps if we are a voting replica and mark REJECTED;
            // that is, if we have some earlier topology where the lack of deps would be an invalid quorum vote, we must reject the transaction
            // otherwise, if we have somehow reached a medium or fast path decision this vote can be safely ignored
            finishWithFailure(Rejected.rejected(node.agent(), txnId, scope.homeKey()));
            return;
        }

        if (executeAt.is(SOFT_REJECT))
        {
            // count soft rejects, and hard reject if too many
            int count = (int) oks.foldlNonNullValues((ok, v) -> ok.witnessedAt.is(SOFT_REJECT) ? v + 1 : v, 0L);
            // TODO (expected): calculate this per shard?
            if (node.agent().hardReject(count, oks.size()))
            {
                finishWithFailure(Rejected.rejected(node.agent(), txnId, scope.homeKey()));
                return;
            }
            executeAt = executeAt.removeFlag(SOFT_REJECT);
        }

        if (tracker.hasFastPathAccepted())
        {
            Deps deps = mergeFastOrMediumDeps(oks);
            if (deps != null)
            {
                CoordinationFlags flags = oks.foldlNonNull((d, k, v, out) -> {
                    ExecuteFlags.collect(out, k, v.flags, d, v.deps);
                    return out;
                }, deps, empty(oks.domain()));

                // note: we merge all Deps regardless of witnessedAt. While we only need fast path votes,
                // we must include Deps from fast path votes from earlier epochs that may have witnessed later transactions
                // TODO (desired): we might mask some bugs by merging more responses than we strictly need, so optimise this to optionally merge minimal deps
                node.agent().coordinatorEvents().onPreAccepted(txnId);
                executeAdapter().execute(node, executor, topologies, scope, Ballot.ZERO, FAST, flags, txnId, txn, txnId, deps, deps, finishAndTakeCallback());
                return;
            }
        }
        else if (tracker.hasMediumPathAccepted() && txnId.hasMediumPath())
        {
            Deps deps = mergeFastOrMediumDeps(oks);
            if (deps != null)
            {
                node.agent().coordinatorEvents().onPreAccepted(txnId);
                proposeAdapter().propose(node, executor, topologies, scope, MEDIUM, Ballot.ZERO, txnId, txn, txnId, deps, finishAndTakeCallback());
                return;
            }
        }

        Deps deps = Deps.merge(oks.valuesAsNullableList(), oks.domainSize(), List::get, ok -> ok.deps);
        node.agent().coordinatorEvents().onPreAccepted(txnId);
        proposeAdapter().propose(node, executor, topologies, scope, SLOW, Ballot.ZERO, txnId, txn, executeAt, deps, finishAndTakeCallback());
    }

    private Deps mergeFastOrMediumDeps(SortedListMap<?, PreAcceptOk> oks)
    {
        // we must merge all Deps replies from prior topologies, but from the latest topology we can safely merge only those replies that voted for the fast path
        // TODO (desired): actually merge these topologies separately, rather than just switching behaviour when multiple topologies
        if (tracker.topologies().size() == 1)
            return Deps.merge(oks.valuesAsNullableList(), oks.domainSize(), List::get, ok -> ok.witnessedAt.equals(ok.txnId) ? ok.deps : null);

        Deps deps = Deps.merge(oks.valuesAsNullableList(), oks.domainSize(), List::get, ok -> ok.deps);
        // it is possible that one of the earlier epochs that did not need to vote for the fast path
        // was also unable to compute valid dependencies, and returned a future TxnId as a proxy.
        // In this case while it is still in principle safe to propose the fast path, it is simpler not to,
        // as it permits us to maintain safety validation logic that detects unsafe behaviour and execution will
        // need to wait for the future transaction to be agreed anyway (so we can use its dependency calculation).
        if (deps.maxTxnId(txnId).compareTo(txnId) > 0)
            return null;

        return deps;
    }

    protected CoordinationAdapter<Result> proposeAdapter()
    {
        return Adapters.standard();
    }

    protected CoordinationAdapter<Result> executeAdapter()
    {
        return node.coordinationAdapter(txnId, Standard);
    }

    class LocalExecute extends AbstractLocalExecute
    {
        @Override
        long expiresAt()
        {
            return node.agent().selfExpiresAt(txnId, PRE_ACCEPT_REQ, MICROSECONDS);
        }

        @Override
        Cancellable submit()
        {
            return node.commandStores().mapReduceConsume(topologies.oldestEpoch(), topologies.currentEpoch(), this);
        }

        @Override
        public void acceptInternal(PreAcceptReply result, Throwable failure)
        {
            if (failure != null)
            {
                // we must be able to continue in the face of a local failure esp. in case of LogUnavailableException
                if (txnId.hasPrivilegedCoordinator())
                    fastPathEnabled = false;
                contactNotSelf(null, false);
                onFailure(node.id(), failure);
            }
            else
            {
                if (result.isOk())
                {
                    PreAcceptOk ok = (PreAcceptOk) result;
                    // TODO (desired): we can probably still process and record fast path votes from peers, just with different quorum requirements
                    boolean hasCoordinatorVote = txnId.equals(ok.witnessedAt);
                    if (!hasCoordinatorVote && txnId.hasPrivilegedCoordinator()) fastPathEnabled = false;
                    Deps deps = hasCoordinatorVote && txnId.is(PrivilegedCoordinatorWithDeps) ? ok.deps : null;
                    contactNotSelf(deps, hasCoordinatorVote);
                    onSuccess(node.id(), ok);
                }
                else
                {
                    finishOnFailure(Preempted.preempted(node.agent(), txnId, scope.homeKey()));
                }
            }
        }

        @Override
        public PreAcceptReply applyInternal(SafeCommandStore safeStore)
        {
            long minEpoch = topologies.oldestEpoch();
            StoreParticipants participants = StoreParticipants.update(safeStore, scope, minEpoch, txnId, txnId.epoch());
            SafeCommand safeCommand = safeStore.get(txnId, participants);

            Timestamp executeAt;
            Deps deps;
            ExecuteFlags flags;
            try (DepsCalculator calculator = new DepsCalculator(txnId))
            {
                deps = calculator.calculate(safeStore, txnId, participants, minEpoch, txnId, true);
                if (deps == null)
                    return PreAcceptNack.INSTANCE;

                boolean hasCoordinatorVote = txnId.hasPrivilegedCoordinator();
                Deps coordinatorDeps = txnId.is(PrivilegedCoordinatorWithDeps) ? deps : null;
                Commands.AcceptOutcome outcome = Commands.preaccept(safeStore, safeCommand, participants, txnId, txn, coordinatorDeps, hasCoordinatorVote);
                if (outcome != Success)
                    return PreAcceptNack.INSTANCE;

                executeAt = calculator.executeAt(safeCommand, node);
                flags = calculator.executeFlags(txnId);
            }

            return new PreAcceptOk(txnId, executeAt, deps, flags);
        }

        @Override
        public PreAcceptReply reduce(PreAcceptReply r1, PreAcceptReply r2)
        {
            return PreAcceptReply.reduce(r1, r2);
        }

        @Override
        public LoadKeys loadKeys()
        {
            return LoadKeys.SYNC;
        }

        @Override
        public LoadKeysFor loadKeysFor()
        {
            return LoadKeysFor.READ_WRITE;
        }

        public ExecutionKind executionKind()
        {
            return ExecutionKind.PREACCEPT;
        }
    }
}
