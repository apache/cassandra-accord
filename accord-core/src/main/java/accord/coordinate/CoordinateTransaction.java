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

import accord.api.Result;
import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.coordinate.CoordinationAdapter.Adapters;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Command;
import accord.local.Commands;
import accord.local.DepsCalculator;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.MapReduceConsumeCommandStores;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SequentialAsyncExecutor;
import accord.local.StoreParticipants;
import accord.messages.PreAccept.PreAcceptNack;
import accord.messages.PreAccept.PreAcceptReply;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Status;
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

import static accord.coordinate.CoordinateTransaction.LocalExecuteState.PENDING;
import static accord.coordinate.CoordinateTransaction.LocalExecuteState.SUCCESS;
import static accord.coordinate.CoordinateTransaction.LocalExecuteState.TIMEOUT;
import static accord.coordinate.CoordinationAdapter.Factory.Kind.Standard;
import static accord.coordinate.ExecuteFlag.CoordinationFlags.empty;
import static accord.coordinate.ExecutePath.FAST;
import static accord.coordinate.Propose.NotAccept.proposeAndCommitInvalidate;
import static accord.local.Commands.AcceptOutcome.Success;
import static accord.messages.Accept.Kind.MEDIUM;
import static accord.messages.Accept.Kind.SLOW;
import static accord.primitives.Timestamp.Flag.REJECTED;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class CoordinateTransaction extends CoordinatePreAccept<Result>
{
    private CoordinateTransaction(Node node, SequentialAsyncExecutor executor, TxnId txnId, Txn txn, FullRoute<?> route, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, executor, txnId, txn, route, callback);
    }

    public static AsyncChain<Result> coordinate(Node node, FullRoute<?> route, TxnId txnId, Txn txn)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            public @Nullable Cancellable start(BiConsumer<? super Result, Throwable> callback)
            {
                coordinate(node, route, txnId, txn, callback);
                return null;
            }
        };
    }

    public static void coordinate(Node node, FullRoute<?> route, TxnId txnId, Txn txn, BiConsumer<? super Result, Throwable> callback)
    {
        TopologyMismatch mismatch = TopologyMismatch.checkForMismatchOrPendingRemoval(node.topology().globalForEpoch(txnId.epoch()), txnId, route.homeKey(), txn.keys());
        if (mismatch != null)
        {
            callback.accept(null, mismatch);
            return;
        }

        CoordinateTransaction coordinate;
        try
        {
            coordinate = new CoordinateTransaction(node, node.someSequentialExecutor(), txnId, txn, route, callback);
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
    void onPreAccepted(Topologies topologies, Timestamp executeAt, SortedListMap<Node.Id, PreAcceptOk> oks)
    {
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
        else if (executeAt.is(REJECTED))
        {
            proposeAndCommitInvalidate(node, executor, Ballot.ZERO, txnId, scope.homeKey(), scope, executeAt, finishAndTakeCallback());
            node.agent().coordinatorEvents().onRejected(txnId);
            return;
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

    enum LocalExecuteState { PENDING, SUCCESS, TIMEOUT}
    class LocalExecute extends MapReduceConsumeCommandStores<Route<?>, PreAcceptReply> implements Timeouts.Timeout
    {
        LocalExecuteState state = LocalExecuteState.PENDING;
        Cancellable cancel;
        RegisteredTimeout timeout;

        protected LocalExecute()
        {
            super(CoordinateTransaction.this.scope);
        }

        void start()
        {
            markSelfContacted();
            Cancellable cancel = node.commandStores().mapReduceConsume(topologies.oldestEpoch(), topologies.currentEpoch(), this);
            long expiresAt = node.agent().selfExpiresAt(txnId, Status.Phase.PreAccept, MICROSECONDS);
            RegisteredTimeout timeout = expiresAt <= 0 ? null : node.timeouts().registerAt(this, expiresAt, MICROSECONDS);
            synchronized (this)
            {
                switch (state)
                {
                    case PENDING:
                        this.cancel = cancel;
                        this.timeout = timeout;
                        break;
                    case TIMEOUT:
                        if (cancel != null)
                            cancel.cancel();
                        break;
                    case SUCCESS:
                        if (timeout != null)
                            timeout.cancel();
                        break;
                }
            }
        }

        @Override
        public void accept(PreAcceptReply result, Throwable failure)
        {
            success();
            executor.executeMaybeImmediately(() -> {
                if (failure != null)
                {
                    finishWithFailureOverride(failure);
                }
                else
                {
                    if (result.isOk())
                    {
                        PreAcceptOk ok = (PreAcceptOk) result;
                        // TODO (desired): we can probably still process and record fast path votes from peers, just with different quorum requirements
                        boolean hasCoordinatorVote = txnId.equals(ok.witnessedAt);
                        if (!hasCoordinatorVote) fastPathEnabled = false;
                        Deps deps = hasCoordinatorVote && txnId.is(PrivilegedCoordinatorWithDeps) ? ok.deps : null;
                        contactNotSelf(deps, hasCoordinatorVote);
                        onSuccess(node.id(), ok);
                    }
                    else
                    {
                        finishWithFailureOverride(Preempted.preempted(node.agent(), txnId, scope.homeKey()));
                    }
                }
            });
        }

        @Override
        public PreAcceptReply applyInternal(SafeCommandStore safeStore)
        {
            long minEpoch = topologies.oldestEpoch();
            StoreParticipants participants = StoreParticipants.update(safeStore, scope, minEpoch, txnId, txnId.epoch());
            SafeCommand safeCommand = safeStore.get(txnId, participants);

            ExecuteFlags flags;
            Deps deps;
            if (txnId.is(PrivilegedCoordinatorWithDeps))
            {
                try (DepsCalculator calculator = new DepsCalculator())
                {
                    deps = calculator.calculate(safeStore, txnId, participants, minEpoch, txnId, true);
                    if (deps == null)
                        return PreAcceptNack.INSTANCE;
                    flags = calculator.executeFlags(txnId);
                }

                Commands.AcceptOutcome outcome = Commands.preaccept(safeStore, safeCommand, participants, txnId, txn, deps, true);
                if (outcome != Success)
                    return PreAcceptNack.INSTANCE;
            }
            else
            {
                Commands.AcceptOutcome outcome = Commands.preaccept(safeStore, safeCommand, participants, txnId, txn, null, true);
                if (outcome != Success)
                    return PreAcceptNack.INSTANCE;

                try (DepsCalculator calculator = new DepsCalculator())
                {
                    deps = calculator.calculate(safeStore, txnId, participants, minEpoch, txnId, true);
                    if (deps == null)
                        return PreAcceptNack.INSTANCE;
                    flags = calculator.executeFlags(txnId);
                }
            }

            Command command = safeCommand.current();
            return new PreAcceptOk(txnId, command.executeAt(), deps, flags);
        }

        @Override
        public PreAcceptReply reduce(PreAcceptReply r1, PreAcceptReply r2)
        {
            return PreAcceptReply.reduce(r1, r2);
        }

        @Override
        public void timeout()
        {
            Cancellable cancel;
            synchronized (this)
            {
                if (state != PENDING)
                    return;

                state = TIMEOUT;
                timeout = null;
                if (this.cancel == null)
                    return;
                cancel = this.cancel;
                this.cancel = null;
            }
            cancel.cancel();
        }

        void success()
        {
            RegisteredTimeout cancel;
            synchronized (this)
            {
                if (state != PENDING)
                    return;

                state = SUCCESS;
                this.cancel = null;
                if (timeout == null)
                    return;
                cancel = timeout;
                timeout = null;
            }
            cancel.cancel();
        }

        @Override
        public int stripe()
        {
            return txnId == null ? 0 : txnId.hashCode();
        }

        @Nullable
        @Override
        public TxnId primaryTxnId()
        {
            return txnId;
        }

        @Override
        public String reason()
        {
            return "Local PreAccept";
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
    }
}
