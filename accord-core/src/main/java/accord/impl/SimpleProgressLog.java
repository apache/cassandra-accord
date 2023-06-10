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

package accord.impl;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import accord.utils.IntrusiveLinkedList;
import accord.utils.IntrusiveLinkedListNode;
import accord.coordinate.*;
import accord.local.*;
import accord.local.Status.Known;
import accord.primitives.*;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;

import accord.api.ProgressLog;

import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.coordinate.InformHomeOfTxn.inform;
import static accord.impl.SimpleProgressLog.CoordinateStatus.ReadyToExecute;
import static accord.impl.SimpleProgressLog.CoordinateStatus.Uncommitted;
import static accord.impl.SimpleProgressLog.Progress.Done;
import static accord.impl.SimpleProgressLog.Progress.Expected;
import static accord.impl.SimpleProgressLog.Progress.Investigating;
import static accord.impl.SimpleProgressLog.Progress.NoProgress;
import static accord.impl.SimpleProgressLog.Progress.NoneExpected;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.PreLoadContext.empty;
import static accord.local.Status.Durability.Majority;
import static accord.local.Status.Known.Nothing;
import static accord.local.Status.PreApplied;
import static accord.local.Status.PreCommitted;

// TODO (desired, consider): consider propagating invalidations in the same way as we do applied
// TODO (expected): report long-lived recurring transactions / operations
public class SimpleProgressLog implements ProgressLog.Factory
{
    enum Progress { NoneExpected, Expected, NoProgress, Investigating, Done }

    enum CoordinateStatus
    {
        NotWitnessed, Uncommitted, Committed, ReadyToExecute, Done;

        boolean isAtMostReadyToExecute()
        {
            return compareTo(CoordinateStatus.ReadyToExecute) <= 0;
        }

        boolean isAtLeastCommitted()
        {
            return compareTo(CoordinateStatus.Committed) >= 0;
        }
    }

    final Node node;
    final List<Instance> instances = new CopyOnWriteArrayList<>();

    public SimpleProgressLog(Node node)
    {
        this.node = node;
    }

    class Instance extends IntrusiveLinkedList<Instance.State.Monitoring> implements ProgressLog, Runnable
    {
        class State
        {
            abstract class Monitoring extends IntrusiveLinkedListNode
            {
                private Progress progress = NoneExpected;

                void setProgress(Progress newProgress)
                {
                    Invariants.checkState(progress != Done);

                    progress = newProgress;
                    switch (newProgress)
                    {
                        default: throw new AssertionError();
                        case NoneExpected:
                        case Done:
                        case Investigating:
                            remove();
                            Invariants.paranoid(isFree());
                            break;
                        case Expected:
                        case NoProgress:
                            if (isFree())
                                addFirst(this);
                    }
                }

                void ensureDone()
                {
                    if (progress != Done)
                        setProgress(Done);
                }

                boolean shouldRun()
                {
                    switch (progress)
                    {
                        default: throw new AssertionError("Unexpected progress: " + progress);
                        case NoneExpected:
                        case Done:
                            return false;
                        case Investigating:
                            throw new IllegalStateException("Unexpected progress: " + progress);
                        case Expected:
                            Invariants.paranoid(!isFree());
                            progress = NoProgress;
                            return false;
                        case NoProgress:
                            remove();
                            return true;
                    }
                }

                abstract void run(SafeCommandStore safeStore, SafeCommand safeCommand);

                Progress progress()
                {
                    return progress;
                }

                TxnId txnId()
                {
                    return txnId;
                }
            }

            // exists only on home shard
            // TODO (expected): should not take any prompt action if we're ourselves already coordinating the transaction
            class CoordinateState extends Monitoring
            {
                CoordinateStatus status = CoordinateStatus.NotWitnessed;
                ProgressToken token = ProgressToken.NONE;

                Object debugInvestigating;

                void ensureAtLeast(Command command, CoordinateStatus newStatus, Progress newProgress)
                {
                    ensureAtLeast(newStatus, newProgress);
                    updateMax(command);
                }

                void ensureAtLeast(CoordinateStatus newStatus, Progress newProgress)
                {
                    if (newStatus.compareTo(status) > 0)
                    {
                        status = newStatus;
                        setProgress(newProgress);
                    }
                }

                void updateMax(Command command)
                {
                    token = token.merge(new ProgressToken(command.durability(), command.status(), command.promised(), command.accepted()));
                }

                void updateMax(ProgressToken ok)
                {
                    // TODO (low priority): perhaps set localProgress back to Waiting if Investigating and we update anything?
                    token = token.merge(ok);
                }

                void durableGlobal()
                {
                    switch (status)
                    {
                        default: throw new IllegalStateException();
                        case NotWitnessed:
                        case Uncommitted:
                        case Committed:
                        case ReadyToExecute:
                            status = CoordinateStatus.Done;
                            setProgress(Done);
                        case Done:
                    }
                }

                @Override
                void run(SafeCommandStore safeStore, SafeCommand safeCommand)
                {
                    Command command = safeCommand.current();
                    Invariants.checkState(!safeStore.isTruncated(command));
                    setProgress(Investigating);
                    switch (status)
                    {
                        default: throw new AssertionError("Unexpected status: " + status);
                        case NotWitnessed: // can't make progress if we haven't witnessed it yet
                        case Committed: // can't make progress if we aren't yet ReadyToExecute
                        case Done: // shouldn't be trying to make progress, as we're done
                            throw new IllegalStateException("Unexpected status: " + status);

                        case ReadyToExecute:
                            if (command.durability().isDurableOrInvalidated())
                            {
                                // should not reach here with an invalidated state
                                Invariants.checkState(command.durability().isDurable());
                                durableGlobal();
                                return;
                            }

                        case Uncommitted:
                        {
                            Invariants.checkState(token.status != Status.Invalidated);
                            // TODO (expected): this should be encapsulated in Recover/FetchData
                            if (!command.durability().isDurable() && token.durability.isDurableOrInvalidated() && (token.durability.isDurable() || command.hasBeen(PreCommitted)))
                                command = Commands.setDurability(safeStore, safeCommand, txnId, Majority);

                            if (command.durability().isDurable() || token.durability.isDurableOrInvalidated())
                            {
                                // not guaranteed to know the execution epoch, as might have received durability info obliquely
                                Timestamp executeAt = command.known().executeAt.hasDecidedExecuteAt() ? command.executeAt() : null;
                                long epoch = executeAt == null ? txnId.epoch() : executeAt.epoch();
                                Route<?> route = command.route();

                                node.withEpoch(epoch, () -> debugInvestigating = FetchData.fetch(PreApplied.minKnown, node, txnId, route, executeAt, (success, fail) -> {
                                    commandStore.execute(empty(), ignore -> {
                                        // should have found enough information to apply the result, but in case we did not reset progress
                                        if (progress() == Investigating)
                                            setProgress(Expected);
                                    }).begin(commandStore.agent());
                                }));
                            }
                            else
                            {
                                Route<?> route = Invariants.nonNull(command.route()).withHomeKey();
                                node.withEpoch(txnId.epoch(), () -> {
                                    AsyncResult<? extends Outcome> recover = node.maybeRecover(txnId, route, token);
                                    recover.addCallback((success, fail) -> {
                                        // TODO (expected): callback should be on safeStore, and should provide safeStore as a parameter
                                        commandStore.execute(contextFor(txnId), safeStore0 -> {
                                            if (status.isAtMostReadyToExecute() && progress() == Investigating)
                                            {
                                                setProgress(Expected);
                                                if (fail != null)
                                                    return;

                                                updateMax(success.asProgressToken());
                                            }
                                        });
                                    });

                                    debugInvestigating = recover;
                                });
                            }
                        }
                    }
                }

                @Override
                public String toString()
                {
                    return "{" + status + ',' + progress() + '}';
                }
            }

            class BlockingState extends State.Monitoring
            {
                Known blockedUntil = Nothing;

                Route<?> route;
                Participants<?> participants;

                Object debugInvestigating;

                void recordBlocking(Known blockedUntil, @Nullable Route<?> route, @Nullable Participants<?> participants)
                {
                    Invariants.checkState(route != null || participants != null);
                    Invariants.checkState(participants == null || !participants.isEmpty());
                    Invariants.checkState(route == null || route.hasParticipants());

                    this.route = Route.merge(this.route, (Route)route);
                    this.participants = Participants.merge(this.participants, (Participants) participants);
                    if (!blockedUntil.isSatisfiedBy(this.blockedUntil))
                    {
                        this.blockedUntil = this.blockedUntil.merge(blockedUntil);
                        setProgress(Expected);
                    }
                }

                void record(Known known)
                {
                    // invalidation coordination callback may fire
                    // before invalidation is committed locally
                    if (progress() == Done)
                        return;

                    if (blockedUntil.isSatisfiedBy(known))
                        setProgress(NoneExpected);
                }

                @Override
                void run(SafeCommandStore safeStore, SafeCommand safeCommand)
                {
                    Command command = safeCommand.current();
                    if (command.has(blockedUntil))
                    {
                        setProgress(NoneExpected);
                        return;
                    }

                    setProgress(Investigating);
                    // first make sure we have enough information to obtain the command locally
                    Timestamp executeAt = command.known().executeAt.hasDecidedExecuteAt() ? command.executeAt() : null;
                    Participants<?> maxParticipants = maxParticipants(command);
                    // we want to fetch a route if we have it, so that we can go to our neighbouring shards for info
                    // (rather than the home shard, which may have GC'd its state if the result is durable)
                    Unseekables<?> fetchKeys = maxContact(command);

                    BiConsumer<Known, Throwable> callback = (success, fail) -> {
                        // TODO (expected): this should be invoked on this commandStore
                        commandStore.execute(contextFor(txnId), safeStore0 -> {
                            if (progress() != Investigating)
                                return;

                            setProgress(Expected);
                            if (fail == null)
                            {
                                if (success.canProposeInvalidation())
                                    invalidate(node, txnId, maxParticipants);
                                else
                                    record(success);
                            }
                        });
                    };

                    node.withEpoch(blockedUntil.fetchEpoch(txnId, executeAt), () -> {
                        debugInvestigating = FetchData.fetch(blockedUntil, node, txnId, fetchKeys, executeAt, callback);
                    });
                }

                private Unseekables<?> maxContact(Command command)
                {
                    Route<?> route = Route.merge(command.route(), (Route)this.route);
                    if (route != null) route = route.withHomeKey();
                    return Unseekables.merge(route == null ? null : route.withHomeKey(), (Unseekables)participants);
                }

                private Participants<?> maxParticipants(Command command)
                {
                    Route<?> route = Route.merge(command.route(), (Route)this.route);
                    return Participants.merge(route == null ? null : route.participants(), (Participants) participants);
                }

                private void invalidate(Node node, TxnId txnId, Participants<?> participants)
                {
                    setProgress(Investigating);
                    // TODO (now): we should avoid invalidating with home key, as this simplifies erasing of invalidated transactions
                    debugInvestigating = Invalidate.invalidate(node, txnId, participants, (success, fail) -> {
                        commandStore.execute(contextFor(txnId), safeStore -> {
                            if (progress() != Investigating)
                                return;

                            setProgress(Expected);
                            SafeCommand safeCommand = safeStore.ifInitialised(txnId);
                            Invariants.checkState(safeCommand != null, "No initialised command for %s but have active progress log", txnId);
                            record(safeCommand.current().known());
                        }).begin(commandStore.agent());
                    });
                }

                @Override
                public String toString()
                {
                    return progress().toString();
                }
            }

            class NonHomeState extends State.Monitoring
            {
                NonHomeState()
                {
                    setProgress(Expected);
                }

                void setSafe()
                {
                    if (progress() != Done)
                        setProgress(Done);
                }

                @Override
                void run(SafeCommandStore safeStore, SafeCommand safeCommand)
                {
                    Command command = safeCommand.current();
                    // make sure a quorum of the home shard is aware of the transaction, so we can rely on it to ensure progress
                    AsyncChain<Void> inform = inform(node, txnId, command.route());
                    inform.begin((success, fail) -> {
                        commandStore.execute(empty(), ignore -> {
                            if (progress() == Done)
                                return;

                            setProgress(fail != null ? Expected : Done);
                        }).begin(commandStore.agent());
                    });
                }

                @Override
                public String toString()
                {
                    return progress() == Done ? "Safe" : "Unsafe";
                }
            }

            final TxnId txnId;

            CoordinateState coordinateState;
            NonHomeState nonHomeState;
            BlockingState blockingState;

            State(TxnId txnId)
            {
                this.txnId = txnId;
            }

            void recordBlocking(TxnId txnId, Known waitingFor, Route<?> route, Participants<?> participants)
            {
                Invariants.checkArgument(txnId.equals(this.txnId));
                if (blockingState == null)
                    blockingState = new BlockingState();
                blockingState.recordBlocking(waitingFor, route, participants);
            }

            CoordinateState local()
            {
                if (coordinateState == null)
                    coordinateState = new CoordinateState();
                return coordinateState;
            }

            void ensureAtLeast(Command command, CoordinateStatus newStatus, Progress newProgress)
            {
                local().ensureAtLeast(command, newStatus, newProgress);
            }

            void ensureAtLeast(CoordinateStatus newStatus, Progress newProgress)
            {
                local().ensureAtLeast(newStatus, newProgress);
            }

            void touchNonHomeUnsafe()
            {
                if (nonHomeState == null)
                    nonHomeState = new NonHomeState();
            }

            void setSafe()
            {
                if (nonHomeState == null)
                    nonHomeState = new NonHomeState();
                nonHomeState.setSafe();
            }

            @Override
            public String toString()
            {
                return coordinateState != null ? coordinateState.toString()
                        : nonHomeState != null
                        ? nonHomeState.toString()
                        : blockingState.toString();
            }
        }

        final CommandStore commandStore;
        final Map<TxnId, State> stateMap = new TreeMap<>();
        boolean isScheduled;

        Instance(CommandStore commandStore)
        {
            this.commandStore = commandStore;
        }

        State ensure(TxnId txnId)
        {
            return stateMap.computeIfAbsent(txnId, State::new);
        }

        State ensure(TxnId txnId, State state)
        {
            return state != null ? state : ensure(txnId);
        }

        private void ensureSafeOrAtLeast(Command command, ProgressShard shard, CoordinateStatus newStatus, Progress newProgress)
        {
            Invariants.checkState(shard != Unsure);

            State state = null;
            assert newStatus.isAtMostReadyToExecute();
            if (newStatus.isAtLeastCommitted())
                state = recordCommit(command.txnId());

            if (shard.isProgress())
            {
                state = ensure(command.txnId(), state);

                if (shard.isHome()) state.ensureAtLeast(command, newStatus, newProgress);
                else ensure(command.txnId()).setSafe();
            }
        }

        State recordCommit(TxnId txnId)
        {
            State state = stateMap.get(txnId);
            if (state != null && state.blockingState != null)
                state.blockingState.record(SaveStatus.Committed.known);
            return state;
        }

        State recordApply(TxnId txnId)
        {
            State state = stateMap.get(txnId);
            if (state != null && state.blockingState != null)
                state.blockingState.record(SaveStatus.PreApplied.known);
            return state;
        }

        @Override
        public void unwitnessed(TxnId txnId, ProgressShard shard)
        {
            if (shard.isHome())
                ensure(txnId).ensureAtLeast(Uncommitted, Expected);
        }

        @Override
        public void preaccepted(Command command, ProgressShard shard)
        {
            Invariants.checkState(shard != Unsure);

            if (shard.isProgress())
            {
                State state = ensure(command.txnId());
                if (shard.isHome()) state.ensureAtLeast(command, Uncommitted, Expected);
                else state.touchNonHomeUnsafe();
            }
        }

        @Override
        public void accepted(Command command, ProgressShard shard)
        {
            ensureSafeOrAtLeast(command, shard, Uncommitted, Expected);
        }

        @Override
        public void committed(Command command, ProgressShard shard)
        {
            ensureSafeOrAtLeast(command, shard, CoordinateStatus.Committed, NoneExpected);
        }

        @Override
        public void readyToExecute(Command command, ProgressShard shard)
        {
            ensureSafeOrAtLeast(command, shard, ReadyToExecute, Expected);
        }

        @Override
        public void executed(Command command, ProgressShard shard)
        {
            recordApply(command.txnId());
            // this is the home shard's state ONLY, so we don't know it is fully durable locally
            ensureSafeOrAtLeast(command, shard, ReadyToExecute, Expected);
        }

        public void clear(TxnId txnId)
        {
            State state = stateMap.remove(txnId);
            if (state == null)
                return;

            if (state.blockingState != null)
                state.blockingState.ensureDone();
            if (state.nonHomeState != null)
                state.nonHomeState.ensureDone();
            if (state.coordinateState != null)
                state.coordinateState.durableGlobal();
        }

        @Override
        public void durable(Command command)
        {
            State state = ensure(command.txnId());
            // if we participate in the transaction and its outcome hasn't been recorded locally then fetch it
            // TODO (expected): we should delete the in-memory state once we reach Done, and guard against backward progress with Command state
            //   however, we need to be careful:
            //     - we might participate in the execution epoch so need to be sure we have received a route covering both
            if (!command.status().hasBeen(PreApplied) && command.route() != null && command.route().hasParticipants())
                state.recordBlocking(command.txnId(), PreApplied.minKnown, command.route(), null);
            if (command.progressShard() == Home)
                state.local().durableGlobal();
        }

        @Override
        public void waiting(SafeCommand blockedBy, Known blockedUntil, Route<?> blockedOnRoute, Participants<?> blockedOnParticipants)
        {
            if (blockedBy.txnId().rw().isLocal())
                return;

            // ensure we have a record to work with later; otherwise may think has been truncated
            blockedBy.initialise();

            // TODO (consider): consider triggering a preemption of existing coordinator (if any) in some circumstances;
            //                  today, an LWT can pre-empt more efficiently (i.e. instantly) a failed operation whereas Accord will
            //                  wait for some progress interval before taking over; there is probably some middle ground where we trigger
            //                  faster preemption once we're blocked on a transaction, while still offering some amount of time to complete.
            // TODO (desirable, efficiency): forward to local progress shard for processing (if known)
            // TODO (desirable, efficiency): if we are co-located with the home shard, don't need to do anything unless we're in a
            //                               later topology that wasn't covered by its coordination
            ensure(blockedBy.txnId()).recordBlocking(blockedBy.txnId(), blockedUntil, blockedOnRoute, blockedOnParticipants);
        }

        @Override
        public void addFirst(State.Monitoring add)
        {
            super.addFirst(add);
            ensureScheduled();
        }

        @Override
        public void addLast(State.Monitoring add)
        {
            throw new UnsupportedOperationException();
        }

        void ensureScheduled()
        {
            if (isScheduled)
                return;

            isScheduled = true;
            node.scheduler().once(() -> commandStore.execute(empty(), ignore -> run()).begin(commandStore.agent()), 1L, TimeUnit.SECONDS);
        }

        @Override
        public void run()
        {
            isScheduled = false;
            try
            {
                for (State.Monitoring run : this)
                {
                    if (run.shouldRun())
                    {
                        commandStore.execute(contextFor(run.txnId()), safeStore -> {
                            if (run.shouldRun()) // could have been completed by a callback
                            {
                                SafeCommand safeCommand = safeStore.ifInitialised(run.txnId());
                                if (safeCommand != null && run.shouldRun()) // could be truncated on access
                                    run.run(safeStore, safeCommand);
                            }
                        }).begin(commandStore.agent());
                    }
                }
            }
            catch (Throwable t)
            {
                t.printStackTrace();
            }
            finally
            {
                if (!isEmpty())
                    ensureScheduled();
            }
        }
    }

    @Override
    public Instance create(CommandStore commandStore)
    {
        Instance instance = new Instance(commandStore);
        instances.add(instance);
        return instance;
    }
}
