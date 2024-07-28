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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ProgressLog;
import accord.coordinate.FetchData;
import accord.coordinate.Outcome;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;

import accord.local.SaveStatus.LocalExecution;
import accord.local.Status.Known;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.IntrusiveLinkedList;
import accord.utils.IntrusiveLinkedListNode;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;

import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.coordinate.InformHomeOfTxn.inform;
import static accord.impl.SimpleProgressLog.CoordinateStatus.ReadyToExecute;
import static accord.impl.SimpleProgressLog.CoordinateStatus.NotStable;
import static accord.impl.SimpleProgressLog.Progress.Done;
import static accord.impl.SimpleProgressLog.Progress.Expected;
import static accord.impl.SimpleProgressLog.Progress.Investigating;
import static accord.impl.SimpleProgressLog.Progress.NoProgress;
import static accord.impl.SimpleProgressLog.Progress.NoneExpected;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.PreLoadContext.empty;
import static accord.local.SaveStatus.LocalExecution.NotReady;
import static accord.local.SaveStatus.LocalExecution.WaitingToApply;
import static accord.local.Status.KnownRoute.Full;
import static accord.local.Status.PreApplied;
import static accord.utils.Invariants.illegalState;

// TODO (desired, consider): consider propagating invalidations in the same way as we do applied
// TODO (expected): report long-lived recurring transactions / operations
public class SimpleProgressLog implements ProgressLog.Factory
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleProgressLog.class);

    enum Progress { NoneExpected, Expected, NoProgress, Investigating, Done }

    public static volatile boolean PAUSE_FOR_TEST = false;

    enum CoordinateStatus
    {
        NotWitnessed, NotStable, Stable, ReadyToExecute, Done;

        boolean isAtMostReadyToExecute()
        {
            return compareTo(CoordinateStatus.ReadyToExecute) <= 0;
        }

        boolean isAtLeastCommitted()
        {
            return compareTo(CoordinateStatus.Stable) >= 0;
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
                            throw illegalState("Unexpected progress: " + progress);
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
                    token = token.merge(new ProgressToken(command.durability(), command.status(), command.promised(), command.acceptedOrCommitted()));
                }

                void updateMax(ProgressToken ok)
                {
                    // TODO (low priority): perhaps set localProgress back to Waiting if Investigating and we update anything?
                    token = token.merge(ok);
                    if (token.durability.isDurableOrInvalidated())
                        durableGlobal();
                }

                void durableGlobal()
                {
                    switch (status)
                    {
                        default: throw new IllegalStateException();
                        case NotWitnessed:
                        case NotStable:
                        case Stable:
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
                    Invariants.checkState(!safeStore.isTruncated(command), "Command %s is truncated", command);
                    setProgress(Investigating);
                    switch (status)
                    {
                        default: throw new AssertionError("Unexpected status: " + status);
                        case NotWitnessed: // can't make progress if we haven't witnessed it yet
                        case Stable: // can't make progress if we aren't yet ReadyToExecute
                        case Done: // shouldn't be trying to make progress, as we're done
                            throw illegalState("Unexpected status: " + status);

                        case ReadyToExecute:
                            // TODO (expected): we should have already exited this loop if this conditions is true
                            if (command.durability().isDurableOrInvalidated())
                            {
                                // should not reach here with an invalidated state
                                Invariants.checkState(command.durability().isDurable(), "Command %s is not durable", command);
                                durableGlobal();
                                return;
                            }

                        case NotStable:
                        {
                            Invariants.checkState(!token.durability.isDurableOrInvalidated(), "ProgressToken is durable invalidated, but we have not cleared the ProgressLog");
                            Invariants.checkState(!command.durability().isDurableOrInvalidated(), "Command is durable or invalidated, but we have not cleared the ProgressLog");

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
                                    }).begin(node.agent());
                                });
                            });
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
                LocalExecution blockedUntil = NotReady;

                Route<?> route;
                Participants<?> participants;

                @SuppressWarnings({"unchecked", "rawtypes"})
                void recordBlocking(LocalExecution blockedUntil, @Nullable Route<?> route, @Nullable Participants<?> participants)
                {
                    Invariants.checkState(route != null || participants != null, "Route and participants are both undefined");
                    Invariants.checkState(participants == null || !participants.isEmpty(), "participants is empty");
                    Invariants.checkState(route == null || !route.isEmpty(), "Route %s is empty", route);

                    this.route = Route.merge(this.route, (Route)route);
                    this.participants = Participants.merge(this.participants, (Participants) participants);
                    if (blockedUntil.compareTo(this.blockedUntil) > 0)
                    {
                        this.blockedUntil = blockedUntil;
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
                    if (command.isAtLeast(blockedUntil))
                    {
                        setProgress(NoneExpected);
                        return;
                    }

                    setProgress(Investigating);
                    // first make sure we have enough information to obtain the command locally
                    Timestamp executeAt = command.executeAtIfKnown();
                    // we want to fetch a route if we have it, so that we can go to our neighbouring shards for info
                    // (rather than the home shard, which may have GC'd its state if the result is durable)
                    Unseekables<?> fetchKeys = maxContact(command);
                    EpochSupplier forLocalEpoch = safeStore.ranges().latestEpochWithNewParticipants(txnId.epoch(), fetchKeys);

                    BiConsumer<Known, Throwable> callback = (success, fail) -> {
                        // TODO (expected): this should be invoked on this commandStore; also do not need to load txn unless in DEBUG mode
                        commandStore.execute(contextFor(txnId), safeStore0 -> {
                            if (progress() != Investigating)
                                return;

                            setProgress(Expected);
                            // TODO (required): we might not be in the coordinating OR execution epochs if an accept round contacted us but recovery did not (quite hard to achieve)
                            Invariants.checkState(fail != null || !blockedUntil.isSatisfiedBy(success.propagates()) || success.route != Full);
                        }).begin(commandStore.agent());
                    };

                    node.withEpoch(blockedUntil.fetchEpoch(txnId, executeAt), () -> {
                        FetchData.fetch(blockedUntil.requires, node, txnId, fetchKeys, forLocalEpoch, executeAt, callback);
                    });
                }

                @SuppressWarnings({"rawtypes", "unchecked"})
                private Unseekables<?> maxContact(Command command)
                {
                    Route<?> route = Route.merge(command.route(), (Route)this.route);
                    if (route != null) route = route.withHomeKey();
                    return Unseekables.merge(route == null ? null : route.withHomeKey(), (Unseekables)participants);
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
                    if (command.route() == null)
                        throw new AssertionError(String.format("Attempted to inform but route is not known for command %s", command));
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

            void recordBlocking(TxnId txnId, LocalExecution waitingFor, Route<?> route, Participants<?> participants)
            {
                Invariants.checkArgument(txnId.equals(this.txnId));
                if (blockingState == null)
                    blockingState = new BlockingState();
                blockingState.recordBlocking(waitingFor, route, participants);
            }

            CoordinateState coordinate()
            {
                if (coordinateState == null)
                    coordinateState = new CoordinateState();
                return coordinateState;
            }

            void ensureAtLeast(Command command, CoordinateStatus newStatus, Progress newProgress)
            {
                coordinate().ensureAtLeast(command, newStatus, newProgress);
            }

            void ensureAtLeast(CoordinateStatus newStatus, Progress newProgress)
            {
                coordinate().ensureAtLeast(newStatus, newProgress);
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
            Invariants.checkState(txnId.kind().isGloballyVisible());
            return stateMap.computeIfAbsent(txnId, ignored -> {
                node.agent().metricsEventsListener().onProgressLogSizeChange(txnId, 1);
                return new State(txnId);
            });
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
                state = recordStable(command.txnId());

            if (shard.isProgress())
            {
                state = ensure(command.txnId(), state);

                if (shard.isHome()) state.ensureAtLeast(command, newStatus, newProgress);
                else ensure(command.txnId()).setSafe();
            }
        }

        State recordStable(TxnId txnId)
        {
            State state = stateMap.get(txnId);
            if (state != null && state.blockingState != null)
                state.blockingState.record(SaveStatus.Stable.known);
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
                ensure(txnId).ensureAtLeast(NotStable, Expected);
        }

        @Override
        public void preaccepted(Command command, ProgressShard shard)
        {
            Invariants.checkState(shard != Unsure);

            if (shard.isProgress())
            {
                State state = ensure(command.txnId());
                if (shard.isHome()) state.ensureAtLeast(command, NotStable, Expected);
                else state.touchNonHomeUnsafe();
            }
        }

        @Override
        public void accepted(Command command, ProgressShard shard)
        {
            ensureSafeOrAtLeast(command, shard, NotStable, Expected);
        }

        @Override
        public void precommitted(Command command)
        {
            State state = stateMap.get(command.txnId());
            if (state != null && state.blockingState != null)
                state.blockingState.record(SaveStatus.PreCommitted.known);
        }

        @Override
        public void stable(Command command, ProgressShard shard)
        {
            ensureSafeOrAtLeast(command, shard, CoordinateStatus.Stable, NoneExpected);
        }

        @Override
        public void readyToExecute(Command command)
        {
            State state = stateMap.get(command.txnId());
            if (state == null)
                return; // not progress shard, and nothing blocking

            Invariants.checkState(state.nonHomeState == null || state.nonHomeState.progress() == Done, "nonHomeState should have been set safe by call to committed");
            if (state.coordinateState != null)
                state.coordinateState.ensureAtLeast(command, ReadyToExecute, Expected);
        }

        @Override
        public void executed(Command command, ProgressShard shard)
        {
            recordApply(command.txnId());
            // this is the home shard's state ONLY, so we don't know it is fully durable locally
            ensureSafeOrAtLeast(command, shard, ReadyToExecute, Expected);
        }

        @Override
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
            if (command.route() == null)
                return;

            Ranges coordinateRanges = commandStore.unsafeRangesForEpoch().allAt(command.txnId().epoch());
            if (!command.status().hasBeen(PreApplied) && command.route().participatesIn(coordinateRanges))
                state.recordBlocking(command.txnId(), WaitingToApply, command.route(), null);
            if (coordinateRanges.contains(command.route().homeKey()))
                state.coordinate().durableGlobal();
        }

        @Override
        public void waiting(SafeCommand blockedBy, LocalExecution blockedUntil, Route<?> blockedOnRoute, Participants<?> blockedOnParticipants)
        {
            if (!blockedBy.txnId().kind().isGloballyVisible())
                return;

            // ensure we have a record to work with later; otherwise may think has been truncated
            // TODO (expected): we shouldn't rely on this anymore
            blockedBy.initialise();
            if (blockedBy.current().has(blockedUntil.requires))
                return;

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
        public void waiting(TxnId blockedBy, LocalExecution blockedUntil, @Nullable Route<?> blockedOnRoute, @Nullable Participants<?> blockedOnParticipants)
        {
            if (!blockedBy.kind().isGloballyVisible())
                return;

            ensure(blockedBy).recordBlocking(blockedBy, blockedUntil, blockedOnRoute, blockedOnParticipants);
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
            Duration delay = node.localConfig().getProgressLogScheduleDelay();
            node.scheduler().once(() -> commandStore.execute(empty(), ignore -> run()).begin(commandStore.agent()), delay.toNanos(), TimeUnit.NANOSECONDS);
        }

        @Override
        public void run()
        {
            isScheduled = false;
            try
            {
                if (PAUSE_FOR_TEST)
                {
                    logger.info("Skipping progress log because it is paused for test");
                    return;
                }

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
                node.agent().onUncaughtException(t);
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
