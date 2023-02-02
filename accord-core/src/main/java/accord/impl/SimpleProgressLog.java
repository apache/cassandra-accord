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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import accord.utils.async.AsyncResult;

import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.coordinate.*;
import accord.local.*;
import accord.local.Node.Id;
import accord.local.Status.Known;
import accord.messages.Callback;
import accord.messages.InformDurable;
import accord.messages.SimpleReply;
import accord.primitives.*;
import accord.topology.Topologies;
import accord.utils.Invariants;

import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.coordinate.InformHomeOfTxn.inform;
import static accord.impl.SimpleProgressLog.CoordinateStatus.ReadyToExecute;
import static accord.impl.SimpleProgressLog.CoordinateStatus.Uncommitted;
import static accord.impl.SimpleProgressLog.DisseminateStatus.NotExecuted;
import static accord.impl.SimpleProgressLog.Progress.Done;
import static accord.impl.SimpleProgressLog.Progress.Expected;
import static accord.impl.SimpleProgressLog.Progress.Investigating;
import static accord.impl.SimpleProgressLog.Progress.NoProgress;
import static accord.impl.SimpleProgressLog.Progress.NoneExpected;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.Status.Durability.Durable;
import static accord.local.Status.Known.Nothing;
import static accord.local.Status.PreApplied;
import static accord.local.Status.PreCommitted;
import static accord.primitives.Route.isFullRoute;

// TODO (desired, consider): consider propagating invalidations in the same way as we do applied
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

    enum DisseminateStatus { NotExecuted, Durable, Done }

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

                abstract void run(Command command);

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
                void run(Command command)
                {
                    setProgress(Investigating);
                    switch (status)
                    {
                        default: throw new AssertionError("Unexpected status: " + status);
                        case NotWitnessed: // can't make progress if we haven't witnessed it yet
                        case Committed: // can't make progress if we aren't yet ReadyToExecute
                        case Done: // shouldn't be trying to make progress, as we're done
                            throw new IllegalStateException("Unexpected status: " + status);

                        case Uncommitted:
                        case ReadyToExecute:
                        {
                            if (status.isAtLeastCommitted() && command.durability().isDurable())
                            {
                                // must also be committed, as at the time of writing we do not guarantee dissemination of Commit
                                // records to the home shard, so we only know the executeAt shards will have witnessed this
                                // if the home shard is at an earlier phase, it must run recovery
                                long epoch = command.executeAt().epoch();
                                node.withEpoch(epoch, () -> debugInvestigating = FetchData.fetch(PreApplied.minKnown, node, txnId, command.route(), epoch, (success, fail) -> {
                                    commandStore.execute(PreLoadContext.empty(), ignore -> {
                                        // should have found enough information to apply the result, but in case we did not reset progress
                                        if (progress() == Investigating)
                                            setProgress(Expected);
                                    });
                                }));
                            }
                            else
                            {
                                RoutingKey homeKey = command.homeKey();
                                node.withEpoch(txnId.epoch(), () -> {

                                    AsyncResult<? extends Outcome> recover = node.maybeRecover(txnId, homeKey, command.route(), token);
                                    recover.addCallback((success, fail) -> {
                                        commandStore.execute(PreLoadContext.empty(), ignore -> {
                                            if (status.isAtMostReadyToExecute() && progress() == Investigating)
                                            {
                                                setProgress(Expected);
                                                if (fail != null)
                                                    return;

                                                ProgressToken token = success.asProgressToken();
                                                if (token.durability.isDurable())
                                                {
                                                    commandStore.execute(contextFor(txnId), safeStore -> {
                                                        Command cmd = safeStore.command(txnId);
                                                        cmd.setDurability(safeStore, token.durability, homeKey, null);
                                                        safeStore.progressLog().durable(txnId, cmd.maxUnseekables(), null);
                                                    }).addCallback(commandStore.agent());
                                                }

                                                updateMax(token);
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

            // exists only on home shard
            class DisseminateState extends State.Monitoring
            {
                class CoordinateAwareness implements Callback<SimpleReply>
                {
                    @Override
                    public void onSuccess(Id from, SimpleReply reply)
                    {
                        // TODO (required, efficiency): callbacks should be associated with a commandStore for processing to avoid this
                        commandStore.execute(PreLoadContext.empty(), ignore -> {
                            if (progress() == Done)
                                return;

                            notAwareOfDurability.remove(from);
                            maybeDone();
                        });
                    }

                    @Override
                    public void onFailure(Id from, Throwable failure)
                    {
                    }

                    @Override
                    public void onCallbackFailure(Id from, Throwable failure)
                    {
                    }
                }

                DisseminateStatus status = NotExecuted;
                Set<Id> notAwareOfDurability; // TODO (easy, efficiency): use Agrona's IntHashSet as soon as Node.Id switches from long to int
                Set<Id> notPersisted;         // TODO (easy, efficiency): use Agrona's IntHashSet as soon as Node.Id switches from long to int

                List<Runnable> whenReady;

                CoordinateAwareness investigating;

                private void whenReady(Node node, Command command, Runnable runnable)
                {
                    if (notAwareOfDurability != null || maybeReady(node, command))
                    {
                        runnable.run();
                    }
                    else
                    {
                        if (whenReady == null)
                            whenReady = new ArrayList<>();
                        whenReady.add(runnable);
                    }
                }

                private void whenReady(Runnable runnable)
                {
                    if (notAwareOfDurability != null)
                    {
                        runnable.run();
                    }
                    else
                    {
                        if (whenReady == null)
                            whenReady = new ArrayList<>();
                        whenReady.add(runnable);
                    }
                }

                // must know the epoch information, and have a valid Route
                private boolean maybeReady(Node node, Command command)
                {
                    if (!command.status().hasBeen(Status.PreCommitted))
                        return false;

                    if (!isFullRoute(command.route()))
                        return false;

                    if (!node.topology().hasEpoch(command.executeAt().epoch()))
                        return false;

                    Topologies topology = node.topology().preciseEpochs(command.route(), command.txnId().epoch(), command.executeAt().epoch());
                    notAwareOfDurability = topology.copyOfNodes();
                    notPersisted = topology.copyOfNodes();
                    if (whenReady != null)
                    {
                        whenReady.forEach(Runnable::run);
                        whenReady = null;
                    }

                    return true;
                }

                private void maybeDone()
                {
                    if (progress() != Done && notAwareOfDurability.isEmpty())
                    {
                        status = DisseminateStatus.Done;
                        setProgress(Done);
                    }
                }

                void durableGlobal(Node node, Command command, @Nullable Set<Id> persistedOn)
                {
                    if (status == DisseminateStatus.Done)
                        return;

                    status = DisseminateStatus.Durable;
                    setProgress(Expected);
                    if (persistedOn == null)
                        return;

                    whenReady(node, command, () -> {
                        notPersisted.removeAll(persistedOn);
                        notAwareOfDurability.removeAll(persistedOn);
                        maybeDone();
                    });
                }

                void durableLocal(Node node)
                {
                    if (status == DisseminateStatus.Done)
                        return;

                    status = DisseminateStatus.Durable;
                    setProgress(Expected);

                    whenReady(() -> {
                        notPersisted.remove(node.id());
                        notAwareOfDurability.remove(node.id());
                        maybeDone();
                    });
                }

                @Override
                void run(Command command)
                {
                    switch (status)
                    {
                        default: throw new IllegalStateException();
                        case NotExecuted:
                        case Done:
                            return;
                        case Durable:
                    }

                    if (notAwareOfDurability == null && !maybeReady(node, command))
                        return;

                    setProgress(Investigating);
                    if (notAwareOfDurability.isEmpty())
                    {
                        // TODO (required, consider): also track actual durability
                        status = DisseminateStatus.Done;
                        setProgress(Done);
                        return;
                    }

                    FullRoute<?> route = Route.castToFullRoute(command.route());
                    Timestamp executeAt = command.executeAt();
                    investigating = new CoordinateAwareness();
                    Topologies topologies = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
                    node.send(notAwareOfDurability, to -> new InformDurable(to, topologies, route, txnId, executeAt, Durable), investigating);
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

                Unseekables<?, ?> blockedOn;

                Object debugInvestigating;

                void recordBlocking(Known blockedUntil, Unseekables<?, ?> blockedOn)
                {
                    Invariants.checkState(!blockedOn.isEmpty());
                    if (this.blockedOn == null) this.blockedOn = blockedOn;
                    else this.blockedOn = Unseekables.merge(this.blockedOn, (Unseekables)blockedOn);
                    if (!blockedUntil.isSatisfiedBy(this.blockedUntil))
                    {
                        this.blockedUntil = this.blockedUntil.merge(blockedUntil);
                        setProgress(Expected);
                    }
                }

                void record(Known known)
                {
                    if (blockedUntil.isSatisfiedBy(known))
                        setProgress(NoneExpected);
                }

                @Override
                void run(Command command)
                {
                    if (command.has(blockedUntil))
                    {
                        setProgress(NoneExpected);
                        return;
                    }

                    setProgress(Investigating);
                    // first make sure we have enough information to obtain the command locally
                    Timestamp executeAt = command.hasBeen(PreCommitted) ? command.executeAt() : null;
                    long srcEpoch = (executeAt != null ? executeAt : txnId).epoch();
                    // TODO (desired, consider): compute fromEpoch, the epoch we already have this txn replicated until
                    long toEpoch = Math.max(srcEpoch, node.topology().epoch());
                    Unseekables<?, ?> someKeys = unseekables(command);

                    BiConsumer<Known, Throwable> callback = (success, fail) -> {
                        commandStore.execute(PreLoadContext.empty(), ignore -> {
                            if (progress() != Investigating)
                                return;

                            setProgress(Expected);
                            if (fail == null)
                            {
                                if (!success.isDefinitionKnown()) invalidate(node, txnId, someKeys);
                                else record(success);
                            }
                        });
                    };

                    node.withEpoch(toEpoch, () -> {
                        debugInvestigating = FetchData.fetch(blockedUntil, node, txnId, someKeys, executeAt, toEpoch, callback);
                    });
                }

                private Unseekables<?, ?> unseekables(Command command)
                {
                    return Unseekables.merge((Route)command.route(), blockedOn);
                }

                private void invalidate(Node node, TxnId txnId, Unseekables<?, ?> someKeys)
                {
                    setProgress(Investigating);
                    RoutingKey someKey = Route.isRoute(someKeys) ? (Route.castToRoute(someKeys)).homeKey() : someKeys.get(0).someIntersectingRoutingKey(null);
                    someKeys = someKeys.with(someKey);
                    debugInvestigating = Invalidate.invalidate(node, txnId, someKeys, (success, fail) -> {
                        commandStore.execute(PreLoadContext.empty(), ignore -> {
                            if (progress() != Investigating)
                                return;

                            setProgress(Expected);
                            if (fail == null && success.asProgressToken().durability.isDurable())
                                setProgress(Done);
                        });
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
                void run(Command command)
                {
                    // make sure a quorum of the home shard is aware of the transaction, so we can rely on it to ensure progress
                    AsyncResult<Void> inform = inform(node, txnId, command.homeKey());
                    inform.addCallback((success, fail) -> {
                        commandStore.execute(PreLoadContext.empty(), ignore -> {
                            if (progress() == Done)
                                return;

                            setProgress(fail != null ? Expected : Done);
                        });
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
            DisseminateState disseminateState;
            NonHomeState nonHomeState;
            BlockingState blockingState;

            State(TxnId txnId)
            {
                this.txnId = txnId;
            }

            void recordBlocking(TxnId txnId, Known waitingFor, Unseekables<?, ?> routables)
            {
                Invariants.checkArgument(txnId.equals(this.txnId));
                if (blockingState == null)
                    blockingState = new BlockingState();
                blockingState.recordBlocking(waitingFor, routables);
            }

            CoordinateState local()
            {
                if (coordinateState == null)
                    coordinateState = new CoordinateState();
                return coordinateState;
            }

            DisseminateState global()
            {
                if (disseminateState == null)
                    disseminateState = new DisseminateState();
                return disseminateState;
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
        final Map<TxnId, State> stateMap = new HashMap<>();
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
        public void unwitnessed(TxnId txnId, RoutingKey homeKey, ProgressShard shard)
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

        @Override
        public void invalidated(Command command, ProgressShard shard)
        {
            State state = recordApply(command.txnId());

            Invariants.checkState(shard == Home || state == null || state.coordinateState == null);

            // note: we permit Unsure here, so we check if we have any local home state
            if (shard.isProgress())
            {
                state = ensure(command.txnId(), state);

                if (shard.isHome()) state.ensureAtLeast(command, CoordinateStatus.Done, Done);
                else ensure(command.txnId()).setSafe();
            }
        }

        @Override
        public void durableLocal(TxnId txnId)
        {
            State state = ensure(txnId);
            state.global().durableLocal(node);
        }

        @Override
        public void durable(Command command, @Nullable Set<Id> persistedOn)
        {
            State state = ensure(command.txnId());
            if (!command.status().hasBeen(PreApplied))
                state.recordBlocking(command.txnId(), PreApplied.minKnown, command.maxUnseekables());
            state.local().durableGlobal();
            state.global().durableGlobal(node, command, persistedOn);
        }

        @Override
        public void durable(TxnId txnId, Unseekables<?, ?> unseekables, ProgressShard shard)
        {
            State state = ensure(txnId);
            // TODO (desirable, efficiency): we can probably simplify things by requiring (empty) Apply messages to be sent also to the coordinating topology
            state.recordBlocking(txnId, PreApplied.minKnown, unseekables);
        }

        @Override
        public void waiting(TxnId blockedBy, Known blockedUntil, Unseekables<?, ?> blockedOn)
        {
            // TODO (consider): consider triggering a preemption of existing coordinator (if any) in some circumstances;
            //                  today, an LWT can pre-empt more efficiently (i.e. instantly) a failed operation whereas Accord will
            //                  wait for some progress interval before taking over; there is probably some middle ground where we trigger
            //                  faster preemption once we're blocked on a transaction, while still offering some amount of time to complete.
            // TODO (desirable, efficiency): forward to local progress shard for processing (if known)
            // TODO (desirable, efficiency): if we are co-located with the home shard, don't need to do anything unless we're in a
            //                               later topology that wasn't covered by its coordination
            ensure(blockedBy).recordBlocking(blockedBy, blockedUntil, blockedOn);
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
            node.scheduler().once(() -> commandStore.execute(PreLoadContext.empty(), ignore -> run()), 200L, TimeUnit.MILLISECONDS);
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
                                run.run(safeStore.command(run.txnId()));
                        });
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
