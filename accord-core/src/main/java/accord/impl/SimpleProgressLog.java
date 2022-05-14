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

import com.google.common.base.Preconditions;

import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.coordinate.FindHomeKey;
import accord.coordinate.FindRoute;
import accord.coordinate.Invalidate;
import accord.impl.SimpleProgressLog.CoordinateState.CoordinateStatus;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.Callback;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.InformDurable;
import accord.messages.SimpleReply;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRanges;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.coordinate.CheckOnCommitted.checkOnCommitted;
import static accord.coordinate.CheckOnUncommitted.checkOnUncommitted;
import static accord.coordinate.InformHomeOfTxn.inform;
import static accord.impl.SimpleProgressLog.DisseminateState.DisseminateStatus.Durable;
import static accord.impl.SimpleProgressLog.DisseminateState.DisseminateStatus.NotExecuted;
import static accord.impl.SimpleProgressLog.CoordinateState.CoordinateStatus.NotWitnessed;
import static accord.impl.SimpleProgressLog.CoordinateState.CoordinateStatus.ReadyToExecute;
import static accord.impl.SimpleProgressLog.CoordinateState.CoordinateStatus.Uncommitted;
import static accord.impl.SimpleProgressLog.NonHomeState.Safe;
import static accord.impl.SimpleProgressLog.NonHomeState.StillUnsafe;
import static accord.impl.SimpleProgressLog.NonHomeState.Unsafe;
import static accord.impl.SimpleProgressLog.Progress.Done;
import static accord.impl.SimpleProgressLog.Progress.Expected;
import static accord.impl.SimpleProgressLog.Progress.Investigating;
import static accord.impl.SimpleProgressLog.Progress.NoProgress;
import static accord.impl.SimpleProgressLog.Progress.NoneExpected;
import static accord.impl.SimpleProgressLog.Progress.advance;
import static accord.local.Status.Executed;

// TODO (now): consider propagating invalidations in the same way as we do applied
public class SimpleProgressLog implements Runnable, ProgressLog.Factory
{
    enum Progress
    {
        NoneExpected, Expected, NoProgress, Investigating, Done;

        static Progress advance(Progress current)
        {
            switch (current)
            {
                default: throw new IllegalStateException();
                case NoneExpected:
                case Investigating:
                case Done:
                    return current;
                case Expected:
                case NoProgress:
                    return NoProgress;
            }
        }
    }

    // exists only on home shard
    static class CoordinateState
    {
        enum CoordinateStatus
        {
            NotWitnessed, Uncommitted, Committed, ReadyToExecute, LocallyDurable, Done;
            boolean isAtMost(CoordinateStatus equalOrLessThan)
            {
                return compareTo(equalOrLessThan) <= 0;
            }
            boolean isAtLeast(CoordinateStatus equalOrGreaterThan)
            {
                return compareTo(equalOrGreaterThan) >= 0;
            }
        }

        CoordinateStatus status = NotWitnessed;
        Progress progress = NoneExpected;
        Status maxStatus;
        Ballot maxPromised;
        boolean maxPromiseHasBeenAccepted;

        Object debugInvestigating;

        void ensureAtLeast(CoordinateStatus newStatus, Progress newProgress, Command command)
        {
            Preconditions.checkState(command.owns(command.txnId().epoch, command.homeKey()));
            if (newStatus.compareTo(status) > 0)
            {
                status = newStatus;
                progress = newProgress;
            }
            updateMax(command);
        }

        void updateMax(Command command)
        {
            if (maxStatus == null || maxStatus.compareTo(command.status()) < 0)
                maxStatus = command.status();
            if (maxPromised == null || maxPromised.compareTo(command.promised()) < 0)
                maxPromised = command.promised();
            maxPromiseHasBeenAccepted |= command.accepted().equals(maxPromised);
        }

        void updateMax(CheckStatusOk ok)
        {
            // TODO: perhaps set localProgress back to Waiting if Investigating and we update anything?
            if (ok.status.logicalCompareTo(maxStatus) > 0) maxStatus = ok.status;
            if (ok.promised.compareTo(maxPromised) > 0)
            {
                maxPromised = ok.promised;
                maxPromiseHasBeenAccepted = ok.accepted.equals(ok.promised);
            }
            else if (ok.promised.equals(maxPromised))
            {
                maxPromiseHasBeenAccepted |= ok.accepted.equals(ok.promised);
            }
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
                    progress = NoneExpected;
                case Done:
            }
        }

        void update(Node node, TxnId txnId, Command command)
        {
            if (progress != NoProgress)
            {
                progress = advance(progress);
                return;
            }

            progress = Investigating;
            switch (status)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case Committed:
                case Done:
                    throw new IllegalStateException(); // NoProgressExpected

                case Uncommitted:
                case ReadyToExecute:
                {
                    if (status.isAtLeast(CoordinateStatus.Committed) && command.isGloballyPersistent())
                    {
                        // must also be committed, as at the time of writing we do not guarantee dissemination of Commit
                        // records to the home shard, so we only know the executeAt shards will have witnessed this
                        // if the home shard is at an earlier phase, it must run recovery
                        long epoch = command.executeAt().epoch;
                        node.withEpoch(epoch, () -> {
                            // TODO (now): slice the route to only those owned locally
                            debugInvestigating = checkOnCommitted(node, txnId, command.route(), epoch, epoch, (success, fail) -> {
                                // should have found enough information to apply the result, but in case we did not reset progress
                                if (progress == Investigating)
                                    progress = Expected;
                            });
                        });
                    }
                    else
                    {
                        RoutingKey homeKey = command.homeKey();
                        node.withEpoch(txnId.epoch, () -> {

                            Future<CheckStatusOk> recover = node.maybeRecover(txnId, command.homeKey(), command.route(), maxStatus, maxPromised, maxPromiseHasBeenAccepted);
                            recover.addCallback((success, fail) -> {
                                if (status.isAtMost(ReadyToExecute) && progress == Investigating)
                                {
                                    progress = Expected;
                                    if (fail != null)
                                        return;

                                    // TODO: avoid returning null (need to change semantics here in this case, though, as Recover doesn't return CheckStatusOk)
                                    if (success == null || success.hasExecutedOnAllShards)
                                    {
                                        command.setGloballyPersistent(homeKey, null);
                                        command.commandStore.progressLog().durable(txnId, homeKey, null);
                                    }

                                    if (success != null)
                                        updateMax(success);
                                }
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
            return "{" + status + ',' + progress + '}';
        }
    }

    // exists only on home shard
    static class DisseminateState
    {
        enum DisseminateStatus { NotExecuted, Durable, Done }

        // TODO: thread safety (schedule on progress log executor)
        class CoordinateAwareness implements Callback<SimpleReply>
        {
            @Override
            public void onSuccess(Id from, SimpleReply reply)
            {
                notAwareOfDurability.remove(from);
                maybeDone();
            }

            @Override
            public void onFailure(Id from, Throwable failure)
            {
            }

            @Override
            public void onCallbackFailure(Throwable failure)
            {
            }
        }

        DisseminateStatus status = NotExecuted;
        Progress progress = NoneExpected;
        Set<Id> notAwareOfDurability;
        Set<Id> notPersisted;

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

        // must know the epoch information, and have a valie Route
        private boolean maybeReady(Node node, Command command)
        {
            if (!command.hasBeen(Status.Committed))
                return false;

            if (!(command.route() instanceof Route))
                return false;

            if (!node.topology().hasEpoch(command.executeAt().epoch))
                return false;

            Topologies topology = node.topology().preciseEpochs(command.route(), command.txnId().epoch, command.executeAt().epoch);
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
            if (notAwareOfDurability.isEmpty())
            {
                status = DisseminateStatus.Done;
                progress = Done;
            }
        }

        void durableGlobal(Node node, Command command, @Nullable Set<Id> persistedOn)
        {
            if (status == DisseminateStatus.Done)
                return;

            status = Durable;
            progress = Expected;
            if (persistedOn == null)
                return;

            whenReady(node, command, () -> {
                notPersisted.removeAll(persistedOn);
                notAwareOfDurability.removeAll(persistedOn);
                maybeDone();
            });
        }

        void durableLocal(Node node, Command command)
        {
            if (status == DisseminateStatus.Done)
                return;

            status = Durable;
            progress = Expected;

            whenReady(node, command, () -> {
                notPersisted.remove(node.id());
                notAwareOfDurability.remove(node.id());
                maybeDone();
            });
        }

        void update(Node node, TxnId txnId, Command command)
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

            if (progress != NoProgress)
            {
                progress = advance(progress);
                return;
            }

            progress = Investigating;
            if (notAwareOfDurability.isEmpty())
            {
                // TODO: also track actual durability
                status = DisseminateStatus.Done;
                progress = Done;
                return;
            }

            Route route = (Route) command.route();
            Timestamp executeAt = command.executeAt();
            investigating = new CoordinateAwareness();
            Topologies topologies = node.topology().preciseEpochs(route, txnId.epoch, executeAt.epoch);
            node.send(notAwareOfDurability, to -> new InformDurable(to, topologies, route, txnId, executeAt), investigating);
        }

        @Override
        public String toString()
        {
            return "{" + status + ',' + progress + '}';
        }
    }

    static class BlockingState
    {
        Status blockedUntil = Status.NotWitnessed;
        Progress progress = NoneExpected;
        RoutingKeys someKeys;

        Object debugInvestigating;

        void recordBlocking(Command command, Status blockedUntil, RoutingKeys someKeys)
        {
            Preconditions.checkState(!someKeys.isEmpty());
            Preconditions.checkState(!command.hasBeen(blockedUntil));
            this.someKeys = someKeys;
            if (blockedUntil.compareTo(this.blockedUntil) > 0)
            {
                this.blockedUntil = blockedUntil;
                progress = Expected;
            }
        }

        void recordCommit()
        {
            if (blockedUntil == Status.Committed)
                progress = NoneExpected;
        }

        void recordExecute()
        {
            blockedUntil = Executed;
            progress = Done;
        }

        void update(Node node, TxnId txnId, Command command)
        {
            if (progress != NoProgress)
            {
                progress = advance(progress);
                return;
            }

            progress = Investigating;
            // first make sure we have enough information to obtain the command locally
            boolean canExecute = command.hasBeen(Status.Committed);
            long srcEpoch = (command.hasBeen(Status.Committed) ? command.executeAt() : txnId).epoch;
            // TODO: compute fromEpoch, the epoch we already have this txn replicated until
            long toEpoch = Math.max(srcEpoch, node.topology().epoch());
            node.withEpoch(srcEpoch, () -> {

                // first check we have enough routing information for the ranges we own; if not, fetch it
                AbstractRoute route = route(command);
                KeyRanges ranges = node.topology().localRangesForEpochs(txnId.epoch, toEpoch);
                if (route == null || !route.covers(ranges))
                {
                    Status blockedOn = this.blockedUntil;
                    BiConsumer<FindRoute.Result, Throwable> foundRoute = (findRoute, fail) -> {
                        if (progress == Investigating && blockedOn == this.blockedUntil)
                        {
                            progress = Expected;
                            if (findRoute != null && findRoute.route != null && !(someKeys instanceof Route))
                                someKeys = findRoute.route;
                            if (findRoute == null && fail == null)
                                invalidate(node, command);
                        }
                    };

                    if (command.homeKey() != null || route != null)
                    {
                        RoutingKey homeKey = route != null ? route.homeKey : command.homeKey();
                        debugInvestigating = FindRoute.findRoute(node, txnId, homeKey, foundRoute);
                    }
                    else
                    {
                        RoutingKeys someKeys = this.someKeys;
                        if (someKeys == null || someKeys.isEmpty()) someKeys = route;
                        debugInvestigating = FindHomeKey.findHomeKey(node, txnId, someKeys, (homeKey, fail) -> {
                            if (progress == Investigating && blockedOn == this.blockedUntil)
                            {
                                if (fail != null) progress = Expected;
                                else if (homeKey != null) FindRoute.findRoute(node, txnId, homeKey, foundRoute);
                                else invalidate(node, command);
                            }
                        });
                    }
                    return;
                }

                // check status with the only keys we know, if any, then:
                // 1. if we cannot find any primary record of the transaction, then it cannot be a dependency so record this fact
                // 2. otherwise record the homeKey for future reference and set the status based on whether progress has been made
                BiConsumer<CheckStatusOkFull, Throwable> callback = (success, fail) -> {
                    if (progress != Investigating)
                        return;

                    progress = Expected;
                    if (fail != null)
                        return;

                    switch (success.fullStatus)
                    {
                        default:
                            throw new IllegalStateException();
                        case Invalidated:
                        case Applied:
                        case Executed:
                            if (canExecute)
                            {
                                Preconditions.checkState(command.hasBeen(Executed));
                                blockedUntil = Executed;
                                progress = Done;
                                break;
                            }
                        case ReadyToExecute:
                        case Committed:
                            Preconditions.checkState(command.hasBeen(Status.Committed));
                            if (blockedUntil == Status.Committed)
                                progress = NoneExpected;
                            break;
                        case Accepted:
                        case AcceptedInvalidate:
                        case PreAccepted:
                        case NotWitnessed:
                            // the home shard is managing progress, give it time
                            break;
                    }
                };

                debugInvestigating = blockedUntil == Executed ? checkOnCommitted(node, txnId, route, srcEpoch, toEpoch, callback)
                                                              : checkOnUncommitted(node, txnId, route, srcEpoch, toEpoch, callback);
            });
        }

        private AbstractRoute route(Command command)
        {
            return AbstractRoute.merge(command.route(), someKeys instanceof AbstractRoute ? (AbstractRoute) someKeys : null);
        }

        private void invalidate(Node node, Command command)
        {
            progress = Investigating;
            RoutingKey someKey = command.homeKey();
            if (someKey == null) someKey = someKeys.get(0);

            RoutingKeys someKeys = route(command);
            if (someKeys == null) someKeys = this.someKeys;
            if (someKeys == null || !someKeys.contains(someKey))
                someKeys = RoutingKeys.of(someKey);
            debugInvestigating = Invalidate.invalidate(node, command.txnId(), someKeys, someKey, (success, fail) -> {
                if (progress != Investigating) return;
                if (fail != null) progress = Expected;
                else switch (success)
                {
                    default: throw new IllegalStateException();
                    case Executed:
                    case Invalidated:
                        progress = Done;
                }
            });
        }

        public String toString()
        {
            return progress.toString();
        }
    }

    enum NonHomeState
    {
        Unsafe, StillUnsafe, Investigating, Safe
    }

    static class State
    {
        final TxnId txnId;
        final Command command;

        CoordinateState coordinateState;
        DisseminateState disseminateState;
        NonHomeState nonHomeState;
        BlockingState blockingState;

        State(TxnId txnId, Command command)
        {
            this.txnId = txnId;
            this.command = command;
        }

        void recordBlocking(Command command, Status waitingFor, RoutingKeys someKeys)
        {
            Preconditions.checkArgument(command.txnId().equals(this.txnId));
            if (blockingState == null)
                blockingState = new BlockingState();
            blockingState.recordBlocking(command, waitingFor, someKeys);
        }

        void ensureAtLeast(NonHomeState ensureAtLeast)
        {
            if (nonHomeState == null || nonHomeState.compareTo(ensureAtLeast) < 0)
                nonHomeState = ensureAtLeast;
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

        void ensureAtLeast(CoordinateStatus newStatus, Progress newProgress)
        {
            local().ensureAtLeast(newStatus, newProgress, command);
        }

        void updateNonHome(Node node)
        {
            switch (nonHomeState)
            {
                default: throw new IllegalStateException();
                case Safe:
                case Investigating:
                    break;
                case Unsafe:
                    nonHomeState = StillUnsafe;
                    break;
                case StillUnsafe:
                    // make sure a quorum of the home shard is aware of the transaction, so we can rely on it to ensure progress
                    Future<Void> inform = inform(node, txnId, command.homeKey());
                    inform.addCallback((success, fail) -> {
                        if (nonHomeState == Safe)
                            return;

                        if (fail != null) nonHomeState = Unsafe;
                        else nonHomeState = Safe;
                    });
                    break;
            }
        }

        void update(Node node)
        {
            if (blockingState != null)
                blockingState.update(node, txnId, command);

            if (coordinateState != null)
                coordinateState.update(node, txnId, command);

            if (disseminateState != null)
                disseminateState.update(node, txnId, command);

            if (nonHomeState != null)
                updateNonHome(node);
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

    final Node node;
    final List<Instance> instances = new CopyOnWriteArrayList<>();

    public SimpleProgressLog(Node node)
    {
        this.node = node;
        node.scheduler().recurring(this, 200L, TimeUnit.MILLISECONDS);
    }

    class Instance implements ProgressLog
    {
        final CommandStore commandStore;
        final Map<TxnId, State> stateMap = new HashMap<>();

        Instance(CommandStore commandStore)
        {
            this.commandStore = commandStore;
            instances.add(this);
        }

        State ensure(TxnId txnId)
        {
            return stateMap.computeIfAbsent(txnId, id -> new State(id, commandStore.command(id)));
        }

        State ensure(TxnId txnId, State state)
        {
            return state != null ? state : ensure(txnId);
        }

        @Override
        public void unwitnessed(TxnId txnId, ProgressShard shard)
        {
            if (shard.isHome())
                ensure(txnId).ensureAtLeast(Uncommitted, Expected);
        }

        @Override
        public void preaccept(TxnId txnId, ProgressShard shard)
        {
            Preconditions.checkState(shard != Unsure);

            if (shard.isProgress())
            {
                State state = ensure(txnId);
                if (shard.isHome()) state.ensureAtLeast(Uncommitted, Expected);
                else state.ensureAtLeast(NonHomeState.Unsafe);
            }
        }

        State recordCommit(TxnId txnId)
        {
            State state = stateMap.get(txnId);
            if (state != null && state.blockingState != null)
                state.blockingState.recordCommit();
            return state;
        }

        State recordExecute(TxnId txnId)
        {
            State state = stateMap.get(txnId);
            if (state != null && state.blockingState != null)
                state.blockingState.recordExecute();
            return state;
        }

        private void ensureSafeOrAtLeast(TxnId txnId, ProgressShard shard, CoordinateStatus newStatus, Progress newProgress)
        {
            Preconditions.checkState(shard != Unsure);

            State state = null;
            assert newStatus.isAtMost(ReadyToExecute);
            if (newStatus.isAtLeast(CoordinateStatus.Committed))
                state = recordCommit(txnId);

            if (shard.isProgress())
            {
                state = ensure(txnId, state);

                if (shard.isHome()) state.ensureAtLeast(newStatus, newProgress);
                else ensure(txnId).ensureAtLeast(Safe);
            }
        }

        @Override
        public void accept(TxnId txnId, ProgressShard shard)
        {
            ensureSafeOrAtLeast(txnId, shard, Uncommitted, Expected);
        }

        @Override
        public void commit(TxnId txnId, ProgressShard shard)
        {
            ensureSafeOrAtLeast(txnId, shard, CoordinateStatus.Committed, NoneExpected);
        }

        @Override
        public void readyToExecute(TxnId txnId, ProgressShard shard)
        {
            ensureSafeOrAtLeast(txnId, shard, CoordinateStatus.ReadyToExecute, Expected);
        }

        @Override
        public void execute(TxnId txnId, ProgressShard shard)
        {
            recordExecute(txnId);
            // this is the home shard's state ONLY, so we don't know it is fully durable locally
            ensureSafeOrAtLeast(txnId, shard, CoordinateStatus.ReadyToExecute, Expected);
        }

        @Override
        public void invalidate(TxnId txnId, ProgressShard shard)
        {
            State state = recordExecute(txnId);

            Preconditions.checkState(shard == Home || state == null || state.coordinateState == null);

            // note: we permit Unsure here, so we check if we have any local home state
            if (shard.isProgress())
            {
                state = ensure(txnId, state);

                if (shard.isHome()) state.ensureAtLeast(CoordinateStatus.Done, Done);
                else ensure(txnId).ensureAtLeast(Safe);
            }
        }

        @Override
        public void durableLocal(TxnId txnId)
        {
            State state = ensure(txnId);
            state.global().durableLocal(node, state.command);
        }

        @Override
        public void durable(TxnId txnId, RoutingKey homeKey, @Nullable Set<Id> persistedOn)
        {
            State state = ensure(txnId);
            if (!state.command.hasBeen(Executed))
                state.recordBlocking(state.command, Executed, RoutingKeys.of(homeKey));
            state.local().durableGlobal();
            state.global().durableGlobal(node, state.command, persistedOn);
        }

        @Override
        public void durable(TxnId txnId, AbstractRoute route, ProgressShard shard)
        {
            State state = ensure(txnId);
            // TODO: we can probably simplify things by requiring (empty) Apply messages to be sent also to the coordinating topology
            if (!state.command.hasBeen(Executed))
                state.recordBlocking(state.command, Executed, route);
        }

        @Override
        public void waiting(TxnId blockedBy, Status blockedUntil, RoutingKeys someKeys)
        {
            // TODO (now): forward to progress shard, if known
            Command blockedByCommand = commandStore.command(blockedBy);
            if (!blockedByCommand.hasBeen(blockedUntil))
                ensure(blockedBy).recordBlocking(blockedByCommand, blockedUntil, someKeys);
        }
    }

    @Override
    public void run()
    {
        for (Instance instance : instances)
        {
            // TODO: we want to be able to poll others about pending dependencies to check forward progress,
            //       as we don't know all dependencies locally (or perhaps any, at execution time) so we may
            //       begin expecting forward progress too early
            instance.stateMap.values().forEach(state -> {
                try
                {
                    state.update(node);
                }
                catch (Throwable t)
                {
                    node.agent().onUncaughtException(t);
                }
            });
        }
    }

    @Override
    public ProgressLog create(CommandStore commandStore)
    {
        return new Instance(commandStore);
    }
}
