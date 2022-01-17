package accord.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.coordinate.CheckShardStatus;
import accord.impl.SimpleProgressLog.HomeState.LocalStatus;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.Apply;
import accord.messages.Callback;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.coordinate.CheckOnCommitted.checkOnCommitted;
import static accord.coordinate.CheckOnUncommitted.checkOnUncommitted;
import static accord.coordinate.InformHomeOfTxn.inform;
import static accord.coordinate.MaybeRecover.maybeRecover;
import static accord.impl.SimpleProgressLog.CoordinateApplyAndCheck.applyAndCheck;
import static accord.impl.SimpleProgressLog.HomeState.GlobalStatus.Durable;
import static accord.impl.SimpleProgressLog.HomeState.GlobalStatus.Disseminating;
import static accord.impl.SimpleProgressLog.HomeState.GlobalStatus.NotExecuted;
import static accord.impl.SimpleProgressLog.HomeState.GlobalStatus.PendingDurable;
import static accord.impl.SimpleProgressLog.HomeState.LocalStatus.Committed;
import static accord.impl.SimpleProgressLog.HomeState.LocalStatus.ReadyToExecute;
import static accord.impl.SimpleProgressLog.HomeState.LocalStatus.Uncommitted;
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

    static class GlobalPendingDurable
    {
        final Set<Id> persistedOn;

        GlobalPendingDurable(Set<Id> persistedOn)
        {
            this.persistedOn = persistedOn;
        }
    }

    static class HomeState
    {
        enum LocalStatus
        {
            NotWitnessed, Uncommitted, Committed, ReadyToExecute, Done;
        }

        enum GlobalStatus
        {
            NotExecuted, Disseminating, PendingDurable, Durable, Done // TODO: manage propagating from Durable to everyone
        }

        LocalStatus local = LocalStatus.NotWitnessed;
        Progress localProgress = NoneExpected;
        Status maxStatus;
        Ballot maxPromised;
        boolean maxPromiseHasBeenAccepted;

        GlobalStatus global = NotExecuted;
        Progress globalProgress = NoneExpected;
        Set<Id> globalNotPersisted;
        GlobalPendingDurable globalPendingDurable;


        void ensureAtLeast(LocalStatus newStatus, Progress newProgress, Node node, Command command)
        {
            if (newStatus == Committed && global.compareTo(Durable) >= 0 && !command.executes())
            {
                local = LocalStatus.Done;
                localProgress = Done;
            }
            else if (newStatus.compareTo(local) > 0)
            {
                local = newStatus;
                localProgress = newProgress;
            }
            refreshGlobal(node, command, null, null);
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
            if (ok.status.compareTo(maxStatus) > 0) maxStatus = ok.status;
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

        private void refreshGlobal(@Nullable Node node, @Nullable Command command, @Nullable Id persistedOn, @Nullable Set<Id> persistedOns)
        {
            if (global == NotExecuted)
                return;

            if (globalPendingDurable != null)
            {
                if (node == null || command == null || command.is(Status.NotWitnessed))
                    return;

                if (persistedOns == null) persistedOns = globalPendingDurable.persistedOn;
                else persistedOns.addAll(globalPendingDurable.persistedOn);

                global = Durable;
                globalProgress = Expected;
            }

            if (globalNotPersisted == null)
            {
                assert node != null && command != null;
                globalNotPersisted = new HashSet<>(node.topology().unsyncForTxn(command.txn(), command.executeAt().epoch).nodes());
                if (local == LocalStatus.Done)
                    globalNotPersisted.remove(node.id());
            }
            if (globalNotPersisted != null)
            {
                if (persistedOn != null)
                    globalNotPersisted.remove(persistedOn);
                if (persistedOns != null)
                    globalNotPersisted.removeAll(persistedOns);

                if (globalNotPersisted.isEmpty())
                {
                    global = GlobalStatus.Done;
                    globalProgress = Done;
                }
            }
        }

        void executedOnAllShards(Node node, Command command, Set<Id> persistedOn)
        {
            if (local == LocalStatus.NotWitnessed)
            {
                global = PendingDurable;
                globalProgress = NoneExpected;
                globalPendingDurable = new GlobalPendingDurable(persistedOn);
            }
            else if (global != GlobalStatus.Done)
            {
                global = Durable;
                globalProgress = Expected;
                refreshGlobal(node, command, null, persistedOn);
                if (local.compareTo(Committed) >= 0 && !command.executes())
                {
                    local = LocalStatus.Done;
                    localProgress = Done;
                }
            }
        }

        void executed(Node node, Command command)
        {
            switch (local)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case Uncommitted:
                case Committed:
                case ReadyToExecute:
                    local = LocalStatus.Done;
                    localProgress = NoneExpected;
                    if (global == NotExecuted)
                    {
                        global = Disseminating;
                        globalProgress = Expected;
                    }
                    refreshGlobal(node, command, node.id(), null);
                case Done:
            }
        }

        void updateLocal(Node node, TxnId txnId, Command command)
        {
            if (localProgress != NoProgress)
            {
                localProgress = advance(localProgress);
                return;
            }

            localProgress = Investigating;
            switch (local)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case Committed:
                    throw new IllegalStateException(); // NoProgressExpected

                case Uncommitted:
                case ReadyToExecute:
                {
                    Key homeKey = command.homeKey();
                    long homeEpoch = (local.compareTo(Uncommitted) <= 0 ? txnId : command.executeAt()).epoch;
                    Shard homeShard = node.topology().forEpochIfKnown(homeKey, homeEpoch);
                    if (homeShard == null)
                    {
                        node.configService().fetchTopologyForEpoch(homeEpoch);
                        localProgress = Expected;
                        break;
                    }
                    if (local.compareTo(Committed) >= 0 && global.compareTo(Durable) >= 0)
                    {
                        checkOnCommitted(node, txnId, command.txn(), homeKey, homeShard, homeEpoch)
                        .addCallback((success, fail) -> {
                            // should have found enough information to apply the result, but in case we did not reset progress
                            if (localProgress.compareTo(Done) < 0 && localProgress == Investigating)
                                localProgress = Expected;
                        });
                    }
                    else
                    {

                        Future<CheckStatusOk> recover = maybeRecover(node, txnId, command.txn(),
                                                                              homeKey, homeShard, homeEpoch,
                                                                              maxStatus, maxPromised, maxPromiseHasBeenAccepted);
                        recover.addCallback((success, fail) -> {
                            if (local.compareTo(ReadyToExecute) <= 0 && localProgress == Investigating)
                            {
                                if (fail == null && success == null)
                                {
                                    // we have globally persisted the result, so move to waiting for the result to be fully replicated amongst our shards
                                    executedOnAllShards(node, command, null);
                                }
                                else
                                {
                                    localProgress = Expected;
                                    if (success != null)
                                        updateMax(success);
                                }
                            }
                        });
                    }
                }
                case Done:
            }
        }

        void updateGlobal(Node node, TxnId txnId, Command command)
        {
            refreshGlobal(node, command, null, null);

            if (global != Disseminating)
                return;

            if (!command.hasBeen(Executed))
                return;

            if (globalProgress != NoProgress)
            {
                globalProgress = advance(globalProgress);
                return;
            }

            globalProgress = Investigating;
            applyAndCheck(node, txnId, command, this).addCallback((success, fail) -> {
                if (globalProgress != Done)
                    globalProgress = Expected;
            });
        }

        void update(Node node, TxnId txnId, Command command)
        {
            updateLocal(node, txnId, command);
            updateGlobal(node, txnId, command);
        }

        @Override
        public String toString()
        {
            return "{" + local + ',' + localProgress + ',' + global + ',' + globalProgress + '}';
        }
    }

    static class BlockingState
    {
        Status blockedOn = Status.NotWitnessed;
        Progress progress = NoneExpected;
        Key homeKey;

        void recordBlocking(Command blocking, Key homeKey)
        {
            Preconditions.checkState(homeKey != null);
            this.homeKey = homeKey;
            switch (blocking.status())
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                    blockedOn = Status.Committed;
                    progress = Expected;
                    break;
                case Committed:
                case ReadyToExecute:
                    blockedOn = Executed;
                    progress = Expected;
                    break;
                case Executed:
                case Applied:
                    throw new IllegalStateException("Should not be recorded as blocked if result already recorded locally");
            }
        }

        void recordCommit()
        {
            if (blockedOn == Status.Committed)
                progress = NoneExpected;
        }

        void recordExecute()
        {
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
            // check status with the only keys we know, if any, then:
            // 1. if we cannot find any primary record of the transaction, then it cannot be a dependency so record this fact
            // 2. otherwise record the homeKey for future reference and set the status based on whether progress has been made
            Key someKey = command.someKey();
            Shard someShard = node.topology().forEpochIfKnown(someKey, txnId.epoch);
            if (someShard == null)
            {
                node.configService().fetchTopologyForEpoch(txnId.epoch);
                progress = Expected;
                return;
            }
            // TODO (now): if we have a quorum of PreAccept and we have contacted the home shard then we can set NonHomeSafe
            CheckShardStatus check = blockedOn == Executed ? checkOnCommitted(node, txnId, command.txn(), someKey, someShard, txnId.epoch)
                                                           : checkOnUncommitted(node, txnId, command.txn(), someKey, someShard, txnId.epoch);

            check.addCallback((success, fail) -> {
                if (fail != null)
                {
                    if (progress == Investigating)
                        progress = Expected;
                    return;
                }
                switch (success.status)
                {
                    default: throw new IllegalStateException();
                    case NotWitnessed:
                        // TODO: this should instead invalidate the transaction on this shard, which invalidates it for all shards,
                        //       but we need to first support invalidation
                        inform(node, txnId, command.txn(), homeKey);
                        progress = Expected;
                        break;
                    case PreAccepted:
                    case Accepted:
                        break;
                    case Committed:
                    case ReadyToExecute:
                        if (blockedOn == Status.Committed)
                            progress = NoneExpected;
                        break;
                    case Executed:
                    case Applied:
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

        HomeState homeState;
        NonHomeState nonHomeState;
        BlockingState blockingState;

        State(TxnId txnId, Command command)
        {
            this.txnId = txnId;
            this.command = command;
        }

        void recordBlocking(Command blockedByCommand, Key homeKey)
        {
            Preconditions.checkArgument(blockedByCommand.txnId().equals(txnId));
            if (blockingState == null)
                blockingState = new BlockingState();
            blockingState.recordBlocking(blockedByCommand, homeKey);
        }

        void ensureAtLeast(NonHomeState ensureAtLeast)
        {
            if (nonHomeState == null || nonHomeState.compareTo(ensureAtLeast) < 0)
                nonHomeState = ensureAtLeast;
        }

        HomeState home()
        {
            if (homeState == null)
                homeState = new HomeState();
            return homeState;
        }

        void ensureAtLeast(LocalStatus newStatus, Progress newProgress, Node node)
        {
            home().ensureAtLeast(newStatus, newProgress, node, command);
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
                    Future<Void> inform = inform(node, txnId, command.txn(), command.homeKey());
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

            if (homeState != null)
                homeState.update(node, txnId, command);

            if (nonHomeState != null)
                updateNonHome(node);
        }

        @Override
        public String toString()
        {
            return homeState != null ? homeState.toString()
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
        public void preaccept(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            if (isProgressShard)
            {
                State state = ensure(txnId);
                if (isHomeShard) state.ensureAtLeast(Uncommitted, Expected, node);
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

        private void ensureSafeOrAtLeast(TxnId txnId, boolean isProgressShard, boolean isHomeShard, LocalStatus newStatus, Progress newProgress)
        {
            State state = null;
            assert newStatus.compareTo(ReadyToExecute) <= 0;
            if (newStatus.compareTo(LocalStatus.Committed) >= 0)
                state = recordCommit(txnId);

            if (isProgressShard)
            {
                state = ensure(txnId, state);

                if (isHomeShard) state.ensureAtLeast(newStatus, newProgress, node);
                else ensure(txnId).ensureAtLeast(Safe);
            }
        }

        @Override
        public void accept(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            ensureSafeOrAtLeast(txnId, isProgressShard, isHomeShard, Uncommitted, Expected);
        }

        @Override
        public void commit(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            ensureSafeOrAtLeast(txnId, isProgressShard, isHomeShard, LocalStatus.Committed, NoneExpected);
        }

        @Override
        public void readyToExecute(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            ensureSafeOrAtLeast(txnId, isProgressShard, isHomeShard, LocalStatus.ReadyToExecute, Expected);
        }

        @Override
        public void execute(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            State state = recordExecute(txnId);

            if (isProgressShard)
            {
                state = ensure(txnId, state);

                if (isHomeShard) state.home().executed(node, state.command);
                else ensure(txnId).ensureAtLeast(Safe);
            }
        }

        @Override
        public void executedOnAllShards(TxnId txnId, Set<Id> persistedOn)
        {
            State state = ensure(txnId);
            state.home().executedOnAllShards(node, state.command, persistedOn);
        }

        @Override
        public void waiting(TxnId blockedBy, Key homeKey)
        {
            Command blockedByCommand = commandStore.command(blockedBy);
            if (!blockedByCommand.hasBeen(Executed))
                ensure(blockedBy).recordBlocking(blockedByCommand, homeKey);
        }
    }

    @Override
    public void run()
    {
        for (Instance instance : instances)
        {
            // TODO (now): we need to be able to poll others about pending dependencies to check forward progress,
            //             as we don't know all dependencies locally (or perhaps any, at execution time)
            instance.stateMap.values().forEach(state -> state.update(node));
        }
    }

    static class CoordinateApplyAndCheck extends AsyncFuture<Void> implements Callback<ApplyAndCheckOk>
    {
        final TxnId txnId;
        final Command command;
        final HomeState state;
        final Set<Id> waitingOnResponses;

        static Future<Void> applyAndCheck(Node node, TxnId txnId, Command command, HomeState state)
        {
            CoordinateApplyAndCheck coordinate = new CoordinateApplyAndCheck(txnId, command, state);
            // TODO (now): whether we need to send to future shards depends on sync logic
            Topologies topologies = node.topology().unsyncForTxn(command.txn(), command.executeAt().epoch);
            state.globalNotPersisted.retainAll(topologies.nodes()); // we might have had some nodes from older shards that are now redundant
            node.send(state.globalNotPersisted, id -> new ApplyAndCheck(id, topologies,
                                                                        command.txnId(), command.txn(), command.homeKey(),
                                                                        command.savedDeps(), command.executeAt(),
                                                                        command.writes(), command.result(),
                                                                        state.globalNotPersisted),
                      coordinate);
            return coordinate;
        }

        CoordinateApplyAndCheck(TxnId txnId, Command command, HomeState state)
        {
            this.txnId = txnId;
            this.command = command;
            this.state = state;
            this.waitingOnResponses = new HashSet<>(state.globalNotPersisted);
        }

        @Override
        public void onSuccess(Id from, ApplyAndCheckOk response)
        {
            state.globalNotPersisted.retainAll(response.notPersisted);
            state.refreshGlobal(null, null, null, null);
        }

        @Override
        public void onFailure(Id from, Throwable throwable)
        {
            if (waitingOnResponses.remove(from) && waitingOnResponses.isEmpty())
                trySuccess(null);
        }
    }

    static class ApplyAndCheck extends Apply
    {
        final Set<Id> notPersisted;
        ApplyAndCheck(Id id, Topologies topologies, TxnId txnId, Txn txn, Key homeKey, Dependencies deps, Timestamp executeAt, Writes writes, Result result, Set<Id> notPersisted)
        {
            super(id, topologies, txnId, txn, homeKey, executeAt, deps, writes, result);
            this.notPersisted = notPersisted;
        }

        @Override
        public void process(Node node, Id from, ReplyContext replyContext)
        {
            Key progressKey = node.trySelectProgressKey(txnId, txn.keys, homeKey);
            node.reply(from, replyContext, node.mapReduceLocalSince(scope(), executeAt, instance -> {
                Command command = instance.command(txnId);
                command.apply(txn, homeKey, progressKey, executeAt, deps, writes, result);
                if (homeKey.equals(progressKey) && command.handles(txnId.epoch, progressKey))
                {
                    SimpleProgressLog.Instance log = ((SimpleProgressLog.Instance)instance.progressLog());
                    State state = log.stateMap.get(txnId);
                    state.homeState.globalNotPersisted.retainAll(notPersisted);
                    return new ApplyAndCheckOk(state.homeState.globalNotPersisted);
                }
                notPersisted.remove(node.id());
                return new ApplyAndCheckOk(notPersisted);
            }, (a, b) -> {
                if (a == null) return b;
                if (b == null) return a;
                a.notPersisted.retainAll(b.notPersisted);
                return a;
            }));
        }

        @Override
        public MessageType type()
        {
            return MessageType.APPLY_AND_CHECK_REQ;
        }

        @Override
        public String toString()
        {
            return "SendAndCheck{" +
                   "txnId:" + txnId +
                   ", txn:" + txn +
                   ", deps:" + deps +
                   ", executeAt:" + executeAt +
                   ", writes:" + writes +
                   ", result:" + result +
                   ", waitingOn:" + notPersisted +
                   '}';
        }
    }

    static class ApplyAndCheckOk implements Reply
    {
        final Set<Id> notPersisted;

        ApplyAndCheckOk(Set<Id> notPersisted)
        {
            this.notPersisted = notPersisted;
        }

        @Override
        public String toString()
        {
            return "SendAndCheckOk{" +
                   "waitingOn:" + notPersisted +
                   '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.APPLY_AND_CHECK_RSP;
        }
    }

    @Override
    public ProgressLog create(CommandStore commandStore)
    {
        return new Instance(commandStore);
    }
}
