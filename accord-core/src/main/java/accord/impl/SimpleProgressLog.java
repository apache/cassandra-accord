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
import accord.coordinate.CheckOnCommitted;
import accord.coordinate.CheckShardStatus;
import accord.coordinate.Invalidate;
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
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.coordinate.CheckOnCommitted.checkOnCommitted;
import static accord.coordinate.CheckOnUncommitted.checkOnUncommitted;
import static accord.coordinate.InformHomeOfTxn.inform;
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
            boolean isAtMost(LocalStatus equalOrLessThan)
            {
                return compareTo(equalOrLessThan) <= 0;
            }
            boolean isAtLeast(LocalStatus equalOrGreaterThan)
            {
                return compareTo(equalOrGreaterThan) >= 0;
            }
        }

        enum GlobalStatus
        {
            NotExecuted, Disseminating, PendingDurable, Durable, Done; // TODO: manage propagating from Durable to everyone
            boolean isAtLeast(GlobalStatus equalOrGreaterThan)
            {
                return compareTo(equalOrGreaterThan) >= 0;
            }
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

        Object debugInvestigating;

        void ensureAtLeast(LocalStatus newStatus, Progress newProgress, Node node, Command command)
        {
            if (newStatus == Committed && global.isAtLeast(Durable) && !command.executes())
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
                if (!node.topology().hasEpoch(command.executeAt().epoch))
                    return;

                globalNotPersisted = new HashSet<>(node.topology().preciseEpochs(command.txn(), command.executeAt().epoch).nodes());
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
                if (local.isAtLeast(Committed) && !command.executes())
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
                    if (local.isAtLeast(Committed) && global.isAtLeast(PendingDurable))
                    {
                        // must also be committed, as at the time of writing we do not guarantee dissemination of Commit
                        // records to the home shard, so we only know the executeAt shards will have witnessed this
                        // if the home shard is at an earlier phase, it must run recovery
                        Key homeKey = command.homeKey();
                        long homeEpoch = command.executeAt().epoch;

                        node.withEpoch(homeEpoch, () -> {
                            Shard homeShard = node.topology().forEpoch(homeKey, homeEpoch);
                            debugInvestigating = checkOnCommitted(node, txnId, homeKey, homeShard, command.executeAt().epoch)
                                                 .addCallback((success, fail) -> {
                                                     // should have found enough information to apply the result, but in case we did not reset progress
                                                     if (localProgress == Investigating)
                                                         localProgress = Expected;
                                                 });
                        });
                    }
                    else
                    {
                        Key homeKey = command.homeKey();
                        long homeEpoch = (local.isAtMost(Uncommitted) ? txnId : command.executeAt()).epoch;

                        node.withEpoch(homeEpoch, () -> {
                            Shard homeShard = node.topology().forEpoch(homeKey, homeEpoch);

                            Future<CheckStatusOk> recover = node.maybeRecover(txnId, command.txn(),
                                                                              homeKey, homeShard, homeEpoch,
                                                                              maxStatus, maxPromised, maxPromiseHasBeenAccepted);
                            recover.addCallback((success, fail) -> {
                                if (local.isAtMost(ReadyToExecute) && localProgress == Investigating)
                                {
                                    localProgress = Expected;
                                    if (fail != null)
                                        return;

                                    if (success == null || success.hasExecutedOnAllShards)
                                        executedOnAllShards(node, command, null);
                                    else
                                        updateMax(success);
                                }
                            });

                            debugInvestigating = recover;
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
        Keys someKeys;
        Command blocking;

        Object debugInvestigating;

        void recordBlocking(Command blocking, Keys someKeys)
        {
            this.blocking = blocking;
            switch (blocking.status())
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case AcceptedInvalidate:
                    this.someKeys = someKeys;
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
                case Invalidated:
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
            long onEpoch = (command.hasBeen(Status.Committed) ? command.executeAt() : txnId).epoch;
            node.withEpoch(onEpoch, () -> {
                Keys someKeys = this.someKeys == null ? command.someKeys() : this.someKeys;
                Key someKey = this.someKeys == null ? command.someKey() : someKeys.get(0);

                Shard someShard = node.topology().forEpoch(someKey, onEpoch);
                CheckOnCommitted check = blockedOn == Executed ? checkOnCommitted(node, txnId, someKey, someShard, onEpoch)
                                                               : checkOnUncommitted(node, txnId, someKeys, someKey, someShard, onEpoch);
                debugInvestigating = check;
                check.addCallback((success, fail) -> {
                    if (progress != Investigating)
                        return;

                    progress = Expected;
                    if (fail != null)
                        return;

                    switch (success.status)
                    {
                        default: throw new IllegalStateException();
                        case NotWitnessed:
                            progress = Investigating;
                            // TODO: this should instead invalidate the transaction on this shard, which invalidates it for all shards,
                            //       but we need to first support invalidation
                            debugInvestigating = Invalidate.invalidate(node, txnId, someKeys, someKey)
                                      .addCallback((success2, fail2) -> {
                                          if (progress != Investigating) return;
                                          if (fail2 != null) progress = Expected;
                                          else switch (success2)
                                          {
                                              default: throw new IllegalStateException();
                                              case PREEMPTED:
                                                  progress = Expected;
                                                  break;
                                              case EXECUTED:
                                              case INVALIDATED:
                                                  progress = Done;
                                          }
                                      });
                            break;
                        case PreAccepted:
                        case Accepted:
                        case AcceptedInvalidate:
                            break;
                        case Committed:
                        case ReadyToExecute:
                            Preconditions.checkState(command.hasBeen(Status.Committed) || !command.commandStore.ranges().intersects(txnId.epoch, someKeys));
                            if (blockedOn == Status.Committed)
                                progress = NoneExpected;
                            break;
                        case Executed:
                        case Applied:
                        case Invalidated:
                            progress = Done;
                    }
                });
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

        void recordBlocking(Command blockedByCommand, Keys someKeys)
        {
            Preconditions.checkArgument(blockedByCommand.txnId().equals(txnId));
            if (blockingState == null)
                blockingState = new BlockingState();
            blockingState.recordBlocking(blockedByCommand, someKeys);
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
            assert newStatus.isAtMost(ReadyToExecute);
            if (newStatus.isAtLeast(LocalStatus.Committed))
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
        public void invalidate(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            State state = recordExecute(txnId);

            if (isProgressShard)
            {
                state = ensure(txnId, state);

                if (isHomeShard) state.ensureAtLeast(LocalStatus.Done, Done, node);
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
        public void waiting(TxnId blockedBy, Keys someKeys)
        {
            Command blockedByCommand = commandStore.command(blockedBy);
            if (!blockedByCommand.hasBeen(Executed))
                ensure(blockedBy).recordBlocking(blockedByCommand, someKeys);
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
            Topologies topologies = node.topology().preciseEpochs(command.txn(), command.executeAt().epoch);
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
        public void onFailure(Id from, Throwable failure)
        {
            if (waitingOnResponses.remove(from) && waitingOnResponses.isEmpty())
                trySuccess(null);
        }

        @Override
        public void onCallbackFailure(Throwable failure)
        {
            tryFailure(failure);
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
                    if (state.homeState.globalNotPersisted != null)
                    {
                        state.homeState.globalNotPersisted.retainAll(notPersisted);
                        return new ApplyAndCheckOk(state.homeState.globalNotPersisted);
                    }
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
