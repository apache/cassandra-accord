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

package accord.local.cfk;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.Data;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Update;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultLocalListeners.DefaultNotifySink;
import accord.impl.DefaultRemoteListeners;
import accord.impl.IntKey;
import accord.impl.SafeTimestampsForKey;
import accord.local.Command;
import accord.local.Command.AbstractCommand;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.CommonAttributes;
import accord.local.Node;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.RangeDeps;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.messages.ReplyContext;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Route;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.DefaultRandom;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResults;

import static accord.local.Command.NotDefined.notDefined;
import static accord.local.cfk.UpdateUnmanagedMode.REGISTER;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Status.Durability.NotDurable;

// TODO (expected): test setting redundant before
// TODO (expected): test ballot updates
// TODO (expected): test transition to Erased
// TODO (expected): ensure execution is not too early
// TODO (expected): validate mapReduce
// TODO (expected): insert linearizability violations and detect them
public class CommandsForKeyTest
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsForKeyTest.class);
    private static final RoutingKey KEY = IntKey.routing(1);
    private static final Range RANGE = IntKey.range(0, 2);
    private static final Keys KEYS = IntKey.keys(1);
    private static final Ranges RANGES = Ranges.of(IntKey.range(0, 2));
    private static final Txn KEY_TXN = new Txn.InMemory(KEYS, new TestRead(KEYS), new TestQuery());
    private static final Txn RANGE_TXN = new Txn.InMemory(RANGES, new TestRead(RANGES), new TestQuery());
    private static final FullRoute KEY_ROUTE = KEYS.toRoute(IntKey.routing(1));
    private static final FullRoute RANGE_ROUTE = RANGES.toRoute(IntKey.routing(1));

    static class CommandUpdate
    {
        final Command prev, next;

        CommandUpdate(Command prev, Command next)
        {
            this.prev = prev;
            this.next = next;
        }
    }

    // TODO (expected): randomise ballots
    static class Canon implements NotifySink
    {
        private static final TxnId MIN = new TxnId(1, 1, Txn.Kind.Read, Key, new Node.Id(1));

        // TODO (expected): randomise ratios
        static final Txn.Kind[] KINDS = new Txn.Kind[] { Txn.Kind.Read, Txn.Kind.Write, Txn.Kind.EphemeralRead, Txn.Kind.SyncPoint, Txn.Kind.ExclusiveSyncPoint };
        final RandomSource rnd;
        final Node.Id[] nodeIds;
        final Domain[] domains;
        final TreeSet<TxnId> unwitnessed = new TreeSet<>();
        final TreeSet<TxnId> undecided = new TreeSet<>();
        final TreeSet<TxnId> candidates = new TreeSet<>();
        final TreeSet<TxnId> unfinished = new TreeSet<>();
        final TreeMap<Timestamp, Command> byId = new TreeMap<>();
        final TreeMap<Timestamp, Command> committedByExecuteAt = new TreeMap<>();
        final Set<Timestamp> executeAts = new HashSet<>();
        boolean closing;
        int undecidedCount;

        static final EnumMap<SaveStatus, SaveStatus[]> TRANSITIONS = new EnumMap<>(SaveStatus.class);

        void set(Command prev, Command next)
        {
            byId.put(next.txnId(), next);
            if (next.hasBeen(Status.Committed))
            {
                undecided.remove(next.txnId());
                committedByExecuteAt.put(next.executeAt(), next);
                if (next.hasBeen(Status.Stable))
                {
                    if (prev.saveStatus() != next.saveStatus())
                        candidates.remove(next.txnId());

                    if (next.hasBeen(Status.Applied))
                    {
                        unfinished.remove(next.txnId());
                        if (!CommandsForKey.managesExecution(next.txnId()))
                            removeWaitingOn(next.txnId(), Timestamp.MAX);
                    }
                    else
                    {
                        Command.Committed committed = next.asCommitted();
                        if (!committed.isWaitingOnDependency() && (!prev.hasBeen(Status.Stable) || prev.asCommitted().isWaitingOnDependency()))
                            readyToExecute(committed);
                    }
                }
                if (next.hasBeen(Status.Committed) && !prev.hasBeen(Status.Committed) && !next.hasBeen(Status.Invalidated))
                {
                    if (!next.executeAt().equals(next.txnId()) && !CommandsForKey.manages(next.txnId()))
                        removeWaitingOn(next.txnId(), next.executeAt());
                }

            }
        }

        private void readyToExecute(Command.Committed committed)
        {
            for (Command pred : committedByExecuteAt.headMap(committed.executeAt(), false).values())
                Invariants.checkState(pred.hasBeen(Status.Applied) || !committed.txnId().kind().witnesses(pred.txnId()));
            candidates.add(committed.txnId());
        }

        private void removeWaitingOn(TxnId waitingId, Timestamp until)
        {
            for (Command command : new ArrayList<>(committedByExecuteAt.subMap(waitingId, false, until, false).values()))
            {
                if (!command.hasBeen(Status.Stable))
                    continue;

                Command.Committed committed = command.asCommitted();
                Command.WaitingOn waitingOn = committed.waitingOn;
                if (waitingOn.isWaitingOn(waitingId))
                {
                    Command.WaitingOn.Update update = new Command.WaitingOn.Update(waitingOn);
                    update.removeWaitingOn(waitingId);
                    set(committed, Command.Committed.committed(committed, committed, update.build()));
                }
            }
        }

        @Override
        public void notWaiting(SafeCommandStore safeStore, TxnId txnId, RoutingKey key)
        {
            Command.Committed prev = byId.get(txnId).asCommitted();
            Command.WaitingOn.Update waitingOn = new Command.WaitingOn.Update(prev.waitingOn);
            if (!waitingOn.removeWaitingOn(key))
                return;

            if (!prev.txnId().kind().awaitsOnlyDeps())
            {
                for (Command command : committedByExecuteAt.headMap(prev.executeAt(), false).values())
                    Invariants.checkState(command.txnId().domain() == Domain.Range || !prev.txnId().kind().witnesses(command.txnId()) || command.saveStatus().compareTo(SaveStatus.Applied) >= 0);
            }

            if (prev.txnId().domain() == Key)
            {
                for (Command command : committedByExecuteAt.tailMap(prev.executeAt(), false).values())
                    Invariants.checkState(command.txnId().kind().awaitsOnlyDeps() || !command.txnId().kind().witnesses(prev.txnId()) || command.saveStatus().compareTo(SaveStatus.Stable) < 0 || command.asCommitted().waitingOn.isWaitingOnKey(0));
            }

            Command.Committed next = Command.Committed.committed(prev, prev, waitingOn.build());
            set(prev, next);
        }

        @Override
        public void waitingOn(SafeCommandStore safeStore, TxnInfo txn, RoutingKey key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk)
        {
        }

        static
        {
            TRANSITIONS.put(SaveStatus.NotDefined, new SaveStatus[] { SaveStatus.PreAccepted, SaveStatus.AcceptedInvalidate, SaveStatus.AcceptedInvalidateWithDefinition, SaveStatus.Accepted, SaveStatus.AcceptedWithDefinition, SaveStatus.Committed, SaveStatus.Stable, SaveStatus.Invalidated });
            TRANSITIONS.put(SaveStatus.PreAccepted, new SaveStatus[] { SaveStatus.AcceptedInvalidateWithDefinition, SaveStatus.AcceptedWithDefinition, SaveStatus.Committed, SaveStatus.Stable, SaveStatus.Invalidated });
            // permit updated ballot and moving to other statuses
            TRANSITIONS.put(SaveStatus.AcceptedInvalidate, new SaveStatus[] { SaveStatus.Invalidated });
            TRANSITIONS.put(SaveStatus.AcceptedInvalidateWithDefinition, new SaveStatus[] { SaveStatus.Invalidated });
            TRANSITIONS.put(SaveStatus.Accepted, new SaveStatus[] { SaveStatus.Committed, SaveStatus.Stable, SaveStatus.Invalidated });
            TRANSITIONS.put(SaveStatus.AcceptedWithDefinition, new SaveStatus[] { SaveStatus.Committed, SaveStatus.Stable, SaveStatus.Invalidated });
            TRANSITIONS.put(SaveStatus.Committed, new SaveStatus[] { SaveStatus.Stable });
            TRANSITIONS.put(SaveStatus.Stable, new SaveStatus[] { SaveStatus.Applied });
        }

        Canon(RandomSource rnd)
        {
            this.rnd = rnd;
            this.nodeIds = new Node.Id[10];
            for (int i = 0 ; i < nodeIds.length ; ++i)
                nodeIds[i] = new Node.Id(i + 1);
            this.domains = Domain.values();
        }

        Canon(RandomSource rnd, Node.Id[] nodeIds)
        {
            this.rnd = rnd;
            this.nodeIds = nodeIds;
            this.domains = Domain.values();
        }

        boolean isDone()
        {
            return closing && unfinished.size() == 0;
        }

        void close()
        {
            closing = true;
        }

        CommandUpdate update(boolean hasWaitingTasks)
        {
            boolean generate = !closing && rnd.decide(1 / (1f + undecidedCount));
            if (!generate && candidates.isEmpty() && hasWaitingTasks)
                return null;
            Invariants.checkArgument (!candidates.isEmpty() || (unfinished.size() == unwitnessed.size()));
            Command prev = generate || candidates.isEmpty() ? unwitnessed(generateId()) : selectOne(candidates);
            boolean invalidate = false;
            if (prev.txnId().kind() == Txn.Kind.ExclusiveSyncPoint && !prev.hasBeen(Status.Committed))
            {
                // may need to invalidate
                if (byId.tailMap(prev.txnId(), false).values().stream().anyMatch(c -> c.hasBeen(Status.Committed) && c.txnId().kind().witnesses(prev.txnId())))
                    invalidate = true;
            }
            Command next = invalidate ? AbstractCommand.validate((AbstractCommand)update(prev, SaveStatus.Invalidated))
                                      : AbstractCommand.validate((AbstractCommand)update(prev));
            set(prev, next);
            unwitnessed.remove(next.txnId());
            return new CommandUpdate(prev, next);
        }

        private Command update(Command prev)
        {
            SaveStatus[] candidates = TRANSITIONS.get(prev.saveStatus());
            return update(prev, candidates[rnd.nextInt(candidates.length)]);
        }

        private Command update(Command prev, SaveStatus newStatus)
        {
            switch (newStatus)
            {
                default:
                case NotDefined:
                case TruncatedApply:
                case TruncatedApplyWithDeps:
                case TruncatedApplyWithOutcome:
                case PreApplied:
                case PreCommitted:
                    throw new AssertionError();

                case PreAccepted:
                    return preaccepted(prev.txnId());

                case Accepted:
                case AcceptedWithDefinition:
                    return accepted(prev.txnId(), generateExecuteAt(prev.txnId()), newStatus);

                case AcceptedInvalidate:
                case AcceptedInvalidateWithDefinition:
                    return acceptedInvalidated(prev.txnId(), newStatus);

                case Committed:
                    return committed(prev.txnId(), prev.executeAtIfKnown(generateExecuteAt(prev.txnId())));

                case Stable:
                    return stable(prev.txnId(), prev.executeAtIfKnown(generateExecuteAt(prev.txnId())), prev.hasBeen(Status.Committed) ? prev.asCommitted() : null);

                case Applied:
                    return applied(prev.txnId(), prev.executeAtIfKnown(generateExecuteAt(prev.txnId())), prev.hasBeen(Status.Committed) ? prev.asCommitted() : null);

                case Invalidated:
                    return invalidated(prev.txnId());
            }
        }

        private Deps generateDeps(TxnId txnId, Timestamp executeAt, Status forStatus)
        {
            maybeGenerateUnwitnessed();
            try (Deps.Builder builder = new Deps.Builder())
            {
                for (Command command : byId.headMap(executeAt, false).values())
                {
                    if (txnId.equals(command.txnId())) continue;
                    if (!txnId.kind().witnesses(command.txnId())) continue;

                    if (command.hasBeen(forStatus.compareTo(Status.Accepted) <= 0 ? Status.Committed : Status.Accepted) || rnd.nextBoolean())
                    {
                        unwitnessed.remove(command.txnId());
                        builder.add(command.txnId().domain() == Key ? KEY : RANGE, command.txnId());
                    }
                }
                return builder.build();
            }
        }

        private Timestamp generateExecuteAt(TxnId txnId)
        {
            if (txnId.kind().awaitsOnlyDeps())
                return txnId;

            Timestamp min = Timestamp.NONE;
            if (!committedByExecuteAt.isEmpty())
            {
                min = committedByExecuteAt.lastEntry().getValue().executeAt();
                min = Timestamp.fromValues(min.epoch(), min.hlc() + 1, min.node);
            }

            if (min.compareTo(txnId) <= 0 && rnd.nextBoolean())
                return txnId;

            min = Timestamp.max(min, Timestamp.fromValues(txnId.epoch(), txnId.hlc() + 1, txnId.node));
            Timestamp max = Timestamp.fromValues(Math.min(100, min.epoch() * 2), min.hlc() + 100, min.node);

            Timestamp executeAt = generateTimestamp(min, max, true);
            Invariants.checkState(executeAt.compareTo(txnId) >= 0);
            executeAts.add(executeAt);
            return executeAt;
        }

        private void maybeGenerateUnwitnessed()
        {
            int transitiveCount = rnd.decide(1 / (1f + unwitnessed.size())) ? rnd.nextInt(0, 3) : 0;
            while (transitiveCount-- > 0)
            {
                // generate new unwitnessed transactions
                TxnId next = generateId();
                byId.put(next, unwitnessed(next));
            }
        }

        TxnId generateId()
        {
            TxnId min = MIN;
            TxnId max;
            if (byId.isEmpty()) max = new TxnId(1, 100, Txn.Kind.Read, Key, nodeIds[0]);
            else
            {
                max = byId.lastEntry().getValue().txnId();
                switch (rnd.nextInt(3))
                {
                    default: throw new AssertionError();
                    case 2:
                        min = max;
                    case 1:
                        max = new TxnId(Math.min(100, max.epoch() * 2), max.hlc() + 100, max.kind(), max.domain(), max.node);
                    case 0:

                }
            }
            return generateId(min, max, true);
        }

        TxnId generateId(TxnId min, TxnId max, boolean unique)
        {
            TxnId result = generateId(min, max);
            while (unique && (byId.containsKey(result) || executeAts.contains(result)))
                result = generateId(min, max);
            return result;
        }

        TxnId generateId(TxnId min, TxnId max)
        {
            long epoch = min.epoch() == max.epoch() ? min.epoch() : rnd.nextLong(min.epoch(), max.epoch());
            long hlc = min.hlc() == max.hlc() ? min.hlc() : rnd.nextLong(min.hlc(), max.hlc());

            Node.Id node;
            if (hlc == min.hlc()) node = min.node.id == nodeIds.length + 1 ? min.node : nodeIds[rnd.nextInt(min.node.id - 1, nodeIds.length)];
            else if (hlc == max.hlc()) node = max.node.id == 1 ? max.node : nodeIds[rnd.nextInt(0, max.node.id - 1)];
            else node = rnd.pick(nodeIds);

            Txn.Kind kind;
            if (hlc == min.hlc()) kind = min.kind();
            else if (hlc == max.hlc()) kind = max.kind();
            else kind = rnd.pick(KINDS);

            Domain domain;
            if (hlc == min.hlc() && min.domain() == Domain.Range) domain = Domain.Range;
            else if (hlc == max.hlc() && max.domain() == Key) domain = Key;
            else domain = rnd.nextBoolean() ? Key : Domain.Range;

            return new TxnId(epoch, hlc, kind, domain, node);
        }

        Timestamp generateTimestamp(Timestamp min, Timestamp max, boolean unique)
        {
            Timestamp result = generateTimestamp(min, max);
            while (unique && (byId.containsKey(result) || executeAts.contains(result)))
                result = generateTimestamp(min, max);
            return result;
        }

        Timestamp generateTimestamp(Timestamp min, Timestamp max)
        {
            Invariants.checkArgument(min.flags() == 0);
            Invariants.checkArgument(max.flags() == 0);
            long epoch = min.epoch() == max.epoch() ? min.epoch() : rnd.nextLong(min.epoch(), max.epoch());
            long hlc;
            Node.Id node;
            if (min.hlc() == max.hlc()) hlc = min.hlc();
            else hlc = rnd.nextLong(min.hlc(), max.hlc());

            if (hlc == min.hlc()) node = min.node.id == nodeIds.length + 1 ? min.node : nodeIds[rnd.nextInt(min.node.id - 1, nodeIds.length)];
            else if (hlc == max.hlc()) node = max.node.id == 1 ? max.node : nodeIds[rnd.nextInt(0, max.node.id - 1)];
            else node = rnd.pick(nodeIds);

            Timestamp result = Timestamp.fromValues(epoch, hlc, node);
            Invariants.checkState(result.compareTo(min) >= 0);
            return result;
        }

        Command unwitnessed(TxnId txnId)
        {
            Command command = notDefined(common(txnId), Ballot.ZERO);
            unwitnessed.add(txnId);
            undecided.add(txnId);
            candidates.add(txnId);
            unfinished.add(txnId);
            return command;
        }

        Command preaccepted(TxnId txnId)
        {
            return Command.PreAccepted.preAccepted(common(txnId), txnId, Ballot.ZERO);
        }

        Command acceptedInvalidated(TxnId txnId, SaveStatus saveStatus)
        {
            return saveStatus == SaveStatus.AcceptedInvalidateWithDefinition
                   ? Command.Accepted.accepted(common(txnId, true), saveStatus, txnId, Ballot.ZERO, Ballot.ZERO)
                   : Command.AcceptedInvalidateWithoutDefinition.acceptedInvalidate(common(txnId, false), Ballot.ZERO, Ballot.ZERO);
        }

        Command accepted(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus)
        {
            Deps deps = generateDeps(txnId, txnId, Status.Accepted);
            return Command.Accepted.accepted(common(txnId, saveStatus.known.definition.isKnown()).partialDeps(deps.intersecting(txnId.domain() == Key ? KEY_ROUTE : RANGE_ROUTE)),
                                        saveStatus, executeAt, Ballot.ZERO, Ballot.ZERO);
        }

        Command committed(TxnId txnId, Timestamp executeAt)
        {
            Deps deps = generateDeps(txnId, executeAt, Status.Committed);
            return Command.Committed.committed(common(txnId).partialDeps(slice(txnId, deps)), SaveStatus.Committed, executeAt, Ballot.ZERO, Ballot.ZERO, null);
        }

        Command stable(TxnId txnId, Timestamp executeAt, @Nullable Command.Committed committed)
        {
            Deps deps = committed == null ? generateDeps(txnId, executeAt, Status.Stable) : committed.partialDeps();
            CommonAttributes common = common(txnId).partialDeps(slice(txnId, deps));
            Command.WaitingOn waitingOn = initialiseWaitingOn(txnId, executeAt, common.route(), deps);
            return Command.Committed.committed(common, SaveStatus.Stable, executeAt, Ballot.ZERO, Ballot.ZERO, waitingOn);
        }

        Command applied(TxnId txnId, Timestamp executeAt, @Nullable Command.Committed committed)
        {
            Deps deps = committed == null ? generateDeps(txnId, executeAt, Status.Applied) : committed.partialDeps();
            CommonAttributes common = common(txnId).partialDeps(slice(txnId, deps));
            Command.WaitingOn waitingOn = committed == null || committed.waitingOn == null ? initialiseWaitingOn(txnId, executeAt, common.route(), deps) : committed.waitingOn;
            return new Command.Executed(common, SaveStatus.Applied, executeAt, Ballot.ZERO, Ballot.ZERO, waitingOn,
                                        new Writes(txnId, executeAt, KEYS, null), new Result(){});
        }

        Command invalidated(TxnId txnId)
        {
            return new Command.Truncated(common(txnId), SaveStatus.Invalidated, Timestamp.NONE, null, null);
        }

        CommonAttributes.Mutable common(TxnId txnId)
        {
            return common(txnId, true);
        }

        CommonAttributes.Mutable common(TxnId txnId, boolean withDefinition)
        {
            CommonAttributes.Mutable result = new CommonAttributes.Mutable(txnId)
                   .durability(NotDurable)
                   .updateParticipants(StoreParticipants.all(txnId.is(Key) ? KEY_ROUTE : RANGE_ROUTE));

            if (withDefinition)
                result.partialTxn((txnId.domain() == Key ? KEY_TXN : RANGE_TXN).slice(RANGES, true));

            return result;
        }

        private Command.WaitingOn initialiseWaitingOn(TxnId txnId, Timestamp executeAt, Route<?> route, Deps deps)
        {
            Command.WaitingOn.Update waitingOn = Command.WaitingOn.Update.unsafeInitialise(txnId, RANGES, route, deps);
            for (int i = 0 ; i < waitingOn.txnIdCount() ; ++i)
            {
                TxnId dep = waitingOn.txnId(i);
                Command command = byId.get(dep);
                if (command.hasBeen(Status.Applied) || (command.hasBeen(Status.Committed) && command.executeAt().compareTo(executeAt) > 0))
                    waitingOn.removeWaitingOn(dep);
            }
            return waitingOn.build();
        }

        private Command selectOne(TreeSet<TxnId> from)
        {
            TxnId bound = generateId(from.first(), from.last());
            TxnId txnId = from.floor(bound);
            return byId.get(txnId);
        }

        private static PartialDeps slice(TxnId txnId, Deps deps)
        {
            return deps.intersecting(txnId.domain() == Key ? KEY_ROUTE : RANGE_ROUTE);
        }
    }

    @Test
    public void testOne()
    {
        test(232412256985395L, 1000);
//        test(System.nanoTime(), 500);
    }

    @Test
    public void testMany()
    {
        long seed = System.nanoTime();
        for (int i = 0 ; i < 1000 ; ++i)
        {
            test(seed++, 1000);
        }
    }

    private static void test(long seed, int minCount)
    {
        logger.info("Seed {}", seed);
        try
        {
            final RandomSource rnd = new DefaultRandom(seed);

            final float runTaskChance = Math.max(0.01f, rnd.nextFloat());
            final float pruneChance = rnd.nextFloat() * (rnd.nextBoolean() ? 0.1f : 0.01f);
            final int pruneHlcDelta = 1 << rnd.nextInt(10);
            final int pruneInterval = 1 << rnd.nextInt(5);
            final int maxConflictsHlcDelta = 1 << rnd.nextInt(10);
            final int maxConflictsPruneInterval = 1 << rnd.nextInt(5);
            final Canon canon = new Canon(rnd);
            TestCommandStore commandStore = new TestCommandStore(pruneInterval, pruneHlcDelta, maxConflictsHlcDelta, maxConflictsPruneInterval);
            TestSafeCommandsForKey safeCfk = new TestSafeCommandsForKey(new CommandsForKey(KEY));
            TestSafeStore safeStore = new TestSafeStore(canon, commandStore, safeCfk);
            int c = 0;
            while (!canon.isDone())
            {
                if (++c >= minCount)
                    canon.close();

                if (rnd.decide(runTaskChance - (runTaskChance/(1 + commandStore.queue.size()))))
                    commandStore.runOneTask(safeStore);

                CommandUpdate update = canon.update(!commandStore.queue.isEmpty());
                if (update == null)
                {
                    commandStore.runOneTask(safeStore);
                    continue;
                }
                TestSafeCommand safeCommand = new TestSafeCommand(update.next.txnId(), canon, update.next);

                CommandsForKeyUpdate result;
                if (CommandsForKey.manages(update.next.txnId()))
                {
                    CommandsForKey prev = safeCfk.current();
                    result = prev.update(update.next);
                    safeCfk.set(result.cfk());
                    if (rnd.decide(pruneChance))
                        safeCfk.set(safeCfk.current.maybePrune(pruneInterval, pruneHlcDelta));
                    result.postProcess(safeStore, prev, update.next, canon);
                }

                if (!CommandsForKey.managesExecution(update.next.txnId()) && update.next.hasBeen(Status.Stable) && !update.next.hasBeen(Status.Truncated))
                {
                    CommandsForKey prev = safeCfk.current();
                    result = prev.registerUnmanaged(safeCommand, REGISTER);
                    safeCfk.set(result.cfk());
                    result.postProcess(safeStore, prev, null, canon);
                }
            }
        }
        catch (Throwable t)
        {
            throw new AssertionError("Seed " + seed + " failed", t);
        }
    }


    static class TestSafeCommand extends SafeCommand
    {
        final Canon canon;
        Command current;
        public TestSafeCommand(TxnId txnId, Canon canon, Command command)
        {
            super(txnId);
            this.canon = canon;
            current = command;
        }

        @Override
        public Command current() { return current; }

        @Override
        public void invalidate() {}

        @Override
        public boolean invalidated() { return false; }

        @Override
        protected void set(Command command)
        {
            canon.set(current, command);
            current = command;
        }
    }

    static class TestSafeCommandsForKey extends SafeCommandsForKey
    {
        CommandsForKey current;
        public TestSafeCommandsForKey(CommandsForKey cfk)
        {
            super(cfk.key());
            current = cfk;
        }

        @Override
        public CommandsForKey current() { return current; }

        @Override
        protected void set(CommandsForKey command)
        {
            current = command;
        }
    }

    static class TestSafeStore extends SafeCommandStore
    {
        final Canon canon;
        final TestCommandStore commandStore;
        final TestSafeCommandsForKey cfk;

        TestSafeStore(Canon canon, TestCommandStore commandStore, TestSafeCommandsForKey cfk)
        {
            this.canon = canon;
            this.commandStore = commandStore;
            this.cfk = cfk;
        }

        @Override
        protected SafeCommand getInternal(TxnId txnId)
        {
            return new TestSafeCommand(txnId, canon, canon.byId.get(txnId));
        }

        @Override
        protected SafeCommand ifLoadedAndInitialisedAndNotErasedInternal(TxnId txnId)
        {
            return getInternal(txnId);
        }

        @Override
        protected SafeCommandsForKey getInternal(RoutingKey key)
        {
            Invariants.checkArgument(key.equals(cfk.key()));
            return cfk;
        }

        @Override
        public SafeCommand get(TxnId txnId)
        {
            return getInternal(txnId);
        }

        @Override
        public SafeCommandsForKey get(RoutingKey key)
        {
            return getInternal(key);
        }

        @Override
        public SafeCommand get(TxnId txnId, StoreParticipants participants)
        {
            return getInternal(txnId);
        }

        @Nullable
        @Override
        public SafeCommand ifInitialised(TxnId txnId)
        {
            return getInternal(txnId);
        }

        @Override
        public SafeCommand ifLoadedAndInitialisedAndNotErased(TxnId txnId)
        {
            if (txnId.compareTo(cfk.current.prunedBefore()) < 0)
                return null;

            return getInternal(txnId);
        }

        @Override
        protected void update(Command prev, Command updated)
        {
        }

        @Override
        public void upsertRedundantBefore(RedundantBefore addRedundantBefore)
        {
            unsafeUpsertRedundantBefore(addRedundantBefore);
        }

        @Override
        protected SafeCommandsForKey ifLoadedInternal(RoutingKey key)
        {
            if (key.equals(cfk.key()))
                return cfk;
            return null;
        }

        @Override
        public SafeTimestampsForKey timestampsForKey(RoutingKey key)
        {
            return null;
        }

        @Override
        public PreLoadContext canExecute(PreLoadContext context)
        {
            return context;
        }

        @Override
        public PreLoadContext context()
        {
            return null;
        }

        @Override
        public <P1, T> T mapReduceActive(Unseekables<?> keys, @Nullable Timestamp withLowerTxnId, Txn.Kind.Kinds kinds, CommandFunction<P1, T, T> map, P1 p1, T initialValue)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <P1, T> T mapReduceFull(Unseekables<?> keys, TxnId testTxnId, Txn.Kind.Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T initialValue)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CommandStore commandStore()
        {
            return commandStore;
        }

        @Override
        public DataStore dataStore()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Agent agent()
        {
            return commandStore;
        }

        @Override
        public ProgressLog progressLog()
        {
            return new ProgressLog.NoOpProgressLog();
        }

        @Override
        public NodeCommandStoreService node()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CommandStores.RangesForEpoch ranges()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TestRead implements Read
    {
        final Seekables<?, ?> keys;
        private TestRead(Seekables<?, ?> keys)
        {
            this.keys = keys;
        }

        @Override
        public Seekables<?, ?> keys()
        {
            return keys;
        }

        @Override
        public AsyncChain<Data> read(Seekable key, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Read slice(Ranges ranges)
        {
            return this;
        }

        @Override
        public Read intersecting(Participants<?> participants)
        {
            return this;
        }

        @Override
        public Read merge(Read other)
        {
            return this;
        }
    }

    private static class TestQuery implements Query
    {
        @Override
        public Result compute(@Nonnull TxnId txnId, @Nonnull Timestamp executeAt, @Nonnull Seekables<?, ?> keys, @Nullable Data data, @Nullable Read read, @Nullable Update update)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestCommandStore extends CommandStore implements Agent
    {
        class Task extends AsyncResults.AbstractResult<Void>
        {
            final Consumer<? super SafeCommandStore> consumer;
            Task(Consumer<? super SafeCommandStore> consumer)
            {
                this.consumer = consumer;
            }

            public void run(SafeCommandStore safeStore)
            {
                consumer.accept(safeStore);
                trySetResult(null, null);
            }
        }

        final int pruneInterval, pruneHlcDelta, maxConflictsHlcDelta, maxConflictsPruneInterval;
        final ArrayDeque<Task> queue = new ArrayDeque<>();

        protected TestCommandStore(int pruneInterval, int pruneHlcDelta, int maxConflictsHlcDelta, int maxConflictsPruneInterval)
        {
            super(0,
                  null,
                  null,
                  null,
                  ignore -> new ProgressLog.NoOpProgressLog(),
                  ignore -> new DefaultLocalListeners(new DefaultRemoteListeners((a, b, c, d, e)->{}), DefaultNotifySink.INSTANCE),
                  new EpochUpdateHolder());
            this.pruneInterval = pruneInterval;
            this.pruneHlcDelta = pruneHlcDelta;
            this.maxConflictsHlcDelta = maxConflictsHlcDelta;
            this.maxConflictsPruneInterval = maxConflictsPruneInterval;
        }

        @Override
        public boolean inStore()
        {
            return true;
        }

        boolean runOneTask(SafeCommandStore safeStore)
        {
            if (queue.isEmpty())
                return false;

            queue.poll().run(safeStore);
            return true;
        }

        @Override
        public Agent agent()
        {
            return this;
        }

        @Override
        public AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            Task task = new Task(consumer);
            queue.add(task);
            return task;
        }

        @Override
        public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown()
        {
            Invariants.checkState(queue.isEmpty());
        }

        @Override
        protected void registerTransitive(SafeCommandStore safeStore, RangeDeps deps){ }

        @Override
        public <T> AsyncChain<T> submit(Callable<T> task)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onRecover(Node node, Result success, Throwable fail)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onFailedBootstrap(String phase, Ranges ranges, Runnable retry, Throwable failure)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onStale(Timestamp staleSince, Ranges ranges)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onUncaughtException(Throwable t)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onHandledException(Throwable t, String context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long preAcceptTimeout()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cfkHlcPruneDelta()
        {
            return pruneHlcDelta;
        }

        @Override
        public int cfkPruneInterval()
        {
            return pruneInterval;
        }

        @Override
        public long maxConflictsHlcPruneDelta()
        {
            return maxConflictsHlcDelta;
        }

        @Override
        public long maxConflictsPruneInterval()
        {
            return maxConflictsPruneInterval;
        }

        @Override
        public Txn emptySystemTxn(Txn.Kind kind, Domain domain)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long attemptCoordinationDelay(Node node, SafeCommandStore safeStore, TxnId txnId, TimeUnit units, int retryCount)
        {
            return 0;
        }

        @Override
        public long seekProgressDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int retryCount, BlockedUntil blockedUntil, TimeUnit units)
        {
            return 0;
        }

        @Override
        public long retryAwaitTimeout(Node node, SafeCommandStore safeStore, TxnId txnId, int retryCount, BlockedUntil retrying, TimeUnit units)
        {
            return 0;
        }

        @Override
        public long expiresAt(ReplyContext replyContext, TimeUnit unit)
        {
            return 0;
        }
    }
}
