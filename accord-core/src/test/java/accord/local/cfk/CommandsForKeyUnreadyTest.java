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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.jupiter.api.Test;

import accord.api.Data;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Result.PersistableResult;
import accord.api.RoutingKey;
import accord.api.Update;
import accord.impl.IntKey;
import accord.local.Command;
import accord.local.CommandBuilder;
import accord.local.Node;
import accord.local.RedundantBefore.QuickBounds;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.CommandsForKey.Unmanaged;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.SaveStatus;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.DefaultRandom;
import accord.utils.async.AsyncChain;
import accord.utils.btree.BTree;

import static accord.local.cfk.UpdateUnmanagedMode.REGISTER;
import static accord.primitives.Timestamp.Flag.UNSTABLE;
import static accord.primitives.TxnId.Cardinality.Any;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This file was authored by LLM
 *
 * Tests for {@code CommandsForKey} when the UNREADY bound ({@code bounds.readyAt}) advances past records we are still
 * tracking, as happens on rebootstrap - and in particular on a {@code LOG_CORRUPTED} rebootstrap, where the region
 * before the bound is also {@code LOG_UNAVAILABLE}, so those records can never be loaded or updated again.
 *
 * A record before {@code readyAt} cannot execute locally ({@code mayExecute() == false}); if the log is unavailable it
 * can also never be loaded, so nothing may be left waiting on it. {@code CommandsForKey} filters such records almost
 * everywhere ({@code mayExecute}, {@code minUndecidedManagedById}, {@code isWaitingOnPruned},
 * {@code redundantOrBootstrappedBefore}, ...) - these tests cover the paths where it does not.
 */
@org.junit.jupiter.api.Disabled("The behaviour these tests cover (declining to load, and discarding loads for, pruned " +
                               "records before the UNREADY bound) was reverted: it also suppressed loads for a plain " +
                               "bootstrap, whose log is intact, leaving CommandsForKey with unresolvable missing " +
                               "entries - unanswerable for recovery, and unserialisable (Serialize encodes missing as " +
                               "an index into byId). The scenario documented here - a sync point blocked on a pruned " +
                               "load that cannot complete because the record is in the LOG_UNAVAILABLE portion of the " +
                               "log - still needs an answer; the intended one is that the load completes via " +
                               "Cleanup.Input.FULL_UNSAFE (AccordJournal.loadCommand), or failing that a filter on the " +
                               "LOG_UNAVAILABLE/LOG_INCOMPLETE bounds rather than on UNREADY.")
public class CommandsForKeyUnreadyTest
{
    static final RoutingKey KEY = IntKey.routing(1);
    private static final Keys KEYS = IntKey.keys(1);
    private static final Ranges RANGES = Ranges.of(IntKey.range(0, 2));
    private static final FullRoute<?> KEY_ROUTE = KEYS.toRoute(IntKey.routing(1));
    private static final FullRoute<?> RANGE_ROUTE = RANGES.toRoute(IntKey.routing(1));
    private static final Node.Id NODE = new Node.Id(1);

    /**
     * An ExclusiveSyncPoint (the kind of transaction rebootstrap itself waits on) witnessed a dependency that has since
     * been pruned, so we registered that we are loading it. The load cannot complete - the command is in the corrupt
     * (LOG_UNAVAILABLE) portion of the log - and then we rebootstrap, so the UNREADY bound moves past that dependency.
     *
     * For the sync point to make progress, both of the following must hold:
     * <li>advancing the bound must discard the {@code loadingPruned} record, and</li>
     * <li>re-evaluating the sync point must not re-register it.</li>
     */
    @Test
    public void unreadyLoadingPrunedDoesNotBlockSyncPoint()
    {
        TxnId pruned = txnId(5, Txn.Kind.Write);            // pruned out of the CFK, then depended upon
        TxnId prunedBefore = txnId(10, Txn.Kind.Write);
        TxnId write = txnId(20, Txn.Kind.Write);
        TxnId syncPoint = rangeSyncPoint(40);
        TxnId readyAt = rangeSyncPoint(30);                 // rebootstrap: everything before this is UNREADY

        // the pruned transaction is in the unavailable portion of the log, so it can never be loaded
        Harness harness = new Harness(prunedBefore);
        CommandsForKey cfk = harness.prunedCfk(pruned, prunedBefore, write);
        assertEquals(prunedBefore, cfk.prunedBefore(), "expected to have pruned to " + prunedBefore);
        assertNull(cfk.get(pruned), "expected " + pruned + " to have been pruned out of the CFK");

        cfk = harness.registerUnmanaged(cfk, harness.stable(syncPoint, syncPoint, deps(pruned)));
        assertTrue(Pruning.find(cfk.loadingPruned, pruned) != null, "expected to be loading pruned " + pruned);
        assertTrue(Pruning.isAnyPredecessorWaitingOnPruned(cfk.loadingPruned, syncPoint));
        assertEquals(1, cfk.unmanagedCount(), "expected the sync point to be waiting");

        // rebootstrap: the UNREADY (and LOG_UNAVAILABLE) bound moves beyond the pruned dependency
        cfk = harness.withBoundsAtLeast(cfk, readyAt);
        assertNull(Pruning.find(cfk.loadingPruned, pruned),
                   "a loadingPruned record before the UNREADY bound can never be loaded, so must be discarded, but have "
                   + Arrays.toString(loadingPruned(cfk)));
        assertFalse(Pruning.isAnyPredecessorWaitingOnPruned(cfk.loadingPruned, syncPoint),
                    "sync point must not be blocked by a record before the UNREADY bound");

        // ... and re-evaluating the sync point must neither re-register the load nor leave it waiting
        harness.notified.clear();
        cfk = harness.registerUnmanaged(cfk, harness.stable(syncPoint, syncPoint, deps(pruned)));
        assertNull(Pruning.find(cfk.loadingPruned, pruned),
                   "re-registration must not re-insert a loadingPruned record before the UNREADY bound, but have "
                   + Arrays.toString(loadingPruned(cfk)));
        assertEquals(0, cfk.unmanagedCount(), "sync point must not be waiting, but have " + Arrays.toString(unmanageds(cfk)));
        assertEquals(Arrays.asList(syncPoint), harness.notified, "sync point should have been notified that it is not waiting");
    }

    /**
     * The same asymmetry in isolation: {@code isWaitingOnPruned} (managed transactions) ignores loadingPruned records
     * before the UNREADY bound, {@code isAnyPredecessorWaitingOnPruned} (sync points) does not.
     */
    @Test
    public void isAnyPredecessorWaitingOnPrunedIgnoresUnready()
    {
        TxnId pruned = txnId(5, Txn.Kind.Write);
        TxnId readyAt = rangeSyncPoint(30);
        TxnId waiting = txnId(40, Txn.Kind.Write);
        QuickBounds bounds = bounds(readyAt);

        Object[] loadingPruned = Pruning.loadPruned(Pruning.LoadingPruned.empty(), new TxnId[]{ pruned }, waiting, new ArrayList<>());

        assertFalse(Pruning.isWaitingOnPruned(loadingPruned, waiting, waiting, bounds),
                    "a record before the UNREADY bound must not block a managed transaction");
        assertFalse(Pruning.isAnyPredecessorWaitingOnPruned(loadingPruned, waiting),
                    "a record before the UNREADY bound must not block a sync point either");
    }

    /**
     * A managed transaction that has witnessed a dependency which is pruned and unloadable: it must still be able to
     * execute once the UNREADY bound has moved past that dependency.
     */
    @Test
    public void unreadyLoadingPrunedDoesNotBlockManaged()
    {
        TxnId pruned = txnId(5, Txn.Kind.Write);
        TxnId prunedBefore = txnId(10, Txn.Kind.Write);
        TxnId write = txnId(20, Txn.Kind.Write);
        TxnId readyAt = rangeSyncPoint(30);
        TxnId stable = txnId(40, Txn.Kind.Write);

        Harness harness = new Harness(prunedBefore);
        CommandsForKey cfk = harness.prunedCfk(pruned, prunedBefore, write);

        // a later transaction witnesses the pruned transaction, so we register that we are loading it
        cfk = harness.update(cfk, harness.stable(stable, stable, deps(pruned, prunedBefore, write)));
        assertTrue(Pruning.find(cfk.loadingPruned, pruned) != null, "expected to be loading pruned " + pruned);
        assertEquals(Arrays.asList(), harness.notified, "must not execute while loading a witnessed dependency");

        harness.notified.clear();
        cfk = harness.withBoundsAtLeast(cfk, readyAt);
        assertNull(Pruning.find(cfk.loadingPruned, pruned),
                   "a loadingPruned record before the UNREADY bound can never be loaded, so must be discarded, but have "
                   + Arrays.toString(loadingPruned(cfk)));
        assertEquals(Arrays.asList(stable), harness.notified,
                     "advancing the UNREADY bound past the unloadable dependency must permit execution");
    }

    /**
     * Once the bound has advanced (e.g. after a restart, where the CFK is loaded with the rebootstrapped bounds) we must
     * not register a load for a pruned dependency before it in the first place - otherwise we simply re-create the state
     * that blocks execution, and the entry can never be removed because the load can never complete.
     */
    @Test
    public void neverLoadPrunedBeforeUnreadyBound()
    {
        TxnId pruned = txnId(5, Txn.Kind.Write);
        TxnId prunedBefore = txnId(10, Txn.Kind.Write);
        TxnId write = txnId(20, Txn.Kind.Write);
        TxnId readyAt = rangeSyncPoint(30);
        TxnId stable = txnId(40, Txn.Kind.Write);
        TxnId syncPoint = rangeSyncPoint(50);

        Harness harness = new Harness(prunedBefore);
        CommandsForKey cfk = harness.prunedCfk(pruned, prunedBefore, write);
        cfk = harness.withBoundsAtLeast(cfk, readyAt);
        harness.notified.clear();

        // a managed transaction that witnessed the pruned (and unloadable) dependency
        cfk = harness.update(cfk, harness.stable(stable, stable, deps(pruned, prunedBefore, write)));
        assertNull(Pruning.find(cfk.loadingPruned, pruned),
                   "must not load a pruned dependency before the UNREADY bound, but have " + Arrays.toString(loadingPruned(cfk)));
        assertEquals(Arrays.asList(stable), harness.notified, "must be free to execute");

        // ... and an unmanaged one
        harness.notified.clear();
        cfk = harness.registerUnmanaged(cfk, harness.stable(syncPoint, syncPoint, deps(pruned)));
        assertNull(Pruning.find(cfk.loadingPruned, pruned),
                   "must not load a pruned dependency before the UNREADY bound, but have " + Arrays.toString(loadingPruned(cfk)));
        assertEquals(0, cfk.unmanagedCount(), "sync point must not be waiting, but have " + Arrays.toString(unmanageds(cfk)));
        assertEquals(Arrays.asList(syncPoint), harness.notified, "sync point must not be waiting");
    }

    /**
     * An UNSTABLE dependency that was pruned is recorded in the witnessing transaction's {@code missing} collection but
     * not in {@code byId} - it is only tracked by {@code loadingPruned}. Discarding that record when the UNREADY bound
     * advances past it must therefore not trip the integrity check that pairs the two (we deliberately stop tracking
     * it: it will not execute locally, and we may not be able to load it).
     */
    @Test
    public void unstableMissingBeforeUnreadyBound()
    {
        TxnId pruned = txnId(5, Txn.Kind.Write);
        TxnId prunedBefore = txnId(10, Txn.Kind.Write);
        TxnId write = txnId(20, Txn.Kind.Write);
        TxnId readyAt = rangeSyncPoint(30);
        TxnId stable = txnId(40, Txn.Kind.Write);

        Harness harness = new Harness(prunedBefore);
        CommandsForKey cfk = harness.prunedCfk(pruned, prunedBefore, write);

        Deps deps;
        try (Deps.Builder builder = new Deps.Builder(true))
        {
            builder.add(KEY, pruned.addFlag(UNSTABLE));
            builder.add(KEY, prunedBefore);
            builder.add(KEY, write);
            deps = builder.build();
        }
        cfk = harness.update(cfk, harness.stable(stable, stable, deps));
        assertTrue(Pruning.find(cfk.loadingPruned, pruned) != null, "expected to be loading pruned " + pruned);
        assertEquals(1, cfk.get(stable).missing().length, "expected " + pruned + " to be recorded as missing");

        harness.notified.clear();
        cfk = harness.withBoundsAtLeast(cfk, readyAt); // must not trip checkIntegrity
        assertNull(Pruning.find(cfk.loadingPruned, pruned),
                   "a loadingPruned record before the UNREADY bound can never be loaded, so must be discarded, but have "
                   + Arrays.toString(loadingPruned(cfk)));
        assertEquals(Arrays.asList(stable), harness.notified, "must be free to execute");
    }

    private static TxnId[] loadingPruned(CommandsForKey cfk)
    {
        List<TxnId> result = new ArrayList<>();
        for (Pruning.LoadingPruned txn : BTree.<Pruning.LoadingPruned>iterable(cfk.loadingPruned))
            result.add(txn.plainTxnId());
        return result.toArray(new TxnId[0]);
    }

    static Unmanaged[] unmanageds(CommandsForKey cfk)
    {
        Unmanaged[] result = new Unmanaged[cfk.unmanagedCount()];
        for (int i = 0; i < result.length; ++i)
            result[i] = cfk.getUnmanaged(i);
        return result;
    }

    static TxnId txnId(long hlc, Txn.Kind kind)
    {
        return new TxnId(1, hlc, 0, kind, Domain.Key, Any, NODE);
    }

    static TxnId rangeSyncPoint(long hlc)
    {
        return new TxnId(1, hlc, 0, Txn.Kind.ExclusiveSyncPoint, Domain.Range, Any, NODE);
    }

    static QuickBounds bounds(TxnId readyAt)
    {
        return new QuickBounds(0, Long.MAX_VALUE, readyAt, TxnId.NONE, TxnId.NONE, TxnId.NONE);
    }

    static Deps deps(TxnId... txnIds)
    {
        try (Deps.Builder builder = new Deps.Builder(true))
        {
            for (TxnId txnId : txnIds)
                builder.add(KEY, txnId);
            return builder.build();
        }
    }

    // package-private so the fuzz test in this package can reuse the harness
    static class Harness implements NotifySink
    {
        final CommandsForKeyTest.Canon canon = new CommandsForKeyTest.Canon(new DefaultRandom(1));
        final CommandsForKeyTest.TestCommandStore commandStore = new CommandsForKeyTest.TestCommandStore(1 << 20, 1 << 20, 1 << 20, 1 << 20);
        final CommandsForKeyTest.TestSafeCommandsForKey safeCfk = new CommandsForKeyTest.TestSafeCommandsForKey(new CommandsForKey(KEY));
        final CommandsForKeyTest.TestSafeStore safeStore;
        final List<TxnId> notified = new ArrayList<>();

        /**
         * @param logUnavailableBefore commands before this cannot be loaded, as if the log were corrupt (LOG_UNAVAILABLE)
         */
        Harness(TxnId logUnavailableBefore)
        {
            this.safeStore = new CommandsForKeyTest.TestSafeStore(canon, commandStore, safeCfk)
            {
                @Override
                protected SafeCommand getInternal(TxnId txnId)
                {
                    if (txnId.compareTo(logUnavailableBefore) < 0)
                        return null;
                    return super.getInternal(txnId);
                }

                @Override
                protected SafeCommand ifLoadedInternal(TxnId txnId)
                {
                    return getInternal(txnId);
                }

                @Override
                public SafeCommand unsafeTryGet(TxnId txnId)
                {
                    return getInternal(txnId);
                }
            };
        }

        @Override
        public void notWaiting(SafeCommandStore safeStore, TxnId txnId, RoutingKey key, long uniqueHlc)
        {
            notified.add(txnId);
        }

        @Override
        public void waitingOn(SafeCommandStore safeStore, TxnInfo txn, RoutingKey key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk)
        {
        }

        /**
         * A CFK that has pruned to {@code prunedBefore}, so that {@code pruned} is no longer present.
         */
        CommandsForKey prunedCfk(TxnId pruned, TxnId prunedBefore, TxnId write)
        {
            CommandsForKey cfk = new CommandsForKey(KEY);
            cfk = update(cfk, applied(pruned, pruned, Deps.NONE));
            cfk = update(cfk, applied(prunedBefore, prunedBefore, deps(pruned)));
            cfk = update(cfk, applied(write, write, deps(pruned, prunedBefore)));
            cfk = cfk.maybePrune(0, 0);
            safeCfk.set(cfk);
            notified.clear();
            return cfk;
        }

        CommandsForKey update(CommandsForKey cfk, Command command)
        {
            canon.byId.put(command.txnId(), command);
            safeCfk.set(cfk);
            CommandsForKeyUpdate update = cfk.update(safeStore, command);
            safeCfk.set(update.cfk());
            update.postProcess(safeStore, cfk, command, this, false);
            return safeCfk.current();
        }

        CommandsForKey registerUnmanaged(CommandsForKey cfk, Command command)
        {
            canon.byId.put(command.txnId(), command);
            safeCfk.set(cfk);
            CommandsForKeyUpdate update = cfk.registerUnmanaged(safeStore, new CommandsForKeyTest.TestSafeCommand(command.txnId(), canon, command), REGISTER);
            safeCfk.set(update.cfk());
            update.postProcess(safeStore, cfk, null, this, false);
            return safeCfk.current();
        }

        CommandsForKey withBoundsAtLeast(CommandsForKey cfk, TxnId readyAt)
        {
            safeCfk.set(cfk);
            CommandsForKeyUpdate update = cfk.withBoundsAtLeast(bounds(readyAt), true);
            safeCfk.set(update.cfk());
            update.postProcess(safeStore, cfk, null, this, false);
            return safeCfk.current();
        }

        Command stable(TxnId txnId, Timestamp executeAt, Deps deps)
        {
            CommandBuilder builder = builder(txnId).partialDeps(slice(txnId, deps))
                                                   .executeAt(executeAt);
            builder.waitingOn(Command.WaitingOn.Update.unsafeInitialise(txnId, RANGES, builder.participants().route(), deps).build());
            return builder.build(SaveStatus.Stable);
        }

        Command applied(TxnId txnId, Timestamp executeAt, Deps deps)
        {
            // durable, so that the record is APPLIED_DURABLE and therefore prunable
            CommandBuilder builder = builder(txnId).durability(Durability.Universal)
                                                   .partialDeps(slice(txnId, deps))
                                                   .executeAt(executeAt)
                                                   .result(new PersistableResult() {});
            builder.waitingOn(Command.WaitingOn.Update.unsafeInitialise(txnId, RANGES, builder.participants().route(), deps).build());
            if (txnId.is(Txn.Kind.Write))
                builder.writes(new Writes(txnId, executeAt, KEYS, null));
            return builder.build(SaveStatus.Applied);
        }

        private static CommandBuilder builder(TxnId txnId)
        {
            boolean isKey = txnId.is(Domain.Key);
            FullRoute<?> route = isKey ? KEY_ROUTE : RANGE_ROUTE;
            Seekables<?, ?> keysOrRanges = isKey ? KEYS : RANGES;
            return new CommandBuilder(txnId)
                   .durability(Durability.NotDurable)
                   .participants(StoreParticipants.all(route))
                   .partialTxn(new Txn.InMemory(keysOrRanges, new TestRead(keysOrRanges), new TestQuery()).slice(RANGES, true));
        }

        private static PartialDeps slice(TxnId txnId, Deps deps)
        {
            return deps.intersecting(txnId.is(Domain.Key) ? KEY_ROUTE : RANGE_ROUTE);
        }
    }

    private static class TestRead implements Read
    {
        final Seekables<?, ?> keys;
        TestRead(Seekables<?, ?> keys) { this.keys = keys; }
        @Override public Seekables<?, ?> keys() { return keys; }
        @Override public AsyncChain<Data> read(SafeCommandStore safeStore, Seekable key, Timestamp executeAt) { throw new UnsupportedOperationException(); }
        @Override public Read slice(Ranges ranges) { return this; }
        @Override public Read intersecting(Participants<?> participants) { return this; }
        @Override public Read merge(Read other) { return this; }
    }

    private static class TestQuery implements Query
    {
        @Override
        public Result compute(@Nonnull TxnId txnId, @Nonnull Timestamp executeAt, @Nonnull Seekables<?, ?> keys, @Nullable Data data, @Nullable Read read, @Nullable Update update)
        {
            throw new UnsupportedOperationException();
        }
    }
}
