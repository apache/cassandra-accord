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

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Data;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Update;
import accord.impl.IntKey;
import accord.local.Command;
import accord.local.CommandBuilder;
import accord.local.Node;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.SaveStatus;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.DefaultRandom;
import accord.utils.async.AsyncChain;

import static accord.local.cfk.UpdateUnmanagedMode.REGISTER;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.TxnId.Cardinality.Any;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * {@code CommandsForKey} is a persistent (immutable) data structure: every update publishes a new instance and the
 * prior instance must be unaffected, because
 * <ul>
 *     <li>{@code byId} and {@code committedByExecuteAt} must reference the <i>same</i> {@link TxnInfo} object for a
 *         committed transaction (several invariants and the in-place notification flags depend on it), and</li>
 *     <li>the prior instance remains reachable: it is passed to {@code postProcess} as {@code prevCfk}, it is retained
 *         by the (Cassandra) cache entry until the operation completes, and it is what a discarded/retried operation
 *         starts from again.</li>
 * </ul>
 *
 * These tests demonstrate that inserting a <i>transitive</i> dependency into the missing collection of an already
 * COMMITTED transaction breaks that: the update writes the rebuilt {@code TxnInfo} into a {@code committedByExecuteAt}
 * array that is still shared with (and reachable from) the previous instance.
 *
 * @see CommandsForKeyTest for the (canon-model driven) fuzz test; {@link #fuzzPriorInstancesAreImmutable()} below adds
 * the same immutability assertion to a randomised sequence of updates.
 */
public class CommandsForKeyImmutabilityTest
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsForKeyImmutabilityTest.class);

    private static final RoutingKey KEY = IntKey.routing(1);
    private static final Keys KEYS = IntKey.keys(1);
    private static final Ranges RANGES = Ranges.of(IntKey.range(0, 2));
    private static final FullRoute<?> KEY_ROUTE = KEYS.toRoute(IntKey.routing(1));
    private static final Node.Id NODE = new Node.Id(1);

    /**
     * A {@code CommandsForKey} update that inserts a transitive dependency which must be added to the missing
     * collection of an existing COMMITTED transaction does not insert anything into {@code committedByExecuteAt}, so
     * the array is not copied - but the COMMITTED transaction's entry in it <i>is</i> rewritten in place, corrupting
     * the previous (still reachable, already validated) instance.
     */
    @Test
    public void insertTransitiveDependencyDoesNotMutatePriorInstance()
    {
        TxnId write = txnId(20, Txn.Kind.Write);      // committed, executes at itself
        TxnId accept = txnId(30, Txn.Kind.Write);     // the update we apply, with a dependency we have not witnessed
        TxnId transitive = txnId(10, Txn.Kind.Write); // ... this one, inserted as a TRANSITIVE record

        Harness harness = new Harness();
        CommandsForKey empty = new CommandsForKey(KEY);
        CommandsForKey committed = harness.update(empty, harness.committed(write, write, Deps.NONE));
        assertEquals(1, committed.committedByExecuteAt.length);

        // the prior instance is consistent, and (being immutable) must stay that way
        assertConsistent("committed", committed);
        TxnInfo[] committedByExecuteAt = committed.committedByExecuteAt;
        TxnInfo[] snapshot = committedByExecuteAt.clone();

        CommandsForKey next = harness.update(committed, harness.accepted(accept, accept, deps(transitive)));
        // the transitive dependency was inserted, and recorded as missing by the committed transaction
        assertEquals(3, next.byId.length);
        assertEquals(1, next.committedByExecuteAt.length);
        assertEquals(Arrays.asList(transitive), Arrays.asList(next.get(write).missing()));

        assertIdentical("the prior instance's committedByExecuteAt array was written in place",
                        snapshot, committedByExecuteAt);
        assertConsistent("prior instance after a transitive insertion", committed);
    }

    /**
     * As above, but with the transitive dependency inserted by its own (PreAccepted) update rather than as an addition
     * of another transaction's dependencies - i.e. via {@code Utils.addToMissingArrays} instead of
     * {@code Updating.insertOrUpdateWithAdditions}. Both mutate the array they were handed, and
     * {@code Updating.updateCommittedByExecuteAt} hands them the live one whenever the update itself neither inserts
     * into, nor removes from, {@code committedByExecuteAt}.
     */
    @Test
    public void insertUncommittedTxnDoesNotMutatePriorInstance()
    {
        TxnId write = txnId(20, Txn.Kind.Write);
        TxnId earlier = txnId(10, Txn.Kind.Write);

        Harness harness = new Harness();
        CommandsForKey committed = harness.update(new CommandsForKey(KEY), harness.committed(write, write, Deps.NONE));
        assertConsistent("committed", committed);
        TxnInfo[] committedByExecuteAt = committed.committedByExecuteAt;
        TxnInfo[] snapshot = committedByExecuteAt.clone();

        CommandsForKey next = harness.update(committed, harness.preaccepted(earlier));
        assertEquals(Arrays.asList(earlier), Arrays.asList(next.get(write).missing()));

        assertIdentical("the prior instance's committedByExecuteAt array was written in place",
                        snapshot, committedByExecuteAt);
        assertConsistent("prior instance after an uncommitted insertion", committed);
    }

    /**
     * The consequence: once a prior instance has been corrupted, anything that derives a new instance from it (a
     * discarded or retried operation restarting from the value the cache still holds) propagates the mismatch into the
     * new instance - and it is the new instance that trips the invariant, having "inherited" a corruption that
     * predates it.
     */
    @Test
    public void corruptionPropagatesToLaterInstances()
    {
        TxnId write = txnId(20, Txn.Kind.Write);
        TxnId accept = txnId(30, Txn.Kind.Write);
        TxnId transitive = txnId(10, Txn.Kind.Write);
        TxnId later = txnId(40, Txn.Kind.Write);

        Harness harness = new Harness();
        CommandsForKey committed = harness.update(new CommandsForKey(KEY), harness.committed(write, write, Deps.NONE));
        // discarded update: it publishes a new instance, but we (like an aborted operation) keep using the old one
        harness.update(committed, harness.accepted(accept, accept, deps(transitive)));

        // now commit another transaction, starting from the instance we retained
        CommandsForKey next = harness.update(committed, harness.committed(later, later, Deps.NONE));
        assertConsistent("instance derived from a corrupted prior instance", next);
    }

    /**
     * Fuzz: as {@code CommandsForKeyTest.testMany}, but asserting after every update that every instance we have ever
     * published is still internally consistent. Nothing may retroactively modify a published instance.
     */
    @Test
    public void fuzzPriorInstancesAreImmutable()
    {
        long seed = System.nanoTime();
        for (int i = 0; i < 20; ++i)
            fuzz(seed + i, 200);
    }

    private static void fuzz(long seed, int minCount)
    {
        logger.info("Seed {}", seed);
        List<CommandsForKey> published = new ArrayList<>();
        try
        {
            // mirrors CommandsForKeyTest.test, but retaining and re-validating every instance we publish
            DefaultRandom rnd = new DefaultRandom(seed);
            float runTaskChance = Math.max(0.01f, rnd.nextFloat());
            float pruneChance = rnd.nextFloat() * (rnd.nextBoolean() ? 0.1f : 0.01f);
            int pruneHlcDelta = 1 << rnd.nextInt(10);
            int pruneInterval = 1 << rnd.nextInt(5);
            CommandsForKeyTest.Canon canon = new CommandsForKeyTest.Canon(rnd);
            CommandsForKeyTest.TestCommandStore commandStore = new CommandsForKeyTest.TestCommandStore(pruneInterval, pruneHlcDelta, 1 << rnd.nextInt(10), 1 << rnd.nextInt(5));
            CommandsForKeyTest.TestSafeCommandsForKey safeCfk = new CommandsForKeyTest.TestSafeCommandsForKey(new CommandsForKey(KEY));
            CommandsForKeyTest.TestSafeStore safeStore = new CommandsForKeyTest.TestSafeStore(canon, commandStore, safeCfk);
            published.add(safeCfk.current());
            int c = 0;
            while (!canon.isDone())
            {
                if (++c >= minCount)
                    canon.close();

                if (rnd.decide(runTaskChance - (runTaskChance / (1 + commandStore.queue.size()))))
                    commandStore.runOneTask(safeStore);

                CommandsForKeyTest.CommandUpdate update = canon.update(!commandStore.queue.isEmpty());
                if (update == null)
                {
                    commandStore.runOneTask(safeStore);
                    continue;
                }

                if (CommandsForKey.manages(update.next.txnId()))
                {
                    CommandsForKey prev = safeCfk.current();
                    CommandsForKeyUpdate result = prev.update(safeStore, update.next);
                    safeCfk.set(result.cfk());
                    if (rnd.decide(pruneChance))
                        safeCfk.set(safeCfk.current().maybePrune(pruneInterval, pruneHlcDelta));
                    published.add(safeCfk.current());
                    result.postProcess(safeStore, prev, update.next, canon, false);
                    published.add(safeCfk.current());
                    assertAllConsistent(published, update.next.saveStatus() + " " + update.next.txnId());
                }

                if (!CommandsForKey.managesExecution(update.next.txnId()) && update.next.hasBeen(Status.Stable) && !update.next.hasBeen(Status.Truncated))
                {
                    CommandsForKey prev = safeCfk.current();
                    CommandsForKeyUpdate result = prev.registerUnmanaged(safeStore, new CommandsForKeyTest.TestSafeCommand(update.next.txnId(), canon, update.next), REGISTER);
                    safeCfk.set(result.cfk());
                    published.add(safeCfk.current());
                    result.postProcess(safeStore, prev, null, canon, false);
                    published.add(safeCfk.current());
                    assertAllConsistent(published, "registerUnmanaged " + update.next.txnId());
                }
            }
        }
        catch (Throwable t)
        {
            throw new AssertionError("Seed " + seed + " failed", t);
        }
    }

    private static void assertAllConsistent(List<CommandsForKey> published, String after)
    {
        for (CommandsForKey cfk : published)
            assertConsistent("after " + after, cfk);
    }

    /**
     * The invariant reported in the field (a superlinear-paranoia check in {@code CommandsForKey.checkIntegrity}):
     * every {@code committedByExecuteAt} entry must be the very same object as the corresponding {@code byId} entry.
     */
    private static void assertConsistent(String message, CommandsForKey cfk)
    {
        for (TxnInfo txn : cfk.committedByExecuteAt)
        {
            TxnInfo byId = cfk.get(txn);
            if (byId != txn)
                fail(message + ": committedByExecuteAt entry " + txn + " (missing: " + Arrays.toString(txn.missing())
                     + ") does not match byId entry " + byId + " (missing: " + (byId == null ? null : Arrays.toString(byId.missing())) + ')');
        }
    }

    private static void assertIdentical(String message, TxnInfo[] expect, TxnInfo[] actual)
    {
        assertEquals(expect.length, actual.length, message);
        for (int i = 0; i < expect.length; ++i)
            assertSame(expect[i], actual[i], message + " (at " + i + ')');
    }

    private static TxnId txnId(long hlc, Txn.Kind kind)
    {
        return new TxnId(1, hlc, 0, kind, Domain.Key, Any, NODE);
    }

    private static Deps deps(TxnId... txnIds)
    {
        try (Deps.Builder builder = new Deps.Builder(true))
        {
            for (TxnId txnId : txnIds)
                builder.add(KEY, txnId);
            return builder.build();
        }
    }

    private static class Harness
    {
        final CommandsForKeyTest.Canon canon = new CommandsForKeyTest.Canon(new DefaultRandom(1));
        final CommandsForKeyTest.TestCommandStore commandStore = new CommandsForKeyTest.TestCommandStore(1 << 20, 1 << 20, 1 << 20, 1 << 20);
        final CommandsForKeyTest.TestSafeCommandsForKey safeCfk = new CommandsForKeyTest.TestSafeCommandsForKey(new CommandsForKey(KEY));
        final CommandsForKeyTest.TestSafeStore safeStore = new CommandsForKeyTest.TestSafeStore(canon, commandStore, safeCfk);

        CommandsForKey update(CommandsForKey cfk, Command command)
        {
            canon.byId.put(command.txnId(), command);
            safeCfk.set(cfk);
            return cfk.update(safeStore, command).cfk();
        }

        Command preaccepted(TxnId txnId)
        {
            return builder(txnId).build(SaveStatus.PreAccepted);
        }

        Command accepted(TxnId txnId, Timestamp executeAt, Deps deps)
        {
            return builder(txnId).partialDeps(slice(deps))
                                 .executeAt(executeAt)
                                 .build(SaveStatus.AcceptedMediumWithDefinition);
        }

        Command committed(TxnId txnId, Timestamp executeAt, Deps deps)
        {
            return builder(txnId).partialDeps(slice(deps))
                                 .executeAt(executeAt)
                                 .build(SaveStatus.Committed);
        }

        private static PartialDeps slice(Deps deps)
        {
            return deps.intersecting(KEY_ROUTE);
        }

        private static CommandBuilder builder(TxnId txnId)
        {
            return new CommandBuilder(txnId)
                   .durability(NotDurable)
                   .participants(StoreParticipants.all(KEY_ROUTE))
                   .partialTxn(new Txn.InMemory(KEYS, new TestRead(), new TestQuery()).slice(RANGES, true));
        }
    }

    private static class TestRead implements Read
    {
        @Override public Seekables<?, ?> keys() { return KEYS; }
        @Override public AsyncChain<Data> read(accord.local.SafeCommandStore safeStore, Seekable key, Timestamp executeAt) { throw new UnsupportedOperationException(); }
        @Override public Read slice(Ranges ranges) { return this; }
        @Override public Read intersecting(accord.primitives.Participants<?> participants) { return this; }
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
