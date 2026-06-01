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
import java.util.List;

import org.junit.jupiter.api.Test;

import accord.local.cfk.CommandsForKey.Unmanaged;
import accord.primitives.Deps;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.DefaultRandom;
import accord.utils.RandomSource;

import static accord.local.cfk.CommandsForKeyUnreadyTest.Harness;
import static accord.local.cfk.CommandsForKeyUnreadyTest.deps;
import static accord.local.cfk.CommandsForKeyUnreadyTest.rangeSyncPoint;
import static accord.local.cfk.CommandsForKeyUnreadyTest.txnId;
import static accord.local.cfk.CommandsForKeyUnreadyTest.unmanageds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This file was authored by LLM
 *
 * Single-key fuzzing of the {@code RedundantBefore} updates a rebootstrap performs, against a
 * {@link CommandsForKey} that has live waiters.
 *
 * <h2>Why this test exists</h2>
 * The load test ({@code AccordRebootstrapLoadTest}) reproduces a stall in ~6 minutes, on one seed, only under CPU
 * starvation, and needs five nodes to do it; see {@code HANDOVER-accord-rebootstrap-4.md} section 3. The stall's final state
 * is entirely local, and entirely about this class:
 * <pre>
 *   DBGWAIT    [51,...,151(RX),5] on store 3 status=PreApplied waitingOn=keys=[... many ...]
 *   DBGNOTREADY[51,...,151(RX),5] on key tid:25:-1602326395564712541:
 *                  waitingToApply=true waitingToExecuteAt=[48,...,194(KW),5] missing=0 loadingPruned=none
 * </pre>
 * i.e. a bootstrap's bound sync point is held, on a key, by a *pre-rebootstrap* write that is committed but will never
 * apply locally - a record that the rebootstrap's UNREADY bound has since covered. The bound is applied to the CFK
 * ({@code withBoundsAtLeast} -> {@code notifyManagedUnready}), but the apply-readiness computation in {@link Updating}
 * decides with {@code mayExecute()/witnessedBy/isAtLeast(APPLIED)} and never consults
 * {@link CommandsForKey#redundantOrUnreadyBefore()}, so the hold survives every re-evaluation.
 *
 * The purpose of this class is to make that family of bugs reproducible in milliseconds, deterministically, without a
 * cluster: build a CFK, put waiters on it, then apply the bound shapes a rebootstrap applies, and assert the liveness
 * property that the cluster ultimately depends on.
 *
 * <h2>The property under test</h2>
 * <b>Once the UNREADY bound covers every dependency a waiter is blocked on, the waiter must not be waiting.</b>
 * A record below the bound can never execute locally ({@code mayExecute() == false}), so waiting for it is waiting
 * forever; and the bound is exactly the statement "everything below me is being replaced by a bootstrap".
 *
 * <h2>Status</h2>
 * {@link #committedButUnappliedWriteBelowUnreadyBoundDoesNotBlockSyncPoint()} is the deterministic reduction of the
 * observed stall and is expected to <b>fail</b> until the gap is fixed; treat it as the reproducer. The fuzz loop then
 * searches the same shape space more broadly. Both are starting points: the operation set is deliberately small
 * (see the TODOs) and should grow to cover LOG_INCOMPLETE/LOG_UNAVAILABLE bounds, GC bounds, pruning and multiple
 * overlapping bootstraps, which is the rest of what a rebootstrap does to {@code RedundantBefore}.
 */
public class CommandsForKeyRedundantBeforeFuzzTest
{
    /**
     * The deterministic reduction of the load-test stall (handover 4 section 3a):
     * a committed-but-not-applied write, a later sync point that depends on it, then the rebootstrap's UNREADY bound
     * moves past the write. The sync point must stop waiting - the write can never apply here.
     */
    @Test
    public void committedButUnappliedWriteBelowUnreadyBoundDoesNotBlockSyncPoint()
    {
        TxnId write = txnId(20, Txn.Kind.Write);        // committed (Stable), never applied: the epoch-48 write
        TxnId syncPoint = rangeSyncPoint(40);           // the bootstrap's bound sync point
        TxnId readyAt = rangeSyncPoint(30);            // rebootstrap: everything before this is UNREADY

        Harness harness = new Harness(TxnId.NONE);
        CommandsForKey cfk = new CommandsForKey(CommandsForKeyUnreadyTest.KEY);
        cfk = harness.update(cfk, harness.stable(write, write, Deps.NONE));
        cfk = harness.registerUnmanaged(cfk, harness.stable(syncPoint, syncPoint, deps(write)));
        assertEquals(1, cfk.unmanagedCount(), "the sync point should be waiting on the unapplied write");

        harness.notified.clear();
        cfk = harness.withBoundsAtLeast(cfk, readyAt);
        // re-evaluate exactly as a replica does when the coordinator re-sends Apply
        cfk = harness.registerUnmanaged(cfk, harness.stable(syncPoint, syncPoint, deps(write)));

        assertNotWaitingOnAnythingBelowTheBound(cfk, readyAt);
        assertEquals(0, cfk.unmanagedCount(),
                     "a committed-but-unappliable record below the UNREADY bound must not block a sync point, but have "
                     + java.util.Arrays.toString(unmanageds(cfk)));
        assertTrue(harness.notified.contains(syncPoint), "the sync point should have been notified that it is not waiting");
    }

    /**
     * The same property, fuzzed: random mixes of records and statuses below and above a random UNREADY bound.
     * TODO (expected): extend the operation set - see the class comment. In particular: interleave two bounds
     *  (concurrent rebootstraps), prune between updates, and drive LOG_INCOMPLETE/LOG_UNAVAILABLE load failures
     *  (the {@code Harness} constructor argument) rather than only UNREADY.
     */
    @Test
    public void unreadyBoundNeverLeavesAWaiterBlocked()
    {
        for (long seed = 0 ; seed < 500 ; ++seed)
        {
            try { checkOneSeed(new DefaultRandom(seed)); }
            catch (Throwable t) { throw new AssertionError("seed " + seed, t); }
        }
    }

    private static void checkOneSeed(RandomSource random)
    {
        int count = 1 + random.nextInt(6);
        long hlc = 10;

        Harness harness = new Harness(TxnId.NONE);
        CommandsForKey cfk = new CommandsForKey(CommandsForKeyUnreadyTest.KEY);

        // records the sync point will depend on, in a mix of the states a pre-rebootstrap record can be in.
        // Applied records must form a prefix: a later record cannot have applied while an earlier one it should have
        // witnessed has not (CommandsForKey reports that as a linearizability violation, and rightly so).
        int appliedPrefix = random.nextInt(count + 1);
        List<TxnId> deps = new ArrayList<>();
        for (int i = 0 ; i < count ; ++i)
        {
            TxnId txnId = txnId(hlc += 1 + random.nextInt(5), random.nextBoolean() ? Txn.Kind.Write : Txn.Kind.Read);
            deps.add(txnId);
            cfk = i < appliedPrefix ? harness.update(cfk, harness.applied(txnId, txnId, Deps.NONE))
                                    : harness.update(cfk, harness.stable(txnId, txnId, Deps.NONE));
        }

        TxnId readyAt = rangeSyncPoint(hlc += 1 + random.nextInt(5)); // the bound covers every dep above
        TxnId syncPoint = rangeSyncPoint(hlc + 10);

        cfk = harness.registerUnmanaged(cfk, harness.stable(syncPoint, syncPoint, deps(deps.toArray(TxnId[]::new))));
        harness.notified.clear();
        cfk = harness.withBoundsAtLeast(cfk, readyAt);
        cfk = harness.registerUnmanaged(cfk, harness.stable(syncPoint, syncPoint, deps(deps.toArray(TxnId[]::new))));

        assertNotWaitingOnAnythingBelowTheBound(cfk, readyAt);
        assertEquals(0, cfk.unmanagedCount(),
                     "no dependency remains that can execute here, so the sync point must not be waiting; have "
                     + java.util.Arrays.toString(unmanageds(cfk)));
    }

    /** nothing below the UNREADY/redundant bound may still be treated as a blocker */
    private static void assertNotWaitingOnAnythingBelowTheBound(CommandsForKey cfk, TxnId bound)
    {
        assertTrue(cfk.redundantOrUnreadyBefore().compareTo(bound) >= 0,
                   "expected the bound to have been applied: redundantOrUnreadyBefore=" + cfk.redundantOrUnreadyBefore());
        for (Unmanaged unmanaged : unmanageds(cfk))
            assertFalse(unmanaged.waitingUntil != null && unmanaged.waitingUntil.compareTo(bound) < 0,
                        unmanaged.txnId + " is still waiting until " + unmanaged.waitingUntil + ", which is below the bound " + bound);
    }
}
