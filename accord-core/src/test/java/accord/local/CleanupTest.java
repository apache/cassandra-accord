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

package accord.local;

import org.junit.jupiter.api.Test;

import accord.local.RedundantStatus.SomeStatus;
import accord.primitives.FullKeyRoute;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.assertj.core.api.Assertions;

import static accord.impl.IntKey.range;
import static accord.impl.IntKey.routing;
import static accord.local.Cleanup.Input.FULL;
import static accord.local.Cleanup.Input.PARTIAL;
import static accord.local.RedundantStatus.SomeStatus.GC_BEFORE_AND_LOCALLY_DURABLE;
import static accord.local.RedundantStatus.SomeStatus.LOG_INCOMPLETE_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOG_UNAVAILABLE_ONLY;
import static accord.local.RedundantStatus.SomeStatus.UNREADY_ONLY;
import static accord.primitives.Status.Durability.NotDurable;

/**
 * The log-fault branch of {@link Cleanup}.  A fault means only that an absent or undecided record may be
 * missing rather than absent, so it forbids us from concluding anything - and it forbids that only when it
 * leaves us no participant range we could still answer for, i.e. when {@code all(LOCALLY_DEFUNCT)}.  It is
 * never a licence to rewrite the log.
 * <p>
 * Two ranges, one transaction touching both, so that {@code any} and {@code all} can disagree.
 */
public class CleanupTest
{
    private static final Range LEFT = range(0, 50), RIGHT = range(50, 150);
    private static final FullKeyRoute ROUTE = RoutingKeys.of(routing(10), routing(100)).toRoute(routing(10));
    private static final StoreParticipants PARTICIPANTS = StoreParticipants.all(ROUTE);

    // the transaction under decision, and a (sync point) bound that covers it
    private static final TxnId TXN_ID = new TxnId(1, 100, Txn.Kind.Write, Routable.Domain.Key, new Node.Id(1));
    private static final Timestamp EXECUTE_AT = Timestamp.fromValues(1, 150, new Node.Id(1));
    private static final TxnId ABOVE = new TxnId(1, 200, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, Node.Id.NONE);

    @Test
    public void mixedLogFaultRetainsTheRecord()
    {
        // LEFT's log is corrupt below ABOVE, but RIGHT is still ours with a complete log, so RIGHT's log is
        // sufficient to have witnessed the command: the record is trustworthy and must be retained
        RedundantBefore redundantBefore = bounds(LOG_UNAVAILABLE_ONLY, SomeStatus.NONE);

        Assertions.assertThat(cleanup(FULL, redundantBefore, SaveStatus.PreAccepted)).isEqualTo(Cleanup.NO);
        Assertions.assertThat(cleanup(PARTIAL, redundantBefore, SaveStatus.PreAccepted)).isEqualTo(Cleanup.NO);
    }

    @Test
    public void logFaultWithNothingLeftToAnswerForRefusesFullRead()
    {
        // LEFT's log is corrupt and RIGHT is pre-bootstrap: nothing is left for us to answer, so a FULL read
        // may not conclude anything from an absent or undecided record
        RedundantBefore redundantBefore = bounds(LOG_UNAVAILABLE_ONLY, UNREADY_ONLY);

        Assertions.assertThatThrownBy(() -> cleanup(FULL, redundantBefore, SaveStatus.PreAccepted))
                  .isInstanceOf(LogFaultException.class);
    }

    @Test
    public void logFaultNeverRewritesTheLog()
    {
        // ... and compaction emits nothing at all, decided or not: every level it could emit asserts something
        // the fault is no evidence for - ERASE that every shard applied at all healthy replicas, TRUNCATE (via
        // TruncatedApply's APPLIED summary) that this shard applied, VESTIGIAL that it cannot have committed
        RedundantBefore redundantBefore = bounds(LOG_UNAVAILABLE_ONLY, UNREADY_ONLY);

        Assertions.assertThat(cleanup(PARTIAL, redundantBefore, SaveStatus.Stable)).isEqualTo(Cleanup.NO);
        Assertions.assertThat(cleanup(PARTIAL, redundantBefore, SaveStatus.PreAccepted)).isEqualTo(Cleanup.NO);
    }

    @Test
    public void ordinaryBoundsStillCollectALogFaultedRecord()
    {
        // collection is deferred to evidence, not lost.  Given TRUNCATE_BEFORE (which implies SHARD_APPLIED)
        // the ordinary path truncates the same record the fault alone may not touch - and here TRUNCATE's
        // APPLIED summary is licensed by the bounds rather than asserted on the strength of the fault
        RedundantBefore redundantBefore = bounds(with(GC_BEFORE_AND_LOCALLY_DURABLE, LOG_UNAVAILABLE_ONLY),
                                                with(GC_BEFORE_AND_LOCALLY_DURABLE, UNREADY_ONLY));

        Assertions.assertThat(cleanup(PARTIAL, redundantBefore, SaveStatus.Stable, Durability.Universal))
                  .isEqualTo(Cleanup.TRUNCATE);
    }

    @Test
    public void expungeStillRemovesALogFaultedRecordEntirely()
    {
        // ... and once the global bounds license it the record goes altogether, which is what makes declining
        // to rewrite the log a deferral rather than a leak: expunge is decided before the fault is consulted,
        // on summary information alone
        RedundantBefore redundantBefore = bounds(with(GC_BEFORE_AND_LOCALLY_DURABLE, LOG_UNAVAILABLE_ONLY),
                                                with(GC_BEFORE_AND_LOCALLY_DURABLE, UNREADY_ONLY));
        DurableBefore durableBefore = DurableBefore.create(Ranges.of(LEFT, RIGHT), ABOVE, ABOVE);

        Assertions.assertThat(Cleanup.shouldCleanup(PARTIAL, TXN_ID, EXECUTE_AT, SaveStatus.Stable, Durability.Universal,
                                                   PARTICIPANTS, redundantBefore, durableBefore))
                  .isEqualTo(Cleanup.EXPUNGE);
    }

    @Test
    public void incompleteLogOnlyAffectsUndecidedRecords()
    {
        // an incomplete log can only mislead us about a record we do not have, or have not decided;
        // a Stable record is its own evidence
        RedundantBefore redundantBefore = bounds(LOG_INCOMPLETE_ONLY, LOG_INCOMPLETE_ONLY);

        Assertions.assertThatThrownBy(() -> cleanup(FULL, redundantBefore, SaveStatus.PreAccepted))
                  .isInstanceOf(LogFaultException.class);
        Assertions.assertThat(cleanup(FULL, redundantBefore, SaveStatus.Stable)).isEqualTo(Cleanup.NO);
    }

    private static SomeStatus with(SomeStatus a, SomeStatus b)
    {
        return new SomeStatus(a.encoded | b.encoded);
    }

    private static RedundantBefore bounds(SomeStatus left, SomeStatus right)
    {
        return RedundantBefore.merge(entry(LEFT, left), entry(RIGHT, right));
    }

    private static RedundantBefore entry(Range range, SomeStatus status)
    {
        // SomeStatus.NONE describes a range we own with nothing redundant, which must be represented -
        // an absent entry would be NOT_OWNED
        if (status == SomeStatus.NONE)
            return RedundantBefore.create(Ranges.of(range), TxnId.NONE, SomeStatus.NONE);

        // a GC_BEFORE bound must be a shard bound
        TxnId bound = status.is(RedundantStatus.Property.GC_BEFORE) ? ABOVE.addFlag(Timestamp.Flag.SHARD_BOUND) : ABOVE;
        return RedundantBefore.create(Ranges.of(range), bound, status);
    }

    private static Cleanup cleanup(Cleanup.Input input, RedundantBefore redundantBefore, SaveStatus saveStatus)
    {
        return cleanup(input, redundantBefore, saveStatus, NotDurable);
    }

    private static Cleanup cleanup(Cleanup.Input input, RedundantBefore redundantBefore, SaveStatus saveStatus, Durability durability)
    {
        return Cleanup.shouldCleanup(input, TXN_ID, EXECUTE_AT, saveStatus, durability, PARTICIPANTS, redundantBefore, DurableBefore.EMPTY);
    }
}
