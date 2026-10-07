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

package accord.primitives;

import org.junit.jupiter.api.Test;

import org.assertj.core.api.Assertions;

import static accord.primitives.SaveStatus.*;

/**
 * This file was authored by LLM
 */
class SaveStatusTest
{
    /**
     * SaveStatus.id is persisted and sent to peers, so must never change. If this test fails, you have most likely
     * reordered or inserted a SaveStatus without allocating it an explicit new id.
     */
    private static final SaveStatus[] BY_ID = new SaveStatus[] {
        Uninitialised,                    // 0
        NotDefined,                       // 1
        PreAccepted,                      // 2
        PreAcceptedWithVote,              // 3
        PreAcceptedWithDeps,              // 4
        AcceptedInvalidate,               // 5
        AcceptedInvalidateWithDefinition, // 6
        AcceptedMedium,                   // 7
        AcceptedMediumWithDefinition,     // 8
        AcceptedMediumWithDefAndVote,     // 9
        AcceptedSlow,                     // 10
        AcceptedSlowWithDefinition,       // 11
        AcceptedSlowWithDefAndVote,       // 12
        PreCommitted,                     // 13
        PreCommittedWithDefinition,       // 14
        PreCommittedWithDeps,             // 15
        PreCommittedWithFixedDeps,        // 16
        PreCommittedWithDefAndDeps,       // 17
        PreCommittedWithDefAndFixedDeps,  // 18
        Committed,                        // 19
        Stable,                           // 20
        ReadyToExecute,                   // 21
        PreApplied,                       // 22
        Applying,                         // 23
        Applied,                          // 24
        TruncatedApplyWithOutcome,        // 25
        TruncatedApply,                   // 26
        TruncatedUnapplied,               // 27
        Vestigial,                        // 28
        Erased,                           // 29
        Invalidated,                      // 30
        PreAcceptedPrivCoord,             // 31
        PreAcceptedPrivCoordRecovery,     // 32
        FastAcceptedPrivCoord,            // 33
        AcceptedPrivCoord,                // 34
        AcceptedPrivCoordRecovery,        // 35
    };

    @Test
    void idsAreStable()
    {
        Assertions.assertThat(SaveStatus.values()).hasSameSizeAs(BY_ID);
        for (int i = 0 ; i < BY_ID.length ; ++i)
        {
            Assertions.assertThat(BY_ID[i].id).describedAs("%s", BY_ID[i]).isEqualTo(i);
            Assertions.assertThat(SaveStatus.forId(i)).isSameAs(BY_ID[i]);
        }
    }

    @Test
    void lookup()
    {
        for (SaveStatus saveStatus : SaveStatus.values())
            Assertions.assertThat(SaveStatus.forOrdinal(saveStatus.ordinal())).isSameAs(saveStatus);
        Assertions.assertThatThrownBy(() -> SaveStatus.forOrdinal(SaveStatus.values().length)).isInstanceOf(IndexOutOfBoundsException.class);
        Assertions.assertThatThrownBy(() -> SaveStatus.forId(BY_ID.length)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void report()
    {
        for (SaveStatus saveStatus : SaveStatus.values())
        {
            SaveStatus shared = saveStatus.report();
            Assertions.assertThat(shared.status).isEqualTo(saveStatus.status);
            Assertions.assertThat(shared.report()).isSameAs(shared);
        }
    }

    @Test
    void privilegedCoordinator()
    {
        for (SaveStatus saveStatus : SaveStatus.values())
        {
            // never derived from Known, as it is a local distinction only
            if (!saveStatus.is(Status.Invalidated) && !saveStatus.is(Status.Truncated))
                Assertions.assertThat(SaveStatus.get(saveStatus.status, saveStatus.known).isPrivilegedCoordinator()).isFalse();

            // ordered with the other statuses of the same Status
            for (SaveStatus other : SaveStatus.values())
            {
                if (saveStatus.status.compareTo(other.status) < 0)
                    Assertions.assertThat(saveStatus).describedAs("%s vs %s", saveStatus, other).isLessThan(other);
            }
        }
        Assertions.assertThat(PreAcceptedPrivCoord.isPrivilegedCoordinatorZeroBallot()).isTrue();
        Assertions.assertThat(AcceptedPrivCoord.isPrivilegedCoordinator()).isTrue();

        // AcceptedPrivCoord* know their proposed deps are those of the original coordinator; this is not reported.
        // As the proposal may be via the medium or slow path, the equivalent for peers is AcceptedSlowWithDefinition (DepsProposed)
        for (SaveStatus saveStatus : new SaveStatus[] { AcceptedPrivCoord, AcceptedPrivCoordRecovery })
        {
            Assertions.assertThat(saveStatus.is(Status.AcceptedSlow)).isTrue();
            Assertions.assertThat(saveStatus.known.deps()).isSameAs(Known.KnownDeps.DepsProposedFromCoordinator);
            Assertions.assertThat(saveStatus.report()).isSameAs(AcceptedSlowWithDefinition);
            Assertions.assertThat(saveStatus.known).isEqualTo(AcceptedSlowWithDefinition.known.with(Known.KnownDeps.DepsProposedFromCoordinator));
            Assertions.assertThat(SaveStatus.enrich(saveStatus, AcceptedSlowWithDefinition.known)).isSameAs(saveStatus);
            Assertions.assertThat(SaveStatus.enrich(saveStatus, AcceptedMediumWithDefinition.known)).isSameAs(saveStatus);
            // upon learning the decision, we (consistent with the PreCommitted deps statuses) only retain that deps were proposed
            Assertions.assertThat(SaveStatus.get(Status.PreCommitted, saveStatus.known.atLeast(PreCommittedWithDefinition.known))).isSameAs(PreCommittedWithDefAndDeps);
        }
        for (SaveStatus saveStatus : SaveStatus.values())
        {
            if (saveStatus.report() != saveStatus && !saveStatus.known.equals(saveStatus.report().known))
                Assertions.assertThat(saveStatus).isIn(AcceptedPrivCoord, AcceptedPrivCoordRecovery);
        }

        // enriching should not be possible with priv coord statuses, as we do not give them deps/votes
        Assertions.assertThatThrownBy(() -> SaveStatus.enrich(PreAcceptedPrivCoord, PreAcceptedWithDeps.known));
        Assertions.assertThatThrownBy(() -> SaveStatus.enrich(PreAcceptedPrivCoordRecovery, PreAcceptedWithDeps.known));
        Assertions.assertThatThrownBy(() -> SaveStatus.enrich(PreAcceptedPrivCoord, PreCommitted.known));
    }
}
