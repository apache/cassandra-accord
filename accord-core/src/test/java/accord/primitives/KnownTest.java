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

import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.primitives.Known.Definition;
import accord.primitives.Known.KnownDeps;
import accord.primitives.Known.KnownExecuteAt;
import accord.primitives.Known.KnownRoute;
import accord.primitives.Known.Outcome;
import accord.primitives.Known.PrivilegedVote;

import static accord.primitives.Known.KnownDeps.*;

/**
 * This file was authored by LLM
 * Known.serializeCompat() and KnownDeps.id are sent to peers, so must be stable; KnownDeps.ordinal() defines its
 * logical order, and Known.encoded is a purely local representation.
 */
class KnownTest
{
    /**
     * KnownDeps.id must never change. If this test fails, you have most likely inserted a KnownDeps without
     * allocating it an explicit new id.
     */
    private static final KnownDeps[] BY_ID = new KnownDeps[] {
        DepsUnknown,                 // 0
        DepsFromCoordinator,         // 1
        DepsProposedFixed,           // 2
        DepsProposed,                // 3
        DepsCommitted,               // 4
        DepsErased,                  // 5
        DepsKnown,                   // 6
        NoDeps,                      // 7
        DepsProposedFromCoordinator, // 8
    };

    /**
     * The logical order of KnownDeps
     */
    private static final KnownDeps[] BY_ORDINAL = new KnownDeps[] {
        DepsUnknown, DepsFromCoordinator, DepsProposedFixed, DepsProposed, DepsProposedFromCoordinator,
        DepsCommitted, DepsErased, DepsKnown, NoDeps
    };

    @Test
    void idsAreStable()
    {
        for (int id = 0 ; id < BY_ID.length ; ++id)
        {
            Assertions.assertThat(BY_ID[id].id).describedAs("%s", BY_ID[id]).isEqualTo(id);
            Assertions.assertThat(KnownDeps.forId(id)).isSameAs(BY_ID[id]);
        }
        Assertions.assertThat(KnownDeps.values()).hasSize(BY_ID.length);
        Assertions.assertThat(KnownDeps.values()).containsExactly(BY_ORDINAL);
        for (KnownDeps deps : KnownDeps.values())
            Assertions.assertThat(KnownDeps.forOrdinal(deps.ordinal())).isSameAs(deps);
        Assertions.assertThatThrownBy(() -> KnownDeps.forId(-1)).isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> KnownDeps.forId(BY_ID.length)).isInstanceOf(IllegalArgumentException.class);
    }

    private static List<Known> all()
    {
        List<Known> result = new ArrayList<>();
        for (KnownRoute route : KnownRoute.values())
        for (Definition definition : Definition.values())
        for (KnownExecuteAt executeAt : KnownExecuteAt.values())
        for (KnownDeps deps : KnownDeps.values())
        for (Outcome outcome : Outcome.values())
        for (PrivilegedVote vote : PrivilegedVote.values())
        {
            try { result.add(new Known(route, definition, executeAt, deps, outcome, vote)); }
            catch (IllegalStateException invalidCombination) {}
        }
        return result;
    }

    /**
     * The serialized layout must never change (see Known)
     */
    private static int expectSerialized(Known known)
    {
        KnownDeps deps = known.deps();
        return known.route().ordinal()
               | known.definition().ordinal() << 2
               | known.executeAt().ordinal() << 4
               | (deps.id & 0x7) << 7
               | known.outcome().ordinal() << 10
               | known.privilegedVote().ordinal() << 13
               | (deps.id >>> 3) << 14;
    }

    private static void assertSame(Known decoded, Known known)
    {
        Assertions.assertThat(decoded).isEqualTo(known);
        Assertions.assertThat(decoded.encoded).isEqualTo(known.encoded);
        Assertions.assertThat(decoded.route()).isSameAs(known.route());
        Assertions.assertThat(decoded.definition()).isSameAs(known.definition());
        Assertions.assertThat(decoded.executeAt()).isSameAs(known.executeAt());
        Assertions.assertThat(decoded.deps()).isSameAs(known.deps());
        Assertions.assertThat(decoded.outcome()).isSameAs(known.outcome());
        Assertions.assertThat(decoded.privilegedVote()).isSameAs(known.privilegedVote());
    }

    @Test
    void encoding()
    {
        Assertions.assertThat(all()).isNotEmpty();
        for (Known known : all())
        {
            KnownDeps deps = known.deps();
            Assertions.assertThat(known.serializeCompat()).describedAs("%s", known).isEqualTo(expectSerialized(known));

            assertSame(new Known(known.encoded), known);
            assertSame(Known.deserializeCompat(known.serializeCompat()), known);
            Assertions.assertThat(known.is(deps)).isTrue();

            for (KnownDeps other : KnownDeps.values())
            {
                // the deps accessors compare by ordinal, not by id
                Assertions.assertThat(Integer.signum(known.compareTo(other))).isEqualTo(Integer.signum(deps.compareTo(other)));
                Assertions.assertThat(known.is(other)).isEqualTo(deps == other);
                Known with;
                try { with = known.with(other); }
                catch (IllegalStateException invalidCombination) { continue; }
                {
                    Assertions.assertThat(with.deps()).isSameAs(other);
                    Assertions.assertThat(with.route()).isSameAs(known.route());
                    Assertions.assertThat(with.definition()).isSameAs(known.definition());
                    Assertions.assertThat(with.executeAt()).isSameAs(known.executeAt());
                    Assertions.assertThat(with.outcome()).isSameAs(known.outcome());
                    Assertions.assertThat(with.privilegedVote()).isSameAs(known.privilegedVote());
                    Assertions.assertThat(with).isEqualTo(new Known(known.route(), known.definition(), known.executeAt(), other, known.outcome(), known.privilegedVote()));
                    Assertions.assertThat(Integer.signum(known.compareDeps(with))).isEqualTo(Integer.signum(deps.compareTo(other)));
                    Assertions.assertThat(known.equalDeps(with)).isEqualTo(deps == other);
                }
            }
            Assertions.assertThat(known.hasAnyDeps()).isEqualTo(deps.hasPreAcceptedOrProposedOrDecidedDeps());
        }
    }

    /**
     * Only KnownDeps introduced after the original layout use the extension bit
     */
    @Test
    void onlyNewDepsUseExtension()
    {
        int count = 0;
        for (Known known : all())
        {
            if (known.deps().id < 8)
            {
                Assertions.assertThat(known.serializeCompat() & (1 << 14)).isZero();
                continue;
            }
            Assertions.assertThat(known.serializeCompat() & (1 << 14)).isNotZero();
            ++count;
        }
        Assertions.assertThat(count).isPositive();
    }

    @Test
    void invalidDepsIdsAreRejected()
    {
        for (int id = 0 ; id < 16 ; ++id)
        {
            int serialized = (id & 0x7) << 7 | (id >>> 3) << 14;
            if (id < BY_ID.length) Assertions.assertThat(Known.deserializeCompat(serialized).deps()).isSameAs(BY_ID[id]);
            else Assertions.assertThatThrownBy(() -> Known.deserializeCompat(serialized)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void depsProposedFromCoordinator()
    {
        Assertions.assertThat(DepsProposedFromCoordinator).isGreaterThan(DepsProposed).isLessThan(DepsCommitted);
        Assertions.assertThat(DepsProposedFromCoordinator.phase).isSameAs(DepsProposed.phase);
        Assertions.assertThat(DepsProposedFromCoordinator.hasPreAcceptedOrProposedOrDecidedDeps()).isTrue();
        Assertions.assertThat(DepsProposedFromCoordinator.hasProposedOrDecidedDeps()).isTrue();
        Assertions.assertThat(DepsProposedFromCoordinator.hasProposedDeps()).isTrue();
        Assertions.assertThat(DepsProposedFromCoordinator.isKnownUntilExecuteAt()).isTrue();
        Assertions.assertThat(DepsProposedFromCoordinator.hasCommittedOrDecidedDeps()).isFalse();
        Assertions.assertThat(DepsProposedFromCoordinator.hasDecidedDeps()).isFalse();
        Assertions.assertThat(DepsProposedFromCoordinator.canProposeInvalidation()).isFalse();
        Assertions.assertThat(DepsProposedFixed.isKnownUntilExecuteAt()).isFalse();
        Assertions.assertThat(DepsProposed.isKnownUntilExecuteAt()).isFalse();
        for (KnownDeps deps : KnownDeps.values())
            Assertions.assertThat(deps.isKnownUntilExecuteAt()).describedAs("%s", deps).isEqualTo(deps == DepsProposedFromCoordinator || deps.hasCommittedOrDecidedDeps());

        Known coord = SaveStatus.AcceptedPrivCoord.known;
        Known slow = SaveStatus.AcceptedSlowWithDefinition.known;
        Known fixed = SaveStatus.AcceptedMediumWithDefinition.known;
        Assertions.assertThat(coord.deps()).isSameAs(DepsProposedFromCoordinator);
        Assertions.assertThat(slow.isSatisfiedBy(coord)).isTrue();
        Assertions.assertThat(fixed.isSatisfiedBy(coord)).isTrue();
        Assertions.assertThat(coord.isSatisfiedBy(slow)).isFalse();
        Assertions.assertThat(coord.atLeast(slow)).isEqualTo(coord);
        Assertions.assertThat(coord.atLeast(fixed)).isEqualTo(coord);
        Assertions.assertThat(coord.min(slow)).isEqualTo(slow);
        Assertions.assertThat(coord.reduce(slow)).isEqualTo(slow);
    }
}
