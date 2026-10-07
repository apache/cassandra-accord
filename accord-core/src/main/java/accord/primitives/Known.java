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

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.utils.Invariants;
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;

import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.Known.KnownExecuteAt.IS_EXECUTE_AT_KNOWN;
import static accord.primitives.Known.Outcome.WasApply;
import static accord.primitives.Known.PrivilegedVote.VotePreAccept;
import static accord.primitives.Known.PrivilegedVote.NoVote;
import static accord.primitives.Known.Definition.DefinitionErased;
import static accord.primitives.Known.Definition.DefinitionKnown;
import static accord.primitives.Known.Definition.DefinitionUnknown;
import static accord.primitives.Known.KnownDeps.DepsErased;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Known.KnownDeps.NoDeps;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtKnown;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtUnknown;
import static accord.primitives.Known.KnownExecuteAt.NoExecuteAt;
import static accord.primitives.Known.KnownRoute.FullRoute;
import static accord.primitives.Known.KnownRoute.MaybeRoute;
import static accord.primitives.Known.Outcome.Unknown;
import static accord.primitives.Status.Phase.Accept;
import static accord.primitives.Status.Phase.Cleanup;
import static accord.primitives.Status.Phase.Commit;
import static accord.primitives.Status.Phase.Execute;
import static accord.primitives.Status.Phase.Invalidate;
import static accord.primitives.Status.Phase.PreAccept;

/**
 * A vector of various facets of knowledge about, or required for, processing a transaction.
 * Each property is treated independently, there is no precedence relationship between them.
 * Each property's values are however ordered with respect to each other.
 * <p>
 * This information does not need to be consistent with
 * TODO (expected): migrate inner class accessor methods to efficient bitmask operations
 */
public class Known
{
    private static final int ROUTE_MASK = 0x3;
    private static final int DEFINITION_SHIFT = 2;
    private static final int DEFINITION_MASK = 0x3 << DEFINITION_SHIFT;
    private static final int EXECUTE_AT_SHIFT = 4;
    private static final int EXECUTE_AT_MASK = 0x7 << EXECUTE_AT_SHIFT;
    private static final int DEPS_SHIFT = 7;
    private static final int DEPS_MASK = 0xf << DEPS_SHIFT;
    private static final int OUTCOME_SHIFT = 11;
    private static final int OUTCOME_MASK = 0x7 << OUTCOME_SHIFT;
    private static final int PRIVILEGED_VOTE_SHIFT = 14;
    private static final int PRIVILEGED_VOTE_MASK = 1 << PRIVILEGED_VOTE_SHIFT;

    /*
     * {@link #encoded} is a purely local representation, and may change between versions. The representation we
     * serialize to peers (see {@link #serializeCompat}) must however NEVER change; it may only be extended:
     *
     *   bits 0-1:   KnownRoute.ordinal()
     *   bits 2-3:   Definition.ordinal()
     *   bits 4-6:   KnownExecuteAt.ordinal()
     *   bits 7-9:   KnownDeps.id & 0x7
     *   bits 10-12: Outcome.ordinal()
     *   bit  13:    PrivilegedVote.ordinal()
     *   bit  14:    KnownDeps.id >>> 3 (added after the above; ignored by earlier versions)
     */
    private static final int SERIALIZE_LOW_MASK = ROUTE_MASK | DEFINITION_MASK | EXECUTE_AT_MASK; // bits 0-6, identical to encoded
    private static final int SERIALIZE_SHIFTED_MASK = OUTCOME_MASK | PRIVILEGED_VOTE_MASK; // encoded bits 11-14, serialized as bits 10-13
    private static final int SERIALIZE_DEPS_LOW_SHIFT = 7;
    private static final int SERIALIZE_DEPS_LOW_MASK = 0x7;
    private static final int SERIALIZE_DEPS_HIGH_MASK = 0x8;
    private static final int SERIALIZE_DEPS_HIGH_SHIFT = 14 - 3;

    static
    {
        Invariants.require(KnownRoute.VALUES.length <= 1 + ROUTE_MASK);
        Invariants.require(Definition.VALUES.length <= 1 + (DEFINITION_MASK >>> DEFINITION_SHIFT));
        Invariants.require(KnownExecuteAt.VALUES.length <= 1 + (EXECUTE_AT_MASK >>> EXECUTE_AT_SHIFT));
        Invariants.require(Outcome.VALUES.length <= 1 + (OUTCOME_MASK >>> OUTCOME_SHIFT));
        Invariants.require(PrivilegedVote.values().length <= 2);
        Invariants.require(KnownDeps.BY_ORDINAL.length <= 1 + (DEPS_MASK >>> DEPS_SHIFT));
        Invariants.require(KnownDeps.BY_ID.length <= 1 + (SERIALIZE_DEPS_LOW_MASK | SERIALIZE_DEPS_HIGH_MASK));
    }

    public static final Known Nothing = new Known(MaybeRoute, DefinitionUnknown, ExecuteAtUnknown, DepsUnknown, Unknown, NoVote);
    public static final Known DefinitionOnly = new Known(MaybeRoute, DefinitionKnown, ExecuteAtUnknown, DepsUnknown, Unknown, NoVote);
    public static final Known DefinitionAndRoute = new Known(FullRoute, DefinitionKnown, ExecuteAtUnknown, DepsUnknown, Unknown, NoVote);
    public static final Known DepsOnly = new Known(MaybeRoute, DefinitionUnknown, ExecuteAtUnknown, DepsKnown, Unknown, NoVote);
    public static final Known Apply = new Known(FullRoute, DefinitionUnknown, ExecuteAtKnown, DepsKnown, Outcome.Apply, NoVote);
    public static final Known Invalidated = new Known(MaybeRoute, DefinitionUnknown, ExecuteAtUnknown, DepsUnknown, Outcome.Abort, NoVote);

    public final int encoded;

    public Known(Known copy)
    {
        this(copy.route(), copy.definition(), copy.executeAt(), copy.deps(), copy.outcome(), copy.privilegedVote());
    }

    public Known(KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome, PrivilegedVote privilegedVote)
    {
        encoded = encode(route, definition, executeAt, deps, outcome, privilegedVote);
        checkInvariants();
    }

    public Known(int encoded)
    {
        this.encoded = encoded;
        checkInvariants();
    }

    private Known withEncoded(int encoded)
    {
        return encoded == this.encoded ? this : new Known(encoded);
    }

    private static int encode(KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome, PrivilegedVote privilegedVote)
    {
        return route.ordinal()
             | (definition.ordinal() << DEFINITION_SHIFT)
             | (executeAt.ordinal() << EXECUTE_AT_SHIFT)
             | (deps.ordinal() << DEPS_SHIFT)
             | (outcome.ordinal() << OUTCOME_SHIFT)
             | (privilegedVote.ordinal() << PRIVILEGED_VOTE_SHIFT);
    }

    public int serializeCompat()
    {
        return serializeCompatInternal(encoded);
    }

    public static Known deserializeCompat(int serialized)
    {
        return new Known(deserializeCompatInternal(serialized));
    }

    private static int serializeCompatInternal(int encoded)
    {
        int depsId = KnownDeps.ORDINAL_TO_ID[depsOrdinal(encoded)];
        return    (encoded & SERIALIZE_LOW_MASK)
               | ((encoded & SERIALIZE_SHIFTED_MASK) >>> 1)
               | ((depsId & SERIALIZE_DEPS_LOW_MASK) << SERIALIZE_DEPS_LOW_SHIFT)
               | ((depsId & SERIALIZE_DEPS_HIGH_MASK) << SERIALIZE_DEPS_HIGH_SHIFT);
    }

    private static int deserializeCompatInternal(int serialized)
    {
        int depsId = ((serialized >>> SERIALIZE_DEPS_LOW_SHIFT) & SERIALIZE_DEPS_LOW_MASK)
                   | ((serialized >>> SERIALIZE_DEPS_HIGH_SHIFT) & SERIALIZE_DEPS_HIGH_MASK);
        int depsOrdinal = KnownDeps.forId(depsId).ordinal();
        return   (serialized & SERIALIZE_LOW_MASK)
               | (depsOrdinal << DEPS_SHIFT)
               | ((serialized & (SERIALIZE_SHIFTED_MASK >>> 1)) << 1);
    }

    public Known atLeast(Known that)
    {
        Invariants.requireArgument(compatibleWith(that));
        KnownRoute maxRoute = route().atLeast(that.route());
        Definition maxDefinition = definition().atLeast(that.definition());
        KnownExecuteAt maxExecuteAt = executeAt().atLeast(that.executeAt());
        KnownDeps maxDeps = deps().atLeast(that.deps());
        Outcome maxOutcome = outcome().atLeast(that.outcome());
        PrivilegedVote maxPrivilegedVote = max(privilegedVote(), that.privilegedVote());
        return selectOrCreate(that, maxRoute, maxDefinition, maxExecuteAt, maxDeps, maxOutcome, maxPrivilegedVote);
    }

    public static Known nonNullOrMin(Known a, Known b)
    {
        return a == null | b == null ? a == null ? b : a : a.min(b);
    }

    public Known min(Known that)
    {
        Invariants.requireArgument(compatibleWith(that));
        KnownRoute minRoute = min(route(), that.route());
        Definition minDefinition = min(definition(), that.definition());
        KnownExecuteAt minExecuteAt = min(executeAt(), that.executeAt());
        KnownDeps minDeps = min(deps(), that.deps());
        Outcome minOutcome = min(outcome(), that.outcome());
        PrivilegedVote minPrivilegedVote = min(privilegedVote(), that.privilegedVote());
        return selectOrCreate(that, minRoute, minDefinition, minExecuteAt, minDeps, minOutcome, minPrivilegedVote);
    }

    static <E extends Enum<E>> E min(E a, E b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }

    static <E extends Enum<E>> E max(E a, E b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    public Known reduce(Known that)
    {
        Invariants.requireArgument(compatibleWith(that));
        KnownRoute maxRoute = route().reduce(that.route());
        Definition minDefinition = definition().reduce(that.definition());
        KnownExecuteAt maxExecuteAt = executeAt().reduce(that.executeAt());
        KnownDeps minDeps = deps().reduce(that.deps());
        Outcome maxOutcome = outcome().reduce(that.outcome());
        PrivilegedVote minPrivilegedVote = min(privilegedVote(), that.privilegedVote());
        return selectOrCreate(that, maxRoute, minDefinition, maxExecuteAt, minDeps, maxOutcome, minPrivilegedVote);
    }

    public Known validForAll()
    {
        KnownRoute maxRoute = route().validForAll();
        Definition minDefinition = definition().validForAll();
        KnownExecuteAt maxExecuteAt = executeAt().validForAll();
        KnownDeps minDeps = deps().validForAll();
        Outcome maxOutcome = outcome().validForAll();
        return selectOrCreate(maxRoute, minDefinition, maxExecuteAt, minDeps, maxOutcome, NoVote);
    }

    boolean compatibleWith(Known that)
    {
        return executeAt().compatibleWith(that.executeAt())
               && deps().compatibleWith(that.deps())
               && outcome().compatibleWith(that.outcome());
    }

    @Nonnull
    private Known selectOrCreate(KnownRoute newRoute, Definition newDefinition, KnownExecuteAt newExecuteAt, KnownDeps newDeps, Outcome newOutcome, PrivilegedVote newPrivilegedVote)
    {
        return withEncoded(encode(newRoute, newDefinition, newExecuteAt, newDeps, newOutcome, newPrivilegedVote));
    }

    @Nonnull
    private Known selectOrCreate(Known with, KnownRoute newRoute, Definition newDefinition, KnownExecuteAt newExecuteAt, KnownDeps newDeps, Outcome newOutcome, PrivilegedVote newPrivilegedVote)
    {
        int encoded = encode(newRoute, newDefinition, newExecuteAt, newDeps, newOutcome, newPrivilegedVote);
        return encoded == this.encoded ? this : encoded == with.encoded ? with : new Known(encoded);
    }

    public boolean isSatisfiedBy(Known that)
    {
        return    compareTo(that.definition()) <= 0
               && compareTo(that.executeAt()) <= 0
               && compareTo(that.deps()) <= 0
               && outcome().isSatisfiedBy(that.outcome());
    }

    /**
     * The logical epoch on which the specified knowledge is best sought or sent.
     * i.e., if we include an outcome then the execution epoch
     */
    public LogicalEpoch epoch()
    {
        if (outcome().isOrWasApply())
            return LogicalEpoch.Execution;

        return LogicalEpoch.Coordination;
    }

    public long fetchEpoch(TxnId txnId, @Nullable Timestamp executeAt)
    {
        if (executeAt == null)
            return txnId.epoch();

        if (outcome().isOrWasApply() && !executeAt.equals(Timestamp.NONE))
            return executeAt.epoch();

        return txnId.epoch();
    }

    public Known with(KnownRoute newKnownRoute)
    {
        return withEncoded((this.encoded & ~ROUTE_MASK) | newKnownRoute.ordinal());
    }

    public Known with(Outcome newOutcome)
    {
        return withEncoded((this.encoded & ~OUTCOME_MASK) | newOutcome.ordinal() << OUTCOME_SHIFT);
    }

    public Known with(Definition newDefinition)
    {
        return withEncoded((this.encoded & ~DEFINITION_MASK) | (newDefinition.ordinal() << DEFINITION_SHIFT));
    }

    public Known with(KnownDeps newDeps)
    {
        return withEncoded((this.encoded & ~DEPS_MASK) | (newDeps.ordinal() << DEPS_SHIFT));
    }

    public Known with(PrivilegedVote newPrivilegedVote)
    {
        return withEncoded((this.encoded & ~PRIVILEGED_VOTE_MASK) | (newPrivilegedVote.ordinal() << PRIVILEGED_VOTE_SHIFT));
    }

    /**
     * Convert this Known to one that represents the knowledge that can be propagated to another replica without
     * further information available to that replica, e.g. we cannot apply without also knowing the definition.
     */
    public Known propagates()
    {
        return propagatesSaveStatus().known;
    }

    public SaveStatus propagatesSaveStatus()
    {
        if (isInvalidated())
            return SaveStatus.Invalidated;

        if (!has(FullRoute))
            return SaveStatus.NotDefined;

        if (definition() == DefinitionUnknown || definition() == DefinitionErased)
        {
            if (isExecuteAtKnown())
                return SaveStatus.PreCommitted;
            return SaveStatus.NotDefined;
        }

        if (!isExecuteAtKnown())
            return SaveStatus.PreAccepted;

        // cannot propagate proposed deps; and cannot propagate known deps without executeAt
        KnownDeps deps = this.deps();
        if (!deps.hasDecidedDeps())
            return SaveStatus.PreCommittedWithDefinition;

        switch (outcome())
        {
            default: throw new UnhandledEnum(outcome());
            case Apply:
                if (is(ApplyAtKnown))
                    return SaveStatus.PreApplied;

            case Unknown:
            case WasApply:
            case Erased:
                return SaveStatus.Stable;

        }
    }

    public boolean isDefinitionKnown()
    {
        return is(DefinitionKnown);
    }

    /**
     * The command may have an incomplete route when this is false
     */
    public boolean hasFullRoute()
    {
        return definition().isKnown() || outcome().isOrWasApply();
    }

    public boolean isTruncated()
    {
        switch (outcome())
        {
            default:
                throw new UnhandledEnum(outcome());
            case Abort:
            case Unknown:
                return false;
            case Apply:
                return deps() == DepsErased || definition() == DefinitionErased;
            case Erased:
            case WasApply:
                return true;
        }
    }

    public boolean canProposeInvalidation()
    {
        return deps().canProposeInvalidation() && executeAt().canProposeInvalidation() && outcome().canProposeInvalidation();
    }

    public Known subtract(Known subtract)
    {
        if (!subtract.isSatisfiedBy(this))
            return Known.Nothing;

        Definition newDefinition = subtract.definition().compareTo(definition()) >= 0 ? DefinitionUnknown : definition();
        KnownExecuteAt newExecuteAt = subtract.executeAt().compareTo(executeAt()) >= 0 ? ExecuteAtUnknown : executeAt();
        KnownDeps newDeps = subtract.deps().compareTo(deps()) >= 0 ? DepsUnknown : deps();
        Outcome newOutcome = outcome().subtract(subtract.outcome());
        PrivilegedVote newPrivilegedVote = privilegedVote().compareTo(subtract.privilegedVote()) > 0 ? VotePreAccept : NoVote;
        return new Known(route(), newDefinition, newExecuteAt, newDeps, newOutcome, newPrivilegedVote);
    }

    public boolean isDecided()
    {
        return executeAt().isDecided() || outcome().isDecided();
    }

    public boolean isDecidedToExecute()
    {
        return isExecuteAtKnown() || outcome().isOrWasApply();
    }

    public String toString()
    {
        return Stream.of(route() == MaybeRoute ? null : route().toString(),
                         definition() == DefinitionUnknown ? null : definition().toString().replace("Known", ""),
                         executeAt() == ExecuteAtUnknown ? null : executeAt().toString().replace("Known", ""),
                         deps() == DepsUnknown ? null : deps().toString().replace("Known", ""),
                         outcome() == Unknown ? null : outcome().toString()
        ).filter(Objects::nonNull).collect(Collectors.joining(",", "[", "]"));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return equals((Known) o);
    }

    // ignores class identity
    boolean equals(Known that)
    {
        return encoded == that.encoded;
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    public boolean hasDefinition()
    {
        return is(DefinitionKnown);
    }

    public boolean hasDecidedDeps()
    {
        return deps().hasDecidedDeps();
    }

    public boolean isInvalidated()
    {
        return outcome().isInvalidated();
    }

    public void checkInvariants()
    {
        if (outcome().isInvalidated()) Invariants.require(deps() != DepsKnown && executeAt() != ExecuteAtKnown);
        else if (outcome().isOrWasApply()) Invariants.require(deps() != NoDeps && executeAt() != NoExecuteAt);
        Invariants.require(!isDefinitionKnown() || hasFullRoute());
    }

    public KnownRoute route()
    {
        return KnownRoute.VALUES[encoded & ROUTE_MASK];
    }

    public boolean has(KnownRoute route)
    {
        return (encoded & ROUTE_MASK) == route.ordinal();
    }

    public int compareTo(KnownRoute route)
    {
        return (encoded & ROUTE_MASK) - route.ordinal();
    }

    private int definitionOrdinal()
    {
        return (encoded & DEFINITION_MASK) >>> DEFINITION_SHIFT;
    }

    public Definition definition()
    {
        return Definition.VALUES[definitionOrdinal()];
    }

    public boolean is(Definition definition)
    {
        return definitionOrdinal() == definition.ordinal();
    }

    public int compareTo(Definition definition)
    {
        return definitionOrdinal() - definition.ordinal();
    }

    public boolean equalDefinition(Known that)
    {
        return definitionOrdinal() == that.definitionOrdinal();
    }

    public int compareDefinition(Known that)
    {
        return definitionOrdinal() - that.definitionOrdinal();
    }

    private int executeAtOrdinal()
    {
        return (encoded & EXECUTE_AT_MASK) >> EXECUTE_AT_SHIFT;
    }

    public KnownExecuteAt executeAt()
    {
        return KnownExecuteAt.VALUES[executeAtOrdinal()];
    }

    public boolean is(KnownExecuteAt executeAt)
    {
        return executeAtOrdinal() == executeAt.ordinal();
    }

    public boolean isExecuteAtKnown()
    {
        return TinyEnumSet.contains(IS_EXECUTE_AT_KNOWN, executeAtOrdinal());
    }

    public int compareTo(KnownExecuteAt executeAt)
    {
        return executeAtOrdinal() - executeAt.ordinal();
    }

    public boolean equalExecuteAt(Known that)
    {
        return executeAtOrdinal() == that.executeAtOrdinal();
    }

    public int compareExecuteAt(Known that)
    {
        return executeAtOrdinal() - that.executeAtOrdinal();
    }

    private int depsOrdinal()
    {
        return depsOrdinal(encoded);
    }

    private static int depsOrdinal(int encoded)
    {
        return (encoded & DEPS_MASK) >>> DEPS_SHIFT;
    }

    public KnownDeps deps()
    {
        return KnownDeps.BY_ORDINAL[depsOrdinal()];
    }

    public boolean hasAnyDeps()
    {
        return TinyEnumSet.contains(KnownDeps.HAS_ANY, depsOrdinal());
    }

    public boolean is(KnownDeps deps)
    {
        return depsOrdinal() == deps.ordinal();
    }

    public int compareTo(KnownDeps deps)
    {
        return depsOrdinal() - deps.ordinal();
    }

    public boolean equalDeps(Known that)
    {
        return depsOrdinal() == that.depsOrdinal();
    }

    public int compareDeps(Known that)
    {
        return depsOrdinal() - that.depsOrdinal();
    }
    
    private int outcomeOrdinal()
    {
        return (encoded & OUTCOME_MASK) >>> OUTCOME_SHIFT;
    }

    public Outcome outcome()
    {
        return Outcome.VALUES[outcomeOrdinal()];
    }

    public boolean isOrWasApply()
    {
        return is(Outcome.Apply) || is(WasApply);
    }

    public boolean is(Outcome outcome)
    {
        return outcomeOrdinal() == outcome.ordinal();
    }

    public int compareTo(Outcome outcome)
    {
        return outcomeOrdinal() - outcome.ordinal();
    }

    public boolean equalOutcome(Known that)
    {
        return outcomeOrdinal() == that.outcomeOrdinal();
    }

    public int compareOutcome(Known that)
    {
        return outcomeOrdinal() - that.outcomeOrdinal();
    }

    public boolean is(PrivilegedVote privilegedVote)
    {
        return privilegedOrdinal() == privilegedVote.ordinal();
    }

    /**
     * This represents a vote by both the replica in question AND the coordinator (if different).
     */
    public PrivilegedVote privilegedVote()
    {
        return privilegedOrdinal() == 0 ? NoVote : VotePreAccept;
    }

    public boolean hasPrivilegedVote()
    {
        return privilegedVote() == VotePreAccept;
    }

    private int privilegedOrdinal()
    {
        return (encoded & PRIVILEGED_VOTE_MASK) >>> PRIVILEGED_VOTE_SHIFT;
    }

    public enum KnownRoute
    {
        /**
         * A route may or may not be known, but it may not cover (or even intersect) this shard.
         * The route should be relied upon only if it is a FullRoute.
         */
        MaybeRoute,

        /**
         * A route is known that covers the ranges this shard participates in.
         * Note that if the status is less than Committed, this may not be the final set of owned ranges,
         * and the route may not cover whatever this is decided as.
         *
         * This status primarily exists to communicate semantically to the reader.
         */
        CoveringRoute,

        /**
         * The full route is known. <i>Generally</i> this coincides with knowing the Definition.
         */
        FullRoute
        ;

        private static final KnownRoute[] VALUES = values();

        public KnownRoute reduce(KnownRoute that)
        {
            if (this == that) return this;
            if (this == FullRoute || that == FullRoute) return FullRoute;
            return MaybeRoute;
        }

        public KnownRoute validForAll()
        {
            return this == CoveringRoute ? MaybeRoute : this;
        }

        public KnownRoute atLeast(KnownRoute that)
        {
            return this.compareTo(that) >= 0 ? this : that;
        }
    }

    public enum KnownExecuteAt
    {
        /**
         * No decision is known to have been reached. If executeAt is not null, it represents either when
         * the transaction was witnessed, or some earlier ExecuteAtProposed that was invalidated by AcceptedInvalidate
         */
        ExecuteAtUnknown,

        /**
         * A decision to execute the transaction is known to have been proposed, and the associated executeAt timestamp
         */
        ExecuteAtProposed,

        /**
         * A decision to execute or invalidate the transaction is known to have been reached and since been cleaned up
         */
        ExecuteAtErased,

        /**
         * A decision to execute the transaction is known to have been reached, and the associated executeAt timestamp
         */
        ExecuteAtKnown,

        /**
         * A decision to execute the transaction is known to have been reached, and the associated executeAt timestamp
         * as well as any uniqueHlc that may be associated with it
         */
        ApplyAtKnown,

        /**
         * A decision to invalidate the transaction is known to have been reached
         */
        NoExecuteAt
        ;

        static final int IS_EXECUTE_AT_KNOWN = TinyEnumSet.encode(ExecuteAtKnown, ApplyAtKnown);
        private static final KnownExecuteAt[] VALUES = values();

        /**
         * Is known to have agreed to execute or not; but the decision is not known (maybe erased)
         */
        public boolean isDecided()
        {
            return compareTo(ExecuteAtErased) >= 0;
        }

        /**
         * Is known to have agreed to execute or not; but the decision is not known (maybe erased)
         */
        public boolean hasDecision()
        {
            return compareTo(ExecuteAtKnown) >= 0;
        }

        /**
         * Is known to execute, and when.
         */
        public boolean isKnown()
        {
            return this == ExecuteAtKnown | this == ApplyAtKnown;
        }

        public KnownExecuteAt atLeast(KnownExecuteAt that)
        {
            return compareTo(that) >= 0 ? this : that;
        }

        public KnownExecuteAt reduce(KnownExecuteAt that)
        {
            return atLeast(that);
        }

        public KnownExecuteAt validForAll()
        {
            return compareTo(ExecuteAtErased) <= 0 ? ExecuteAtUnknown : this;
        }

        public boolean canProposeInvalidation()
        {
            return this == ExecuteAtUnknown;
        }

        public boolean compatibleWith(KnownExecuteAt that)
        {
            if (this == that) return true;
            int c = compareTo(that);
            KnownExecuteAt max = c >= 0 ? this : that;
            KnownExecuteAt min = c <= 0 ? this : that;
            return max != NoExecuteAt || (min != ExecuteAtKnown && min != ApplyAtKnown);
        }
    }

    public enum KnownDeps
    {
        /**
         * No decision is known to have been reached
         */
        DepsUnknown(0, PreAccept),

        /**
         * No decision is known to have been reached, but we have dependencies broadcast from the original coordinator
         * (or its proxy) that may be used for recovery decisions.
         */
        DepsFromCoordinator(1, PreAccept),

        /**
         * A decision to execute the transaction at the original timestamp (TxnId) is known to have been proposed,
         * and the associated dependencies for the shard(s) in question are known.
         *
         * Proposed means Accepted at some replica, but not necessarily a majority and so not committed
         */
        DepsProposedFixed(2, Accept),

        /**
         * A decision to execute the transaction at a given timestamp is known to have been proposed,
         * and the associated dependencies for the shard(s) in question are known for the coordination epoch (txnId.epoch) only.
         *
         * Proposed means Accepted at some replica, but not necessarily a majority and so not committed
         */
        DepsProposed(3, Accept),

        /**
         * Deps proposed by a privileged coordinator during the Accept phase, including the coordinator's own vote for
         * dependencies up to executeAt. This permits the coordinator to take the fast quorum medium path
         * (i.e. skipping Commit phase)
         */
        DepsProposedFromCoordinator(8, Accept),

        /**
         * A decision to execute the transaction at a given timestamp with certain dependencies is known to have been proposed,
         * and some associated dependencies for the shard(s) in question have been committed.
         *
         * However, the dependencies are only known to committed at a replica, not necessarily a majority, i.e. not stable
         */
        DepsCommitted(4, Commit),

        /**
         * A decision to execute or invalidate the transaction is known to have been reached, and any associated
         * dependencies for the shard(s) in question have been cleaned up.
         */
        DepsErased(5, Cleanup),

        /**
         * A decision to execute the transaction is known to have been reached, and the associated dependencies
         * for the shard(s) in question are known for the coordination and execution epochs, and are stable.
         */
        DepsKnown(6, Execute),

        /**
         * A decision to invalidate the transaction is known to have been reached
         */
        NoDeps(7, Invalidate);

        private static final int HAS_ANY = TinyEnumSet.encode(DepsFromCoordinator, DepsProposed, DepsProposedFixed, DepsProposedFromCoordinator, DepsCommitted, DepsKnown);
        private static final KnownDeps[] BY_ORDINAL = values();
        private static final KnownDeps[] BY_ID = new KnownDeps[BY_ORDINAL.length];
        private static final int[] ORDINAL_TO_ID = new int[BY_ORDINAL.length];

        static
        {
            for (KnownDeps deps : BY_ORDINAL)
            {
                Invariants.require(deps.id >= 0 && deps.id < BY_ID.length, "KnownDeps id %s (%s) must be in the range [0..%s)", (Object) deps.id, deps, BY_ID.length); // can be relaxed in future if necessary
                Invariants.require(BY_ID[deps.id] == null, "Duplicate KnownDeps id %d (%s, %s)", deps.id, deps, BY_ID[deps.id]);
                BY_ID[deps.id] = deps;
                ORDINAL_TO_ID[deps.ordinal()] = deps.id;
            }
        }

        public final int id;
        public final Status.Phase phase;

        KnownDeps(int id, Status.Phase phase)
        {
            this.id = id;
            this.phase = phase;
        }

        public boolean canProposeInvalidation()
        {
            return this == DepsUnknown;
        }

        public boolean hasPreAcceptedOrProposedOrDecidedDeps()
        {
            return TinyEnumSet.contains(HAS_ANY, this);
        }

        public boolean hasProposedOrDecidedDeps()
        {
            switch (this)
            {
                default: throw new UnhandledEnum(this);
                case DepsProposedFixed:
                case DepsProposed:
                case DepsProposedFromCoordinator:
                case DepsCommitted:
                case DepsKnown:
                    return true;
                case DepsFromCoordinator:
                case NoDeps:
                case DepsErased:
                case DepsUnknown:
                    return false;
            }
        }

        public boolean hasProposedDeps()
        {
            switch (this)
            {
                default: throw new UnhandledEnum(this);
                case DepsProposedFixed:
                case DepsProposed:
                case DepsProposedFromCoordinator:
                    return true;
                case DepsCommitted:
                case DepsKnown:
                case NoDeps:
                case DepsErased:
                case DepsUnknown:
                    return false;
            }
        }

        /**
         * @return true iff any dependencies are known (for this replica) to be complete until executeAt;
         * otherwise they may only be known to be complete until TxnId
         */
        public boolean isKnownUntilExecuteAt()
        {
            switch (this)
            {
                default: throw new UnhandledEnum(this);
                case DepsProposedFromCoordinator:
                case DepsCommitted:
                case DepsKnown:
                    return true;
                case DepsUnknown:
                case DepsFromCoordinator:
                case DepsProposedFixed:
                case DepsProposed:
                case DepsErased:
                case NoDeps:
                    return false;
            }
        }

        public boolean hasDecidedDeps()
        {
            return this == DepsKnown;
        }

        public boolean hasCommittedOrDecidedDeps()
        {
            return this == DepsCommitted || this == DepsKnown;
        }

        public KnownDeps atLeast(KnownDeps that)
        {
            return compareTo(that) >= 0 ? this : that;
        }

        public KnownDeps reduce(KnownDeps that)
        {
            return compareTo(that) <= 0 ? this : that;
        }

        public KnownDeps validForAll()
        {
            return this == NoDeps ? NoDeps : DepsUnknown;
        }

        public boolean compatibleWith(KnownDeps that)
        {
            if (this == that) return true;
            int c = compareTo(that);
            KnownDeps max = c >= 0 ? this : that;
            KnownDeps min = c <= 0 ? this : that;
            return max != NoDeps || (min != DepsCommitted && min != DepsKnown);
        }

        public static KnownDeps forId(int id)
        {
            if (id < 0 || id >= BY_ID.length)
                throw new IllegalArgumentException("Unknown KnownDeps id " + id);
            return BY_ID[id];
        }

        public static KnownDeps forOrdinal(int ordinal)
        {
            return BY_ORDINAL[ordinal];
        }
    }

    /**
     * Whether the transaction's definition is known.
     */
    public enum Definition
    {
        /**
         * The definition is not known
         */
        DefinitionUnknown,

        /**
         * The definition was perhaps known previously, but has since been erased
         */
        DefinitionErased,

        /**
         * The definition is irrelevant, as the transaction has been invalidated and may be treated as a no-op
         */
        NoOp,

        /**
         * The definition is known
         *
         * TODO (expected, clarity): distinguish between known for coordination epoch and known for commit/execute
         */
        DefinitionKnown;

        private static final Definition[] VALUES = values();

        public boolean isKnown()
        {
            return this == DefinitionKnown;
        }

        public boolean isOrWasKnown()
        {
            return this != DefinitionUnknown;
        }

        public Definition atLeast(Definition that)
        {
            return compareTo(that) >= 0 ? this : that;
        }

        // combine info about two shards into a composite representation
        public Definition reduce(Definition that)
        {
            return compareTo(that) <= 0 ? this : that;
        }

        public Definition validForAll()
        {
            return this == NoOp ? NoOp : DefinitionUnknown;
        }
    }

    /**
     * Whether a transaction's outcome (and its application) is known
     */
    public enum Outcome
    {
        /**
         * The outcome is not yet known (and may not yet be decided)
         */
        Unknown,

        /**
         * The transaction has been *completely cleaned up* - this means it has been made
         * durable at every live replica of every shard we contacted (or was Invalidated)
         */
        Erased,

        /**
         * The transaction has been cleaned-up, but was applied and the relevant portion of its outcome has been cleaned up
         */
        WasApply,

        /**
         * The outcome is known
         */
        Apply,

        /**
         * The transaction is known to have been invalidated
         */
        Abort
        ;

        private static final Outcome[] VALUES = values();

        public boolean isOrWasApply()
        {
            return this == Apply || this == WasApply;
        }

        public boolean isSatisfiedBy(Outcome other)
        {
            switch (this)
            {
                default: throw new AssertionError();
                case Unknown:
                    return true;
                case WasApply:
                    if (other == Apply)
                        return true;
                case Apply:
                case Abort:
                case Erased:
                    return other == this;
            }
        }

        public boolean canProposeInvalidation()
        {
            return this == Unknown;
        }

        public boolean isInvalidated()
        {
            return this == Abort;
        }

        public Outcome atLeast(Outcome that)
        {
            return this.compareTo(that) >= 0 ? this : that;
        }

        // outcomes are universal - any replica of any shard may propagate its outcome to any other replica of any other shard
        public Outcome reduce(Outcome that)
        {
            return atLeast(that);
        }

        /**
         * Do not imply truncation where none has happened
         */
        public Outcome validForAll()
        {
            return compareTo(WasApply) <= 0 ? Unknown : this;
        }

        public Outcome subtract(Outcome that)
        {
            return this.compareTo(that) <= 0 ? Unknown : this;
        }

        public boolean isDecided()
        {
            return this != Unknown;
        }

        public boolean compatibleWith(Outcome that)
        {
            if (this == that) return true;
            int c = compareTo(that);
            Outcome max = c >= 0 ? this : that;
            Outcome min = c <= 0 ? this : that;
            return max != Abort || (min != Apply && min != WasApply);
        }
    }

    public enum PrivilegedVote
    {
        NoVote, VotePreAccept
    }

    public enum LogicalEpoch
    {
        Coordination, Execution
    }
}
