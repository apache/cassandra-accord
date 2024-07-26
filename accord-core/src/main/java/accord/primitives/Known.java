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

import static accord.primitives.Known.Definition.DefinitionKnown;
import static accord.primitives.Known.Definition.DefinitionUnknown;
import static accord.primitives.Known.KnownDeps.DepsErased;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Known.KnownDeps.NoDeps;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtKnown;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtUnknown;
import static accord.primitives.Known.KnownExecuteAt.NoExecuteAt;
import static accord.primitives.Known.KnownRoute.Full;
import static accord.primitives.Known.KnownRoute.Maybe;
import static accord.primitives.Known.Outcome.Unknown;
import static accord.primitives.Status.Phase.Accept;
import static accord.primitives.Status.Phase.Cleanup;
import static accord.primitives.Status.Phase.Commit;
import static accord.primitives.Status.Phase.Execute;
import static accord.primitives.Status.Phase.Persist;
import static accord.primitives.Status.Phase.PreAccept;

/**
 * A vector of various facets of knowledge about, or required for, processing a transaction.
 * Each property is treated independently, there is no precedence relationship between them.
 * Each property's values are however ordered with respect to each other.
 * <p>
 * This information does not need to be consistent with
 */
public class Known
{
    public static final Known Nothing = new Known(Maybe, DefinitionUnknown, ExecuteAtUnknown, DepsUnknown, Unknown);
    public static final Known DefinitionAndRoute = new Known(Full, DefinitionKnown, ExecuteAtUnknown, DepsUnknown, Unknown);
    public static final Known Apply = new Known(Full, DefinitionUnknown, ExecuteAtKnown, DepsKnown, Outcome.Apply);
    public static final Known Invalidated = new Known(Maybe, DefinitionUnknown, ExecuteAtUnknown, DepsUnknown, Outcome.Invalidated);

    // TODO (desired): pack into an integer
    public final KnownRoute route;
    public final Definition definition;
    public final KnownExecuteAt executeAt;
    public final KnownDeps deps;
    public final Outcome outcome;

    public Known(Known copy)
    {
        this.route = copy.route;
        this.definition = copy.definition;
        this.executeAt = copy.executeAt;
        this.deps = copy.deps;
        this.outcome = copy.outcome;
    }

    public Known(KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome)
    {
        this.route = route;
        this.definition = definition;
        this.executeAt = executeAt;
        this.deps = deps;
        this.outcome = outcome;
        checkInvariants();
    }

    public Known atLeast(Known that)
    {
        Invariants.checkArgument(compatibleWith(that));
        KnownRoute maxRoute = route.atLeast(that.route);
        Definition maxDefinition = definition.atLeast(that.definition);
        KnownExecuteAt maxExecuteAt = executeAt.atLeast(that.executeAt);
        KnownDeps maxDeps = deps.atLeast(that.deps);
        Outcome maxOutcome = outcome.atLeast(that.outcome);
        return selectOrCreate(that, maxRoute, maxDefinition, maxExecuteAt, maxDeps, maxOutcome);
    }

    public Known min(Known that)
    {
        Invariants.checkArgument(compatibleWith(that));
        KnownRoute minRoute = min(route, that.route);
        Definition minDefinition = min(definition, that.definition);
        KnownExecuteAt minExecuteAt = min(executeAt, that.executeAt);
        KnownDeps minDeps = min(deps, that.deps);
        Outcome minOutcome = min(outcome, that.outcome);
        return selectOrCreate(that, minRoute, minDefinition, minExecuteAt, minDeps, minOutcome);
    }

    static <E extends Enum<E>> E min(E a, E b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }

    public Known reduce(Known that)
    {
        Invariants.checkArgument(compatibleWith(that));
        KnownRoute maxRoute = route.reduce(that.route);
        Definition minDefinition = definition.reduce(that.definition);
        KnownExecuteAt maxExecuteAt = executeAt.reduce(that.executeAt);
        KnownDeps minDeps = deps.reduce(that.deps);
        Outcome maxOutcome = outcome.reduce(that.outcome);
        return selectOrCreate(that, maxRoute, minDefinition, maxExecuteAt, minDeps, maxOutcome);
    }

    public Known validForAll()
    {
        KnownRoute maxRoute = route.validForAll();
        Definition minDefinition = definition.validForAll();
        KnownExecuteAt maxExecuteAt = executeAt.validForAll();
        KnownDeps minDeps = deps.validForAll();
        Outcome maxOutcome = outcome.validForAll();
        return selectOrCreate(maxRoute, minDefinition, maxExecuteAt, minDeps, maxOutcome);
    }

    boolean compatibleWith(Known that)
    {
        return executeAt.compatibleWith(that.executeAt)
               && deps.compatibleWith(that.deps)
               && outcome.compatibleWith(that.outcome);
    }

    @Nonnull
    private Known selectOrCreate(Known with, KnownRoute maxRoute, Definition maxDefinition, KnownExecuteAt maxExecuteAt, KnownDeps maxDeps, Outcome maxOutcome)
    {
        if (maxRoute == with.route && maxDefinition == with.definition && maxExecuteAt == with.executeAt && maxDeps == with.deps && maxOutcome == with.outcome)
            return with;
        return selectOrCreate(maxRoute, maxDefinition, maxExecuteAt, maxDeps, maxOutcome);
    }

    @Nonnull
    private Known selectOrCreate(KnownRoute maxRoute, Definition maxDefinition, KnownExecuteAt maxExecuteAt, KnownDeps maxDeps, Outcome maxOutcome)
    {
        if (maxRoute == route && maxDefinition == definition && maxExecuteAt == executeAt && maxDeps == deps && maxOutcome == outcome)
            return this;
        return new Known(maxRoute, maxDefinition, maxExecuteAt, maxDeps, maxOutcome);
    }

    public boolean isSatisfiedBy(Known that)
    {
        return this.definition.compareTo(that.definition) <= 0
               && this.executeAt.compareTo(that.executeAt) <= 0
               && this.deps.compareTo(that.deps) <= 0
               && this.outcome.isSatisfiedBy(that.outcome);
    }

    /**
     * The logical epoch on which the specified knowledge is best sought or sent.
     * i.e., if we include an outcome then the execution epoch
     */
    public LogicalEpoch epoch()
    {
        if (outcome.isOrWasApply())
            return LogicalEpoch.Execution;

        return LogicalEpoch.Coordination;
    }

    public long fetchEpoch(TxnId txnId, @Nullable Timestamp executeAt)
    {
        if (executeAt == null)
            return txnId.epoch();

        if (outcome.isOrWasApply() && !executeAt.equals(Timestamp.NONE))
            return executeAt.epoch();

        return txnId.epoch();
    }

    public Known with(Outcome newOutcome)
    {
        if (outcome == newOutcome)
            return this;
        return new Known(route, definition, executeAt, deps, newOutcome);
    }

    public Known with(KnownDeps newDeps)
    {
        if (deps == newDeps)
            return this;
        return new Known(route, definition, executeAt, newDeps, outcome);
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

        if (route != Full)
            return SaveStatus.NotDefined;

        if (definition == DefinitionUnknown)
        {
            if (executeAt.isDecidedAndKnownToExecute())
                return SaveStatus.PreCommitted;
            return SaveStatus.NotDefined;
        }

        KnownExecuteAt executeAt = this.executeAt;
        if (!executeAt.isDecidedAndKnownToExecute())
            return SaveStatus.PreAccepted;

        // cannot propagate proposed deps; and cannot propagate known deps without executeAt
        KnownDeps deps = this.deps;
        if (!deps.hasDecidedDeps())
            return SaveStatus.PreCommittedWithDefinition;

        switch (outcome)
        {
            default:
                throw new AssertionError("Unhandled outcome: " + outcome);
            case Unknown:
            case WasApply:
            case Erased:
                return SaveStatus.Stable;

            case Apply:
                return SaveStatus.PreApplied;
        }
    }

    public boolean isDefinitionKnown()
    {
        return definition.isKnown();
    }

    /**
     * The command may have an incomplete route when this is false
     */
    public boolean hasFullRoute()
    {
        return definition.isKnown() || outcome.isOrWasApply();
    }

    public boolean isTruncated()
    {
        switch (outcome)
        {
            default:
                throw new AssertionError("Unhandled outcome: " + outcome);
            case Invalidated:
            case Unknown:
                return false;
            case Apply:
                // since Apply is universal, we can
                return deps == DepsErased;
            case Erased:
            case WasApply:
                return true;
        }
    }

    public boolean canProposeInvalidation()
    {
        return deps.canProposeInvalidation() && executeAt.canProposeInvalidation() && outcome.canProposeInvalidation();
    }

    public Known subtract(Known subtract)
    {
        if (!subtract.isSatisfiedBy(this))
            return Known.Nothing;

        Definition newDefinition = subtract.definition.compareTo(definition) >= 0 ? DefinitionUnknown : definition;
        KnownExecuteAt newExecuteAt = subtract.executeAt.compareTo(executeAt) >= 0 ? ExecuteAtUnknown : executeAt;
        KnownDeps newDeps = subtract.deps.compareTo(deps) >= 0 ? DepsUnknown : deps;
        Outcome newOutcome = outcome.subtract(subtract.outcome);
        return new Known(route, newDefinition, newExecuteAt, newDeps, newOutcome);
    }

    public boolean isDecided()
    {
        return executeAt.isDecided() || outcome.isDecided();
    }

    public boolean isDecidedToExecute()
    {
        return executeAt.isDecidedAndKnownToExecute() || outcome.isOrWasApply();
    }

    public String toString()
    {
        return Stream.of(definition.isKnown() ? "Definition" : null,
                         executeAt.isDecidedAndKnownToExecute() ? "ExecuteAt" : null,
                         deps.hasDecidedDeps() ? "Deps" : null,
                         outcome.isDecided() ? outcome.toString() : null
        ).filter(Objects::nonNull).collect(Collectors.joining(",", "[", "]"));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Known that = (Known) o;
        return route == that.route && definition == that.definition && executeAt == that.executeAt && deps == that.deps && outcome == that.outcome;
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    public boolean hasDefinition()
    {
        return definition.isKnown();
    }

    public boolean hasDecidedDeps()
    {
        return deps.hasDecidedDeps();
    }

    public boolean isInvalidated()
    {
        return outcome.isInvalidated();
    }

    public void checkInvariants()
    {
        if (outcome.isInvalidated()) Invariants.checkState(deps != DepsKnown && executeAt != ExecuteAtKnown);
        else if (outcome.isOrWasApply()) Invariants.checkState(deps != NoDeps && executeAt != NoExecuteAt);
        Invariants.checkState(!isDefinitionKnown() || hasFullRoute());
    }

    public enum KnownRoute
    {
        /**
         * A route may or may not be known, but it may not cover (or even intersect) this shard.
         * The route should be relied upon only if it is a FullRoute.
         */
        Maybe,

        /**
         * A route is known that covers the ranges this shard participates in.
         * Note that if the status is less than Committed, this may not be the final set of owned ranges,
         * and the route may not cover whatever this is decided as.
         *
         * This status primarily exists to communicate semantically to the reader.
         */
        Covering,

        /**
         * The full route is known. <i>Generally</i> this coincides with knowing the Definition.
         */
        Full
        ;

        public KnownRoute reduce(KnownRoute that)
        {
            if (this == that) return this;
            if (this == Full || that == Full) return Full;
            return Maybe;
        }

        public KnownRoute validForAll()
        {
            return this == Covering ? Maybe : this;
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
         * A decision to invalidate the transaction is known to have been reached
         */
        NoExecuteAt
        ;

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
        public boolean isDecidedAndKnownToExecute()
        {
            return this == ExecuteAtKnown;
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
            return max != NoExecuteAt || min != ExecuteAtKnown;
        }
    }

    public enum KnownDeps
    {
        /**
         * No decision is known to have been reached
         */
        DepsUnknown(PreAccept),

        /**
         * A decision to execute the transaction at a given timestamp is known to have been proposed,
         * and the associated dependencies for the shard(s) in question are known for the coordination epoch (txnId.epoch) only.
         *
         * Proposed means Accepted at some replica, but not necessarily a majority and so not committed
         */
        DepsProposed(Accept),

        /**
         * A decision to execute the transaction at a given timestamp with certain dependencies is known to have been proposed,
         * and some associated dependencies for the shard(s) in question have been committed.
         *
         * However, the dependencies are only known to committed at a replica, not necessarily a majority, i.e. not stable
         */
        DepsCommitted(Commit),

        /**
         * A decision to execute or invalidate the transaction is known to have been reached, and any associated
         * dependencies for the shard(s) in question have been cleaned up.
         */
        DepsErased(Cleanup),

        /**
         * A decision to execute the transaction is known to have been reached, and the associated dependencies
         * for the shard(s) in question are known for the coordination and execution epochs, and are stable.
         */
        DepsKnown(Execute),

        /**
         * A decision to invalidate the transaction is known to have been reached
         */
        NoDeps(Persist);

        public final Status.Phase phase;

        KnownDeps(Status.Phase phase)
        {
            this.phase = phase;
        }

        public boolean hasDecidedDeps()
        {
            return this == DepsKnown;
        }

        public boolean canProposeInvalidation()
        {
            return this == DepsUnknown;
        }

        public boolean hasProposedOrDecidedDeps()
        {
            switch (this)
            {
                default: throw new AssertionError("Unhandled KnownDeps: " + this);
                case DepsCommitted:
                case DepsProposed:
                case DepsKnown:
                    return true;
                case NoDeps:
                case DepsErased:
                case DepsUnknown:
                    return false;
            }
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

        public boolean isKnown()
        {
            return this == DefinitionKnown;
        }

        public boolean isOrWasKnown()
        {
            return this == DefinitionKnown;
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
        Invalidated
        ;

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
                case Invalidated:
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
            return this == Invalidated;
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
            return max != Invalidated || (min != Apply && min != WasApply);
        }
    }

    public enum LogicalEpoch
    {
        Coordination, Execution
    }
}
