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

import accord.messages.BeginRecovery;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import static accord.local.Status.Definition.*;
import static accord.local.Status.Known.*;
import static accord.local.Status.KnownDeps.*;
import static accord.local.Status.KnownExecuteAt.*;
import static accord.local.Status.Outcome.*;
import static accord.local.Status.Phase.*;

public enum Status
{
    NotDefined        (None,      Nothing),
    PreAccepted       (PreAccept, DefinitionOnly),
    AcceptedInvalidate(Accept,    DefinitionUnknown, ExecuteAtUnknown, DepsUnknown, Unknown), // may or may not have witnessed
    Accepted          (Accept,    DefinitionUnknown, ExecuteAtProposed, DepsProposed, Unknown), // may or may not have witnessed

    /**
     * PreCommitted is a peculiar state, half-way between Accepted and Committed.
     * We know the transaction is Committed and its execution timestamp, but we do
     * not know its dependencies, and we may still have state leftover from the Accept round
     * that is necessary for recovery.
     *
     * So, for execution of other transactions we may treat a PreCommitted transaction as Committed,
     * using the timestamp to update our dependency set to rule it out as a dependency.
     * But we do not have enough information to execute the transaction, and when recovery calculates
     * {@link BeginRecovery#acceptedStartedBeforeWithoutWitnessing}, {@link BeginRecovery#hasCommittedExecutesAfterWithoutWitnessing}
     *
     * and {@link BeginRecovery#committedStartedBeforeAndWitnessed} we may not have the dependencies
     * to calculate the result. For these operations we treat ourselves as whatever Accepted status
     * we may have previously taken, using any proposed dependencies to compute the result.
     *
     * This state exists primarily to permit us to efficiently separate work between different home shards.
     * Take a transaction A that reaches the Committed status and commits to all of its home shard A*'s replicas,
     * but fails to commit to all shards. A takes an execution time later than its TxnId, and in the process
     * adopts a dependency on a transaction B that is coordinated by its home shard B*, that has itself taken
     * a dependency upon A. Importantly, B commits a lower executeAt than A and so will execute first, and once A*
     * commits B, A will remove it from its dependencies. However, there is insufficient information on A*
     * to commit B since it does not know A*'s dependencies, and B* will not process B until A* executes A.
     * To solve this problem we simply permit the executeAt we discover for B to be propagated to A* without
     * its dependencies. Though this does complicate the state machine a little.
     */
    PreCommitted      (Accept,  DefinitionUnknown, ExecuteAtKnown,   DepsUnknown, Unknown),

    Committed         (Commit,  DefinitionKnown,   ExecuteAtKnown,   DepsKnown,   Unknown),
    // TODO (expected): do we need ReadyToExecute here, or can we keep it to SaveStatus only?
    ReadyToExecute    (Commit,  DefinitionKnown,   ExecuteAtKnown,   DepsKnown,   Unknown),
    // TODO (expected): do we need both PreApplied and Applied here, or can we keep them to SaveStatus only?
    PreApplied        (Persist, DefinitionKnown,   ExecuteAtKnown,   DepsKnown,   Outcome.Apply),
    Applied           (Persist, DefinitionKnown,   ExecuteAtKnown,   DepsKnown,   Outcome.Apply),
    Truncated         (Cleanup, DefinitionUnknown, ExecuteAtUnknown, DepsUnknown, Unknown),
    Invalidated       (Persist, NoOp,              NoExecuteAt,      NoDeps,      Outcome.Invalidated),
    ;

    /**
     * Represents the phase of a transaction from the perspective of coordination
     * None:       the transaction is not currently being processed by us (it may be known to us, but only transitively)
     * PreAccept:  the transaction is being disseminated and is seeking an execution order
     * Accept:     the transaction did not achieve 1RT consensus and is making durable its execution order
     * Commit:     the transaction's execution order has been durably decided, and is being disseminated
     * Persist:    the transaction has executed, and its outcome is being persisted
     * Cleanup:    the transaction has completed, and state used for processing it is being reclaimed
     */
    public enum Phase
    {
        None, PreAccept, Accept, Commit, Persist, Cleanup
    }

    /**
     * A vector of various facets of knowledge about, or required for, processing a transaction.
     * Each property is treated independently, there is no precedence relationship between them.
     * Each property's values are however ordered with respect to each other.
     */
    public static class Known
    {
        public static final Known Nothing           = new Known(DefinitionUnknown, ExecuteAtUnknown, DepsUnknown, Unknown);
        public static final Known DefinitionOnly    = new Known(DefinitionKnown,   ExecuteAtUnknown, DepsUnknown, Unknown);
        public static final Known ExecuteAtOnly     = new Known(DefinitionUnknown, ExecuteAtKnown,   DepsUnknown, Unknown);
        public static final Known Decision          = new Known(DefinitionKnown,   ExecuteAtKnown,   DepsKnown,   Unknown);
        public static final Known Apply             = new Known(DefinitionUnknown, ExecuteAtKnown,   DepsKnown,   Outcome.Apply);
        public static final Known Invalidated       = new Known(DefinitionUnknown, ExecuteAtUnknown, DepsUnknown, Outcome.Invalidated);

        public final Definition definition;
        public final KnownExecuteAt executeAt;
        public final KnownDeps deps;
        public final Outcome outcome;

        public Known(Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome)
        {
            this.definition = definition;
            this.executeAt = executeAt;
            this.deps = deps;
            this.outcome = outcome;
        }

        public Known merge(Known with)
        {
            Definition maxDefinition = definition.compareTo(with.definition) >= 0 ? definition : with.definition;
            KnownExecuteAt maxExecuteAt = executeAt.compareTo(with.executeAt) >= 0 ? executeAt : with.executeAt;
            KnownDeps maxDeps = deps.compareTo(with.deps) >= 0 ? deps : with.deps;
            Outcome maxOutcome = outcome.compareTo(with.outcome) >= 0 ? outcome : with.outcome;
            if (maxDefinition == definition && maxExecuteAt == executeAt && maxDeps == deps &&  maxOutcome == outcome)
                return this;
            if (maxDefinition == with.definition && maxExecuteAt == with.executeAt && maxDeps == with.deps && maxOutcome == with.outcome)
                return with;
            return new Known(maxDefinition, maxExecuteAt, maxDeps, maxOutcome);
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
            return new Known(definition, executeAt, deps, newOutcome);
        }

        public Known with(KnownDeps newDeps)
        {
            if (deps == newDeps)
                return this;
            return new Known(definition, executeAt, newDeps, outcome);
        }

        public Status propagate()
        {
            switch (outcome)
            {
                default: throw new AssertionError();
                case Erased:
                    return Status.Truncated;

                case Invalidated:
                    return Status.Invalidated;

                case Apply:
                case WasApply:
                    if (executeAt.hasDecidedExecuteAt() && definition.isKnown() && deps.hasDecidedDeps())
                        return PreApplied;

                case Unknown:
                    if (executeAt.hasDecidedExecuteAt() && definition.isKnown() && deps.hasDecidedDeps())
                        return Committed;

                    if (executeAt.hasDecidedExecuteAt())
                        return PreCommitted;

                    if (definition.isKnown())
                        return PreAccepted;

                    if (deps.hasDecidedDeps())
                        throw new IllegalStateException();
            }

            return NotDefined;
        }

        public boolean isDefinitionKnown()
        {
            return definition.isKnown();
        }

        /**
         * The command may have an incomplete route when this is false
         */
        public boolean hasCompleteRoute()
        {
            return definition.isKnown() || outcome.isOrWasApply();
        }

        public boolean canProposeInvalidation()
        {
            return deps.canProposeInvalidation() && executeAt.canProposeInvalidation() && outcome.canProposeInvalidation();
        }

        public String toString()
        {
            return Stream.of(definition.isKnown() ? "Definition" : null,
                             executeAt.hasDecidedExecuteAt() ? "ExecuteAt" : null,
                             deps.hasDecidedDeps() ? "Deps" : null,
                             outcome.isOrWasApply() ? "Outcome" : null
            ).filter(Objects::nonNull).collect(Collectors.joining(",", "[", "]"));
        }
    }

    public enum KnownExecuteAt
    {
        /**
         * No decision is known to have been reached
         */
        ExecuteAtUnknown,

        /**
         * A decision to execute the transaction is known to have been proposed, and the associated executeAt timestamp
         */
        ExecuteAtProposed,

        /**
         * A decision to execute the transaction is known to have been reached, and the associated executeAt timestamp
         */
        ExecuteAtKnown,

        /**
         * A decision to invalidate the transaction is known to have been reached
         */
        NoExecuteAt
        ;

        public boolean hasDecidedExecuteAt()
        {
            return compareTo(ExecuteAtKnown) >= 0;
        }

        public boolean hasDecidedNonZeroExecuteAt()
        {
            return this == ExecuteAtKnown;
        }

        public final Timestamp executeAtIfKnownElseTxnId(TxnId txnId, Timestamp executeAt)
        {
            return this == ExecuteAtKnown ? executeAt : txnId;
        }

        public boolean canProposeInvalidation()
        {
            return this == ExecuteAtUnknown;
        }
    }

    public enum KnownDeps
    {
        /**
         * No decision is known to have been reached
         */
        DepsUnknown,

        /**
         * A decision to execute the transaction is known to have been proposed, and the associated dependencies
         * for the shard(s) in question are known for the coordination epoch (txnId.epoch) only.
         */
        DepsProposed,

        /**
         * A decision to execute the transaction is known to have been reached, and the associated dependencies
         * for the shard(s) in question are known for the coordination and execution epochs.
         */
        DepsKnown,

        /**
         * A decision to invalidate the transaction is known to have been reached
         */
        NoDeps
        ;

        public boolean hasDecidedDeps()
        {
            return this == DepsKnown;
        }

        public boolean isDecided()
        {
            return compareTo(DepsKnown) >= 0;
        }

        public boolean canProposeInvalidation()
        {
            return this == DepsUnknown;
        }

        public boolean hasProposedOrDecidedDeps()
        {
            return this == DepsProposed || this == DepsKnown;
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
         * The definition is known
         *
         * TODO (expected, clarity): distinguish between known for coordination epoch and known for commit/execute
         */
        DefinitionKnown,

        /**
         * The definition is irrelevant, as the transaction has been invalidated and may be treated as a no-op
         */
        NoOp;

        public boolean canProposeInvalidation()
        {
            return this == DefinitionUnknown;
        }

        public boolean isKnown()
        {
            return this == DefinitionKnown;
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
         * The outcome is known
         */
        Apply,

        /**
         * The transaction has been cleaned-up, but was applied and the relevant portion of its outcome has been cleaned up
         * TODO (expected): is this state helpful? Feels like we can do without it
         */
        WasApply,

        /**
         * The transaction is known to have been invalidated
         */
        Invalidated,

        /**
         * The transaction has been *completely cleaned up* - this means it has been made
         * durable at every live replica of every shard we contacted
         */
        Erased
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

        public boolean propagatesBetweenShards()
        {
            return this == Invalidated;
        }

        public boolean isTruncated()
        {
            return this == Erased || this == WasApply;
        }
    }

    public enum LogicalEpoch
    {
        Coordination, Execution
    }

    /**
     * Represents the durability of a transaction's Persist phase.
     * NotDurable: the outcome has not been durably recorded
     * Local:      the outcome has been durably recorded at least locally
     * Majority:   the outcome has been durably recorded to a majority of each participating shard
     * Universal:  the outcome has been durably recorded to every participating replica
     * DurableOrInvalidated:  the outcome was either invalidated, or has been durably recorded to every replica
     */
    public enum Durability
    {
        NotDurable, Local, Majority, Universal, DurableOrInvalidated;

        public boolean isDurable()
        {
            return compareTo(Majority) >= 0 && compareTo(DurableOrInvalidated) < 0;
        }

        public boolean isDurableOrInvalidated()
        {
            return compareTo(Majority) >= 0;
        }
    }

    public final Phase phase;
    public final Known minKnown;

    Status(Phase phase, Known minKnown)
    {
        this.phase = phase;
        this.minKnown = minKnown;
    }

    Status(Phase phase, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Status.Outcome outcome)
    {
        this.phase = phase;
        this.minKnown = new Known(definition, executeAt, deps, outcome);
    }

    // TODO (desired, clarity): investigate all uses of hasBeen, and migrate as many as possible to testing
    //                          Phase, ReplicationPhase and ExecutionStatus where these concepts are inadequate,
    //                          see if additional concepts can be introduced
    public boolean hasBeen(Status equalOrGreaterThan)
    {
        return compareTo(equalOrGreaterThan) >= 0;
    }

    public static <T> T max(List<T> list, Function<T, Status> getStatus, Function<T, Ballot> getAccepted, Predicate<T> filter)
    {
        T max = null;
        Status maxStatus = null;
        Ballot maxAccepted = null;
        for (T item : list)
        {
            if (!filter.test(item))
                continue;

            Status status = getStatus.apply(item);
            Ballot accepted = getAccepted.apply(item);
            boolean update = max == null
                          || maxStatus.phase.compareTo(status.phase) < 0
                          || (status.phase.equals(Phase.Accept) && maxAccepted.compareTo(accepted) < 0);

            if (!update)
                continue;

            max = item;
            maxStatus = status;
            maxAccepted = accepted;
        }

        return max;
    }

    public static <T> T max(T a, Status statusA, Ballot acceptedA, T b, Status statusB, Ballot acceptedB)
    {
        int c = statusA.phase.compareTo(statusB.phase);
        if (c > 0) return a;
        if (c < 0) return b;
        if (statusA.phase != Phase.Accept || acceptedA.compareTo(acceptedB) >= 0)
            return a;
        return b;
    }

    public static Status simpleMax(Status a, Status b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }
}
