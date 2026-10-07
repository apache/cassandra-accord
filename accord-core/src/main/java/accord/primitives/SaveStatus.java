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

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import accord.local.CommandSummaries.SummaryStatus;
import accord.primitives.Known.Definition;
import accord.primitives.Known.KnownDeps;
import accord.primitives.Known.KnownExecuteAt;
import accord.primitives.Known.KnownRoute;
import accord.primitives.Known.Outcome;
import accord.primitives.Known.PrivilegedVote;
import accord.primitives.Status.Phase;
import accord.utils.Invariants;

import static accord.local.CommandSummaries.SummaryStatus.ACCEPTED;
import static accord.local.CommandSummaries.SummaryStatus.APPLIED;
import static accord.primitives.Known.Definition.DefinitionErased;
import static accord.primitives.Known.Definition.DefinitionKnown;
import static accord.primitives.Known.Definition.DefinitionUnknown;
import static accord.primitives.Known.KnownDeps.DepsErased;
import static accord.primitives.Known.KnownDeps.DepsFromCoordinator;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsProposed;
import static accord.primitives.Known.KnownDeps.DepsProposedFixed;
import static accord.primitives.Known.KnownDeps.DepsProposedFromCoordinator;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtErased;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtKnown;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtProposed;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtUnknown;
import static accord.primitives.Known.KnownRoute.FullRoute;
import static accord.primitives.Known.KnownRoute.MaybeRoute;
import static accord.primitives.Known.Outcome.Apply;
import static accord.primitives.Known.Outcome.Unknown;
import static accord.primitives.Known.Outcome.WasApply;
import static accord.primitives.Known.PrivilegedVote.NoVote;
import static accord.primitives.Known.PrivilegedVote.VotePreAccept;
import static accord.primitives.SaveStatus.LocalExecution.CleaningUp;
import static accord.primitives.SaveStatus.LocalExecution.NotReady;
import static accord.primitives.SaveStatus.LocalExecution.ReadyToExclude;
import static accord.primitives.SaveStatus.LocalExecution.WaitingToExecute;
import static accord.primitives.Status.Truncated;

/**
 * A version of Status that preserves additional local state, including whether we have previously been PreAccepted
 * and therefore know the definition of the transaction, and what knowledge remains post-truncation.
 *
 * This would potentially complicate users of Status, and the distributed state machine is complicated
 * enough. But it helps to formalise the relationships here as an auxiliary enum.
 * Intended to be used internally by Command implementations.
 */
public enum SaveStatus
{
    Uninitialised                   ( 0, Status.NotDefined),
    NotDefined                      ( 1, Status.NotDefined),
    PreAccepted                     ( 2, Status.PreAccepted),
    // the privileged coordinator's own preaccept
    PreAcceptedPrivCoord            (31, SaveStatus.PreAccepted), // identical to PreAccepted but for coordinator; no need to plumb through self node id to know if we're the coordinator
    PreAcceptedPrivCoordRecovery    (32, SaveStatus.PreAccepted), // identical to PreAccepted but for coordinator when ballot is non-zero; no need to query promised ballot
    PreAcceptedWithVote             ( 3, Status.PreAccepted,                                                                                                    VotePreAccept),
    PreAcceptedWithDeps             ( 4, Status.PreAccepted,            FullRoute,  DefinitionKnown,    ExecuteAtUnknown,   DepsFromCoordinator,    Unknown,    VotePreAccept),
    FastAcceptedPrivCoord           (33, SaveStatus.PreAccepted), // a special status for multi shard transactions using privileged coordinator optimisation

    // note: this status means we have durably cleared any Accepted that may have been inflight, but we must retain DepsFromCoordinator for deciding if a fast path decision may have been taken
    // note: AcceptedInvalidate(s), PreNotAccepted(s) and NotAccepted(s) clear any proposed Deps.
    // This means voters recovering an earlier transaction will not consider the record when excluding the possibility of another transaction's fast-path commit.
    // This is safe, because any Accept that may override the Pre/NotAccepted will construct new Deps that must now witness the recovering transaction.

    AcceptedInvalidate              ( 5, Status.AcceptedInvalidate),
    AcceptedInvalidateWithDefinition( 6, Status.AcceptedInvalidate,     FullRoute,  DefinitionKnown,    ExecuteAtUnknown,   DepsUnknown,        Unknown),

    // TODO (expected): enrich with whether the proposal is a re-proposal of the original coordinator's or a fast path recovery (this may help future optimisations)
    // the txnId==executeAt medium path via TrackStable
    AcceptedMedium                  ( 7, Status.AcceptedMedium),
    AcceptedMediumWithDefinition    ( 8, Status.AcceptedMedium,         FullRoute,  DefinitionKnown,    ExecuteAtProposed,  DepsProposedFixed,  Unknown),
    AcceptedMediumWithDefAndVote    ( 9, Status.AcceptedMedium,         FullRoute,  DefinitionKnown,    ExecuteAtProposed,  DepsProposedFixed,  Unknown,        VotePreAccept),
    AcceptedSlow                    (10, Status.AcceptedSlow),
    AcceptedSlowWithDefinition      (11, Status.AcceptedSlow,           FullRoute,  DefinitionKnown,    ExecuteAtProposed,  DepsProposed,       Unknown),
    // the privileged coordinator's own accept, allowing the coordinator optimisation to apply to the fast quorum medium path
    AcceptedPrivCoord               (34, SaveStatus.AcceptedSlowWithDefinition, DepsProposedFromCoordinator),
    AcceptedPrivCoordRecovery       (35, SaveStatus.AcceptedSlowWithDefinition, DepsProposedFromCoordinator), // identical to AcceptedPrivCoord, but promised a non-zero ballot
    AcceptedSlowWithDefAndVote      (12, Status.AcceptedSlow,           FullRoute,  DefinitionKnown,    ExecuteAtProposed,  DepsProposed,       Unknown,        VotePreAccept),
    PreCommitted                    (13, Status.PreCommitted,                                                                                            ReadyToExclude),
    PreCommittedWithDefinition      (14, Status.PreCommitted,           FullRoute,  DefinitionKnown,    ExecuteAtKnown,     DepsUnknown,        Unknown, ReadyToExclude),
    PreCommittedWithDeps            (15, Status.PreCommitted, ACCEPTED, FullRoute,  DefinitionUnknown,  ExecuteAtKnown,     DepsProposed,       Unknown, ReadyToExclude),
    PreCommittedWithFixedDeps       (16, Status.PreCommitted, ACCEPTED, FullRoute,  DefinitionUnknown,  ExecuteAtKnown,     DepsProposedFixed,  Unknown, ReadyToExclude),
    PreCommittedWithDefAndDeps      (17, Status.PreCommitted, ACCEPTED, FullRoute,  DefinitionKnown,    ExecuteAtKnown,     DepsProposed,       Unknown, ReadyToExclude),
    PreCommittedWithDefAndFixedDeps (18, Status.PreCommitted, ACCEPTED, FullRoute,  DefinitionKnown,    ExecuteAtKnown,     DepsProposedFixed,  Unknown, ReadyToExclude),

    Committed                       (19, Status.Committed,                                                                                               ReadyToExclude),
    Stable                          (20, Status.Stable,                                                                                                  WaitingToExecute),
    ReadyToExecute                  (21, Status.Stable,                                                                                                  LocalExecution.ReadyToExecute),

    PreApplied                      (22, Status.PreApplied,                                                                                              LocalExecution.WaitingToApply),
    Applying                        (23, Status.PreApplied,                                                                                              LocalExecution.Applying),
    // similar to Truncated, but doesn't imply we have any global knowledge about application
    Applied                         (24, Status.Applied,                                                                                                 LocalExecution.Applied),
    // TruncatedApplyWithOutcomeAndDeps exists to support re-populating CommandsForKey on replay with any dependencies needed for computing recovery superseding-rejects decisions
    // TODO (expected): test replay
    TruncatedApplyWithOutcome       (25, Status.Truncated,   APPLIED,   FullRoute,  DefinitionErased,   ApplyAtKnown,       DepsErased,         Apply,   CleaningUp),
    TruncatedApply                  (26, Status.Truncated,   APPLIED,   FullRoute,  DefinitionErased,   ApplyAtKnown,       DepsErased,         WasApply,CleaningUp),
    TruncatedUnapplied              (27, Status.Truncated,              MaybeRoute, DefinitionErased,   ExecuteAtKnown,     DepsErased,         WasApply,CleaningUp),
    // Vestigial means the command cannot be completed and is either pre-bootstrap, did not commit, or did not participate in this shard's epoch
    // TODO (expected): should Vestigial NOT be a Truncated status? We should really only use Known or KnownMap to make decisions, so we don't interpret Truncated as implying a decision, e.g. in Invalidate (as of this commit)
    Vestigial                       (28, Status.Truncated,              MaybeRoute, DefinitionUnknown,  ExecuteAtUnknown,   DepsUnknown,        Unknown, CleaningUp),
    // NOTE: Erased should ONLY be adopted on a replica that knows EVERY shard has successfully applied the transaction at all healthy replicas (or else that it is durably invalidated)
    Erased                          (29, Status.Truncated,              MaybeRoute, DefinitionErased,   ExecuteAtErased,    DepsErased,         Outcome.Erased,CleaningUp),
    Invalidated                     (30, Status.Invalidated,                                                                                             CleaningUp),
    ;

    /**
     * Note that this is a LOCAL concept ONLY, and should not be used to infer anything remotely.
     */
    public enum LocalExecution
    {
        /**
         * Still coordinating a decision
         */
        NotReady,

        /**
         * Ready to exclude based on the decided executeAt, but the dependencies are not known.
         */
        ReadyToExclude,

        /**
         * A complete execution decision has been made, but the dependencies have not executed
         */
        WaitingToExecute,

        /**
         * The command is ready to execute, and a coordinator should promptly compute and distribute the command's outcome
         */
        ReadyToExecute,

        /**
         * The command has been executed and its outcome is known, but we have not locally executed all of its dependencies
         * TODO (expected): we should only await this when we have no local execution dependencies, or we know that
         *   the command is durable remotely
         */
        WaitingToApply,

        /**
         * The command is being asynchronously applied to the local data store
         */
        Applying,

        /**
         * The command has been applied to the local data store
         */
        Applied,

        /**
         * Some or all of the command's local state has been garbage collected
         */
        CleaningUp
    }

    private static final SaveStatus[] byOrdinal = values();
    private static final SaveStatus[] byId;
    static
    {
        byId = new SaveStatus[byOrdinal.length]; // we require 1:1 mapping over same domain
        for (SaveStatus saveStatus : byOrdinal)
        {
            Invariants.require(byId[saveStatus.id] == null, "Duplicate SaveStatus id %d (%s, %s)", saveStatus.id, saveStatus, byId[saveStatus.id]);
            // we expect today that we only report to peers an id that was known prior to decoupling id from ordinal, however this is not a hard requirement (but it could temporarily break serialization if not careful)
            Invariants.expect(saveStatus.report.id <= 30);
            byId[saveStatus.id] = saveStatus;
        }
    }

    /**
     * A stable identifier used for serialization; this is distinct from ordinal(), which defines the logical order
     * of SaveStatus and may change between versions. This must NEVER change for a given SaveStatus.
     */
    public final int id;
    public final Status status;
    public final Phase phase;
    public final SummaryStatus summary;
    public final Known known;
    public final LocalExecution execution;
    private final SaveStatus report; // respond to peer queries with this status - primarily used to make compatibility easier

    SaveStatus(int id, Status status)
    {
        this(id, status, status.phase);
    }

    SaveStatus(int id, SaveStatus status)
    {
        this(id, status, status.known);
    }

    /**
     * Identical to {@code status}, except for the local knowledge of the dependencies
     */
    SaveStatus(int id, SaveStatus status, KnownDeps deps)
    {
        this(id, status, status.known.with(deps));
        Invariants.require(status.known.deps().compareTo(deps) <= 0, "%s should know at least as much as %s", this, status);
    }

    private SaveStatus(int id, SaveStatus status, Known known)
    {
        this.id = id;
        this.status = status.status;
        this.summary = status.summary;
        this.phase = status.phase;
        this.known = known;
        this.execution = status.execution;
        this.report = status;
    }

    SaveStatus(int id, Status status, PrivilegedVote privilegedVote)
    {
        this(id, status, status.summary, status.phase, status.minKnown.with(privilegedVote), NotReady);
    }

    SaveStatus(int id, Status status, LocalExecution execution)
    {
        this(id, status, status.phase, execution);
    }

    SaveStatus(int id, Status status, Phase phase)
    {
        this(id, status, phase, NotReady);
    }

    SaveStatus(int id, Status status, Phase phase, LocalExecution execution)
    {
        this(id, status, phase, status.minKnown, execution);
    }

    SaveStatus(int id, Status status, KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome)
    {
        this(id, status, route, definition, executeAt, deps, outcome, NotReady);
    }

    SaveStatus(int id, Status status, KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome, PrivilegedVote privilegedVote)
    {
        this(id, status, status.summary, status.phase, new Known(route, definition, executeAt, deps, outcome, privilegedVote), NotReady);
    }

    SaveStatus(int id, Status status, KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome, LocalExecution execution)
    {
        this(id, status, status.summary, route, definition, executeAt, deps, outcome, execution);
    }

    SaveStatus(int id, Status status, SummaryStatus summaryStatus, KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome, LocalExecution execution)
    {
        this(id, status, summaryStatus, status.phase, new Known(route, definition, executeAt, deps, outcome, NoVote), execution);
    }

    SaveStatus(int id, Status status, Phase phase, Known known, LocalExecution execution)
    {
        this(id, status, status.summary, phase, known, execution);
    }

    SaveStatus(int id, Status status, SummaryStatus summaryStatus, Phase phase, Known known, LocalExecution execution)
    {
        this.id = id;
        this.status = status;
        this.summary = summaryStatus;
        this.phase = phase;
        this.known = known;
        this.execution = execution;
        this.report = this;
    }

    public boolean is(Status status)
    {
        return this.status == status;
    }

    public boolean hasBeen(Status status)
    {
        return this.status.compareTo(status) >= 0;
    }

    public SaveStatus report()
    {
        return report;
    }

    /**
     * @return true iff we are a privileged coordinator of the transaction on a relevant phase
     */
    public boolean isPrivilegedCoordinator()
    {
        switch (this)
        {
            default: return false;
            case PreAcceptedPrivCoord:
            case PreAcceptedPrivCoordRecovery:
            case FastAcceptedPrivCoord:
            case AcceptedPrivCoord:
            case AcceptedPrivCoordRecovery:
                return true;
        }
    }

    public boolean isPrivilegedCoordinatorZeroBallot()
    {
        return this == PreAcceptedPrivCoord || this == AcceptedPrivCoord;
    }

    public boolean isUninitialised()
    {
        return compareTo(Uninitialised) <= 0;
    }

    public boolean isComplete()
    {
        switch (this)
        {
            case Applied:
            case Invalidated:
                return true;
            default:
                return false;
        }
    }

    // TODO (testing): exhaustive testing, particularly around PreCommitted
    public static SaveStatus get(Status status, Known known)
    {
        if (known.isInvalidated())
            return Invalidated;

        if (status.compareTo(Status.PreCommitted) < 0)
        {
            if (known.isExecuteAtKnown())
            {
                switch (status)
                {
                    case NotDefined: return PreCommitted;
                    case PreAccepted: return PreCommittedWithDefinition;
                    case AcceptedMedium:
                    case AcceptedSlow:
                    case AcceptedInvalidate:
                        known = known.with(status.minKnown.deps());
                        // if the decision is known, we're really PreCommitted, but we will update our dependency information
                }
            }
            else
            {
                switch (status)
                {
                    case NotDefined: return NotDefined;
                    case PreAccepted:
                        return known.is(NoVote) ? PreAccepted : known.is(DepsUnknown) ? PreAcceptedWithVote : PreAcceptedWithDeps;
                    case AcceptedMedium:
                        if (!known.isDefinitionKnown()) return AcceptedMedium;
                        if (!known.hasPrivilegedVote()) return AcceptedMediumWithDefinition;
                        return AcceptedMediumWithDefAndVote;
                    case AcceptedSlow:
                        if (!known.isDefinitionKnown()) return AcceptedSlow;
                        if (!known.hasPrivilegedVote()) return AcceptedSlowWithDefinition;
                        return AcceptedSlowWithDefAndVote;
                    case AcceptedInvalidate:
                        return known.isDefinitionKnown() ? AcceptedInvalidateWithDefinition : AcceptedInvalidate;
                        // if the decision is known, we're really PreCommitted
                }
            }
        }

        switch (status)
        {
            default: throw new AssertionError("Unexpected status: " + status);
            case AcceptedMedium:
            case AcceptedSlow:
            case AcceptedInvalidate:
            case PreCommitted:
                if (!known.isDefinitionKnown())
                {
                    switch (known.deps())
                    {
                        case DepsProposedFixed: return PreCommittedWithFixedDeps;
                        case DepsProposedFromCoordinator: // we do not retain the privileged coordinator's distinction once PreCommitted
                        case DepsProposed:      return PreCommittedWithDeps;
                        default:                return PreCommitted;
                    }
                }
                switch (known.deps())
                {
                    default:                    throw new AssertionError("Unexpected DepsKnown: " + known.deps());
                    case DepsErased:
                    case DepsUnknown:
                    case DepsFromCoordinator:   return PreCommittedWithDefinition; // TODO (required): consider if this is correct
                    case DepsProposedFixed:     return PreCommittedWithDefAndFixedDeps;
                    case DepsProposedFromCoordinator: // we do not retain the privileged coordinator's distinction once PreCommitted
                    case DepsProposed:          return PreCommittedWithDefAndDeps;
                    case DepsKnown:
                    case DepsCommitted:
                }
            case Committed: return known.deps() == DepsKnown ? Stable : Committed;
            case Stable: return Stable;
            case PreApplied: return PreApplied;
            case Applied: return Applied;
            case Invalidated: return Invalidated;
        }
    }

    private static final Known DefinitionOnly = new Known(FullRoute, DefinitionKnown, ExecuteAtUnknown, DepsUnknown, Unknown, NoVote);
    public static SaveStatus withDefinition(SaveStatus status)
    {
        return enrich(status, DefinitionOnly);
    }

    public static SaveStatus enrich(SaveStatus status, Known known)
    {
        // most statuses already know everything they can
        switch (status.status)
        {
            case NotDefined:
            case PreAccepted:
            case AcceptedInvalidate:
            case AcceptedMedium:
            case AcceptedSlow:
            case PreCommitted:
            case Committed:
            {
                if (known.isSatisfiedBy(status.known))
                    return status;

                Invariants.require(!status.isPrivilegedCoordinator(), "%s (a privileged coordinator state) should not be 'enrichable' (by %s)", status, known);
                return get(status.status, status.known.atLeast(known));
            }

            case Stable:
                return status;

            case Truncated:
                switch (status)
                {
                    default: throw new AssertionError("Unexpected status: " + status);
                    case Erased:
                    case Vestigial:
                        if (known.outcome().isInvalidated())
                            return Invalidated;

                        if (!known.outcome().isOrWasApply() || !known.is(ExecuteAtKnown))
                            return status;

                    case TruncatedUnapplied:
                        if (!known.is(ApplyAtKnown))
                            return TruncatedUnapplied;

                    case TruncatedApply:
                        if (!known.is(Outcome.Apply))
                            return TruncatedApply;

                    case TruncatedApplyWithOutcome:
                        if (!known.is(DepsKnown))
                            return TruncatedApplyWithOutcome;

                        return Applied;
                }
        }

        return status;
    }

    // TODO (expected): tighten up distinction of "preferKnowledge" and its interaction with CheckStatus
    public static SaveStatus merge(SaveStatus a, Ballot ballotA, SaveStatus b, Ballot ballotB, boolean preferKnowledge)
    {
        // we first enrich cleanups with the knowledge of the other, to avoid counter-intuitive situations where
        // we might be able to convert a TruncatedWithApply into Applied, but instead select something much earlier
        // such as ReadyToExecute; we then apply the normal max process to the results
        if (a.phase == Phase.Cleanup) a = enrich(a, b.known);
        if (b.phase == Phase.Cleanup) b = enrich(b, a.known);
        SaveStatus result = max(a, a, ballotA, b, b, ballotB, preferKnowledge);
        return enrich(result, (result == a ? b : a).known);
    }

    public static <T> T max(T av, SaveStatus a, Ballot ballotA, T bv, SaveStatus b, Ballot ballotB, boolean preferKnowledge)
    {
        if (a == b)
            return av;

        if (a.phase != b.phase)
        {
            return a.phase.compareTo(b.phase) > 0
                   ? (preferKnowledge && a.phase == Phase.Cleanup ? bv : av)
                   : (preferKnowledge && b.phase == Phase.Cleanup ? av : bv);
        }

        if (a.phase.tieBreakWithBallot)
            return ballotA.compareTo(ballotB) >= 0 ? av : bv;

        if (preferKnowledge && a.lowerHasMoreKnowledge(b))
            return a.compareTo(b) <= 0 ? av : bv;
        return a.compareTo(b) >= 0 ? av : bv;
    }

    public static <T> T max(T av, T bv, Function<T, SaveStatus> getStatus, Function<T, Ballot> getAcceptedOrCommittedBallot)
    {
        SaveStatus a = getStatus.apply(av), b = getStatus.apply(bv);

        if (a == b)
            return av;

        if (a.phase != b.phase)
            return a.phase.compareTo(b.phase) > 0 ? av : bv;

        if (a.phase.tieBreakWithBallot)
            return getAcceptedOrCommittedBallot.apply(av).compareTo(getAcceptedOrCommittedBallot.apply(bv)) >= 0 ? av : bv;

        return a.compareTo(b) >= 0 ? av : bv;
    }

    public static <T> T maxOfList(List<T> list, Function<T, SaveStatus> getStatus, Function<T, Ballot> getAcceptedOrCommittedBallot, Predicate<T> filter)
    {
        T max = null;
        SaveStatus maxStatus = null;
        Ballot maxBallot = null;
        for (T item : list)
        {
            if (!filter.test(item))
                continue;

            SaveStatus status = getStatus.apply(item);
            Ballot ballot = getAcceptedOrCommittedBallot.apply(item);
            if (max != null)
            {
                if (maxStatus.phase.compareTo(status.phase) > 0) continue;
                if (maxStatus.phase == status.phase)
                {
                    if (maxStatus.phase.tieBreakWithBallot)
                    {
                        int c = maxBallot.compareTo(ballot);
                        if (c > 0) continue;
                        if (c == 0 && maxStatus.compareTo(status) >= 0) continue;
                    }
                    else if (maxStatus.compareTo(status) >= 0) continue;
                }
            }

            max = item;
            maxStatus = status;
            maxBallot = ballot;
        }

        return max;
    }

    // TODO (desired): this isn't a simple linear relationship - Committed has some more knowledge, but some less; PreAccepted has much less
    //   also, Erased has more knowledge than Vestigial, but less knowledge then TruncatedX
    //  Should probably move away from propagating SaveStatus altogether
    public boolean lowerHasMoreKnowledge(SaveStatus than)
    {
        if (this.is(Truncated) && !than.is(Status.NotDefined))
            return true;

        if (than.is(Truncated) && !this.is(Status.NotDefined))
            return true;

        return false;
    }

    /**
     * Lookup by {@link #ordinal()}. This is NOT stable across versions, use {@link #forId} for serialization.
     */
    public static SaveStatus forOrdinal(int ordinal)
    {
        if (ordinal < 0 || ordinal >= byOrdinal.length)
            throw new IndexOutOfBoundsException(ordinal);
        return byOrdinal[ordinal];
    }

    /**
     * Lookup by the stable serialization {@link #id}.
     */
    public static SaveStatus forId(int id)
    {
        if (id < 0 || id >= byId.length)
            throw new IndexOutOfBoundsException(id);
        return byId[id];
    }
}
