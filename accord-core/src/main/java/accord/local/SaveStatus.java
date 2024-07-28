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

import accord.local.Status.*;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.local.SaveStatus.LocalExecution.CleaningUp;
import static accord.local.SaveStatus.LocalExecution.NotReady;
import static accord.local.Status.Definition.DefinitionErased;
import static accord.local.Status.Definition.DefinitionKnown;
import static accord.local.Status.Definition.DefinitionUnknown;
import static accord.local.Status.Known.Apply;
import static accord.local.Status.Known.Decision;
import static accord.local.Status.Known.ExecuteAtOnly;
import static accord.local.Status.Known.Nothing;
import static accord.local.Status.KnownDeps.*;
import static accord.local.Status.KnownExecuteAt.*;
import static accord.local.Status.KnownRoute.Covering;
import static accord.local.Status.KnownRoute.Full;
import static accord.local.Status.KnownRoute.Maybe;
import static accord.local.Status.Outcome.Unknown;
import static accord.local.Status.Truncated;

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
    // TODO (expected): erase Uninitialised in Context once command finishes
    // TODO (expected): we can use Uninitialised in several places to simplify/better guarantee correct behaviour with truncation
    Uninitialised                   (Status.NotDefined),
    // TODO (expected): reify PreAcceptedNotDefined and NotDefinedWithSomeRoute (latter to semantically represent outcome of InformHome)
    NotDefined                      (Status.NotDefined),
    PreAccepted                     (Status.PreAccepted),
    // note: AcceptedInvalidate and AcceptedInvalidateWithDefinition clear any proposed Deps.
    // This means voters recovering an earlier transaction will not consider the record when excluding the possibility of a fast-path commit.
    // This is safe, because any Accept that may override the AcceptedInvalidate will construct new Deps that must now witness the recovering transaction.
    AcceptedInvalidate              (Status.AcceptedInvalidate),
    AcceptedInvalidateWithDefinition(Status.AcceptedInvalidate,    Full,     DefinitionKnown,   ExecuteAtUnknown,  DepsUnknown,  Unknown),
    Accepted                        (Status.Accepted),
    AcceptedWithDefinition          (Status.Accepted,              Full,     DefinitionKnown,   ExecuteAtProposed, DepsProposed, Unknown),
    PreCommitted                    (Status.PreCommitted,                                                                                          LocalExecution.ReadyToExclude),
    PreCommittedWithAcceptedDeps    (Status.PreCommitted,          Covering, DefinitionUnknown, ExecuteAtKnown,    DepsProposed, Unknown,          LocalExecution.ReadyToExclude),
    PreCommittedWithDefinition      (Status.PreCommitted,          Full,     DefinitionKnown,   ExecuteAtKnown,    DepsUnknown,  Unknown,          LocalExecution.ReadyToExclude),
    PreCommittedWithDefinitionAndAcceptedDeps(Status.PreCommitted, Full,     DefinitionKnown,   ExecuteAtKnown,    DepsProposed, Unknown,          LocalExecution.ReadyToExclude),
    Committed                       (Status.Committed,                                                                                             LocalExecution.ReadyToExclude),
    Stable                          (Status.Stable,                                                                                                LocalExecution.WaitingToExecute),
    ReadyToExecute                  (Status.Stable,                                                                                                LocalExecution.ReadyToExecute),
    PreApplied                      (Status.PreApplied,                                                                                            LocalExecution.WaitingToApply),
    Applying                        (Status.PreApplied,                                                                                            LocalExecution.Applying),
    // similar to Truncated, but doesn't imply we have any global knowledge about application
    Applied                         (Status.Applied,                                                                                               LocalExecution.Applied),
    // TruncatedApplyWithDeps is a state never adopted within a single replica; it is however a useful state we may enter by combining state from multiple replicas
    // TODO (expected): TruncatedApplyWithDeps should be redundant now we have migrated away from SaveStatus in CheckStatusOk to Known; remove in isolated commit once stable
    TruncatedApplyWithDeps          (Status.Truncated,             Full,    DefinitionErased,   ExecuteAtKnown,    DepsKnown,    Outcome.Apply,    CleaningUp),
    TruncatedApplyWithOutcome       (Status.Truncated,             Full,    DefinitionErased,   ExecuteAtKnown,    DepsErased,   Outcome.Apply,    CleaningUp),
    TruncatedApply                  (Status.Truncated,             Full,    DefinitionErased,   ExecuteAtKnown,    DepsErased,   Outcome.WasApply, CleaningUp),
    // NOTE: Erased should ONLY be adopted on a replica that knows EVERY shard has successfully applied the transaction at all healthy replicas (or else it is durably invalidated)
    Erased                          (Status.Truncated,             Maybe,   DefinitionErased,   ExecuteAtErased,   DepsErased,   Outcome.Erased,   CleaningUp),
    // ErasedOrInvalidOrVestigial means the command cannot be completed and is either pre-bootstrap, did not commit, or did not participate in this shard's epoch
    ErasedOrInvalidOrVestigial      (Status.Truncated,             Maybe,   DefinitionUnknown,  ExecuteAtUnknown,  DepsUnknown,  Unknown,          CleaningUp),
    Invalidated                     (Status.Invalidated,                                                                                           CleaningUp),
    ;

    /**
     * Note that this is a LOCAL concept ONLY, and should not be used to infer anything remotely.
     */
    public enum LocalExecution
    {
        NotReady(Nothing),
        ReadyToExclude(ExecuteAtOnly),
        WaitingToExecute(Decision),
        ReadyToExecute(Decision),
        // TODO (expected): we seem to be able to await this when we know there are still local execution dependencies
        //   we should only request this when we know the transaction can execute locally
        WaitingToApply(Apply),
        Applying(Apply),
        Applied(Apply),
        CleaningUp(Nothing);

        public final Known requires;

        LocalExecution(Known requires)
        {
            this.requires = requires;
        }

        public boolean isSatisfiedBy(Known known)
        {
            return requires.isSatisfiedBy(known);
        }

        public long fetchEpoch(TxnId txnId, Timestamp timestamp)
        {
            if (timestamp == null || timestamp.equals(Timestamp.NONE))
                return txnId.epoch();

            switch (this)
            {
                default: throw new AssertionError("Unexpected status: " + this);
                case NotReady:
                case ReadyToExclude:
                case ReadyToExecute:
                case WaitingToExecute:
                    return txnId.epoch();

                case WaitingToApply:
                case Applying:
                case Applied:
                case CleaningUp:
                    return timestamp.epoch();
            }
        }
    }

    public final Status status;
    public final Phase phase;
    public final Known known;
    public final LocalExecution execution;

    SaveStatus(Status status)
    {
        this(status, status.phase);
    }

    SaveStatus(Status status, LocalExecution execution)
    {
        this(status, status.phase, execution);
    }

    SaveStatus(Status status, Phase phase)
    {
        this(status, phase, NotReady);
    }

    SaveStatus(Status status, Phase phase, LocalExecution execution)
    {
        this(status, phase, status.minKnown, execution);
    }

    SaveStatus(Status status, KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome)
    {
        this(status, route, definition, executeAt, deps, outcome, NotReady);
    }

    SaveStatus(Status status, KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome, LocalExecution execution)
    {
        this(status, status.phase, new Known(route, definition, executeAt, deps, outcome), execution);
    }

    SaveStatus(Status status, Phase phase, Known known, LocalExecution execution)
    {
        this.status = status;
        this.phase = phase;
        this.known = known;
        this.execution = execution;
    }

    public boolean is(Status status)
    {
        return this.status.equals(status);
    }

    public boolean hasBeen(Status status)
    {
        return this.status.compareTo(status) >= 0;
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

    // TODO (expected): merge Known only, and ensure 1:1 mapping so can reconstruct composite
    // TODO (expected, testing): exhaustive testing, particularly around PreCommitted
    public static SaveStatus get(Status status, Known known)
    {
        if (known.isInvalidated())
            return Invalidated;

        switch (status)
        {
            default: throw new AssertionError("Unexpected status: " + status);
            case NotDefined: return known.executeAt.isDecidedAndKnownToExecute() ? PreCommitted : NotDefined;
            case PreAccepted: return known.executeAt.isDecidedAndKnownToExecute() ? PreCommittedWithDefinition : PreAccepted;
            case AcceptedInvalidate:
                // AcceptedInvalidate logically clears any proposed deps and executeAt
                if (!known.executeAt.isDecidedAndKnownToExecute())
                    return known.isDefinitionKnown() ? AcceptedInvalidateWithDefinition : AcceptedInvalidate;
                // If we know the executeAt decision then we do not clear it, and fall-through to PreCommitted
                // however, we still clear the deps, as any deps we might have previously seen proposed are now expired
                // TODO (expected, consider): consider clearing Command.partialDeps in this case also
                known = known.with(DepsUnknown);
            case Accepted:
                if (!known.executeAt.isDecidedAndKnownToExecute())
                    return known.isDefinitionKnown() ? AcceptedWithDefinition : Accepted;
                // if the decision is known, we're really PreCommitted
            case PreCommitted:
                if (!known.isDefinitionKnown() || !known.deps.hasCommittedOrDecidedDeps())
                {
                    if (known.isDefinitionKnown())
                        return known.deps == DepsProposed ? PreCommittedWithDefinitionAndAcceptedDeps : PreCommittedWithDefinition;
                    return known.deps == DepsProposed ? PreCommittedWithAcceptedDeps : PreCommitted;
                }
            case Committed: return known.deps == DepsKnown ? Stable : Committed;
            case Stable: return Stable;
            case PreApplied: return PreApplied;
            case Applied: return Applied;
            case Invalidated: return Invalidated;
        }
    }

    public static SaveStatus enrich(SaveStatus status, Known known)
    {
        switch (status.status)
        {
            // most statuses already know everything they can
            case NotDefined:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
            case Committed:
                if (known.isSatisfiedBy(status.known))
                    return status;
                return get(status.status, status.known.atLeast(known));
            case Stable:
                return status;

            case Truncated:
                switch (status)
                {
                    default: throw new AssertionError("Unexpected status: " + status);
                    case ErasedOrInvalidOrVestigial:
                        if (known.outcome.isInvalidated())
                            return Invalidated;

                        if (!known.outcome.isOrWasApply() || known.executeAt == ExecuteAtKnown)
                            return ErasedOrInvalidOrVestigial;

                    case Erased:
                        if (!known.outcome.isOrWasApply() || known.executeAt != ExecuteAtKnown)
                            return Erased;

                    case TruncatedApply:
                        if (known.outcome != Outcome.Apply)
                            return TruncatedApply;

                    case TruncatedApplyWithOutcome:
                        if (known.deps != DepsKnown)
                            return TruncatedApplyWithOutcome;

                    case TruncatedApplyWithDeps:
                        if (!known.isDefinitionKnown())
                            return TruncatedApplyWithDeps;

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

    // TODO (desired): this isn't a simple linear relationship - Committed has some more knowledge, but some less; PreAccepted has much less
    public boolean lowerHasMoreKnowledge(SaveStatus than)
    {
        if (this.is(Truncated) && !than.is(Status.NotDefined))
            return true;

        if (than.is(Truncated) && !this.is(Status.NotDefined))
            return true;

        return false;
    }
}
