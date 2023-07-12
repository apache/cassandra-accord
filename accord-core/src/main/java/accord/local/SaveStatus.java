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

import static accord.local.SaveStatus.LocalExecution.NotReady;
import static accord.local.Status.Definition.DefinitionKnown;
import static accord.local.Status.Definition.DefinitionUnknown;
import static accord.local.Status.Known.Apply;
import static accord.local.Status.Known.Decision;
import static accord.local.Status.Known.ExecuteAtOnly;
import static accord.local.Status.Known.Nothing;
import static accord.local.Status.KnownDeps.*;
import static accord.local.Status.KnownExecuteAt.*;
import static accord.local.Status.NotDefined;
import static accord.local.Status.Outcome.Unknown;
import static accord.local.Status.Truncated;

/**
 * Identical to Status but preserves whether we have previously been PreAccepted and therefore know the definition
 * of the transaction. This would potentially complicate users of Status, and the distributed state machine is complicated
 * enough. But it helps to formalise the relationships here as an auxiliary enum.
 * Intended to be used internally by Command implementations.
 */
public enum SaveStatus
{
    // TODO (expected): erase Uninitialised in Context once command finishes
    // TODO (expected): we can use Uninitialised in several places to simplify/better guarantee correct behaviour with truncation
    Uninitialised                   (Status.NotDefined),
    NotDefined                      (Status.NotDefined),
    PreAccepted                     (Status.PreAccepted),
    AcceptedInvalidate              (Status.AcceptedInvalidate),
    AcceptedInvalidateWithDefinition(Status.AcceptedInvalidate,    DefinitionKnown,   ExecuteAtUnknown,  DepsUnknown,  Unknown),
    Accepted                        (Status.Accepted),
    AcceptedWithDefinition          (Status.Accepted,              DefinitionKnown,   ExecuteAtProposed, DepsProposed, Unknown),
    PreCommitted                    (Status.PreCommitted),
    PreCommittedWithAcceptedDeps    (Status.PreCommitted,          DefinitionUnknown, ExecuteAtKnown,    DepsProposed, Unknown),
    PreCommittedWithDefinition      (Status.PreCommitted,          DefinitionKnown,   ExecuteAtKnown,    DepsUnknown,  Unknown),
    PreCommittedWithDefinitionAndAcceptedDeps(Status.PreCommitted, DefinitionKnown,   ExecuteAtKnown,    DepsProposed, Unknown),
    Committed                       (Status.Committed),
    ReadyToExecute                  (Status.ReadyToExecute,                                                                                    LocalExecution.ReadyToExecute),
    PreApplied                      (Status.PreApplied),
    Applying                        (Status.PreApplied),
    Applied                         (Status.Applied,                                                                                           LocalExecution.Applied),
    TruncatedApplyWithDeps          (Status.Truncated,             DefinitionUnknown, ExecuteAtKnown,    DepsKnown,    Outcome.Apply,          LocalExecution.CleaningUp),
    TruncatedApplyWithOutcome       (Status.Truncated,             DefinitionUnknown, ExecuteAtKnown,    DepsUnknown,  Outcome.Apply,          LocalExecution.CleaningUp),
    TruncatedApply                  (Status.Truncated,             DefinitionUnknown, ExecuteAtKnown,    DepsUnknown,  Outcome.WasApply,       LocalExecution.CleaningUp),
    Erased                          (Status.Truncated,             DefinitionUnknown, ExecuteAtUnknown,  DepsUnknown,  Outcome.Erased,         LocalExecution.CleaningUp),
    Invalidated                     (Status.Invalidated,                                                                                       LocalExecution.CleaningUp),
    ;

    public enum LocalExecution
    {
        NotReady(Nothing),
        ReadyToExclude(ExecuteAtOnly),
        WaitingToExecute(Decision),
        ReadyToExecute(Decision),
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
                default: throw new AssertionError();
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

    SaveStatus(Status status, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome)
    {
        this(status, definition, executeAt, deps, outcome, NotReady);
    }

    SaveStatus(Status status, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome, LocalExecution execution)
    {
        this(status, status.phase, new Known(definition, executeAt, deps, outcome), execution);
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

    // TODO (expected, testing): exhaustive testing, particularly around PreCommitted
    public static SaveStatus get(Status status, Known known)
    {
        switch (status)
        {
            default: throw new AssertionError();
            case NotDefined: return NotDefined;
            case PreAccepted: return PreAccepted;
            case AcceptedInvalidate:
                // AcceptedInvalidate logically clears any proposed deps and executeAt
                if (!known.executeAt.hasDecidedExecuteAt())
                    return known.isDefinitionKnown() ? AcceptedInvalidateWithDefinition : AcceptedInvalidate;
                // If we know the executeAt decision then we do not clear it, and fall-through to PreCommitted
                // however, we still clear the deps, as any deps we might have previously seen proposed are now expired
                // TODO (expected, consider): consider clearing Command.partialDeps in this case also
                known = known.with(DepsUnknown);
            case Accepted:
                if (!known.executeAt.hasDecidedExecuteAt())
                    return known.isDefinitionKnown() ? AcceptedWithDefinition : Accepted;
                // if the decision is known, we're really PreCommitted
            case PreCommitted:
                if (known.isDefinitionKnown())
                    return known.deps.hasProposedOrDecidedDeps() ? PreCommittedWithDefinitionAndAcceptedDeps : PreCommittedWithDefinition;
                return known.deps.hasProposedOrDecidedDeps() ? PreCommittedWithAcceptedDeps : PreCommitted;
            case Committed: return Committed;
            case ReadyToExecute: return ReadyToExecute;
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
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
                if (known.isSatisfiedBy(status.known))
                    return status;
                return get(status.status, status.known.merge(known));

            case Truncated:
                switch (status)
                {
                    default: throw new AssertionError();
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

    public static SaveStatus merge(SaveStatus a, Ballot acceptedA, SaveStatus b, Ballot acceptedB, boolean preferKnowledge)
    {
        SaveStatus result = max(a, a, acceptedA, b, b, acceptedB, preferKnowledge);
        return enrich(result, (result == a ? b : a).known);
    }

    public static <T> T max(T av, SaveStatus a, Ballot acceptedA, T bv, SaveStatus b, Ballot acceptedB, boolean preferKnowledge)
    {
        if (a == b)
            return av;

        if (a.phase != b.phase)
        {
            return a.phase.compareTo(b.phase) > 0
                   ? (preferKnowledge && a.phase == Phase.Cleanup ? bv : av)
                   : (preferKnowledge && b.phase == Phase.Cleanup ? av : bv);
        }

        if (a.phase == Phase.Accept)
            return acceptedA.compareTo(acceptedB) >= 0 ? av : bv;

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
