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

import static accord.local.Status.Definition.DefinitionKnown;
import static accord.local.Status.Definition.DefinitionUnknown;
import static accord.local.Status.KnownDeps.*;
import static accord.local.Status.KnownExecuteAt.*;
import static accord.local.Status.Outcome.OutcomeUnknown;

/**
 * Identical to Status but preserves whether we have previously been PreAccepted and therefore know the definition
 * of the transaction. This would potentially complicate users of Status, and the distributed state machine is complicated
 * enough. But it helps to formalise the relationships here as an auxiliary enum.
 * Intended to be used internally by Command implementations.
 */
public enum SaveStatus
{
    NotWitnessed                    (Status.NotWitnessed),
    PreAccepted                     (Status.PreAccepted),
    AcceptedInvalidate              (Status.AcceptedInvalidate),
    AcceptedInvalidateWithDefinition(Status.AcceptedInvalidate,    DefinitionKnown,   ExecuteAtUnknown,  DepsUnknown,  OutcomeUnknown),
    Accepted                        (Status.Accepted),
    AcceptedWithDefinition          (Status.Accepted,              DefinitionKnown,   ExecuteAtProposed, DepsProposed, OutcomeUnknown),
    PreCommitted                    (Status.PreCommitted),
    PreCommittedWithAcceptedDeps    (Status.PreCommitted,          DefinitionUnknown, ExecuteAtKnown,    DepsProposed, OutcomeUnknown),
    PreCommittedWithDefinition      (Status.PreCommitted,          DefinitionKnown,   ExecuteAtKnown,    DepsUnknown,  OutcomeUnknown),
    PreCommittedWithDefinitionAndAcceptedDeps(Status.PreCommitted, DefinitionKnown,   ExecuteAtKnown,    DepsProposed, OutcomeUnknown),
    Committed                       (Status.Committed),
    ReadyToExecute                  (Status.ReadyToExecute),
    PreApplied                      (Status.PreApplied),
    Applied                         (Status.Applied),
    Invalidated                     (Status.Invalidated);
    
    public final Status status;
    public final Phase phase;
    public final Known known; // TODO (easy, API/efficiency): duplicate contents here to reduce indirection for majority of cases

    SaveStatus(Status status)
    {
        this.status = status;
        this.phase = status.phase;
        this.known = status.minKnown;
    }

    SaveStatus(Status status, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome)
    {
        this.status = status;
        this.phase = status.phase;
        this.known = new Known(definition, executeAt, deps, outcome);
    }

    public boolean hasBeen(Status status)
    {
        return this.status.compareTo(status) >= 0;
    }

    // TODO (expected, testing): exhaustive testing, particularly around PreCommitted
    public static SaveStatus get(Status status, Known known)
    {
        switch (status)
        {
            default: throw new AssertionError();
            case NotWitnessed: return NotWitnessed;
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
        if (known.isSatisfiedBy(status.known))
            return status;
        return get(status.status, status.known.merge(known));
    }

    public static SaveStatus merge(SaveStatus a, Ballot acceptedA, SaveStatus b, Ballot acceptedB)
    {
        SaveStatus prefer;
        if (a.phase != b.phase) prefer = a.phase.compareTo(b.phase) >= 0 ? a : b;
        else if (a.phase == Phase.Accept) prefer = acceptedA.compareTo(acceptedB) >= 0 ? a : b;
        else prefer = a.compareTo(b) >= 0 ? a : b;
        return SaveStatus.enrich(prefer, (prefer == a ? b : a).known);
    }
}
