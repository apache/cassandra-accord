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

import accord.local.Status.ExecutionStatus;
import accord.local.Status.Known;
import accord.local.Status.Phase;

import static accord.local.Status.ExecutionStatus.*;
import static accord.local.Status.Known.*;

/**
 * Identical to Status but preserves whether we have previously been PreAccepted and therefore know the definition
 * of the transaction. This would potentially complicate users of Status, and the distributed state machine is complicated
 * enough. But it helps to formalise the relationships here as an auxiliary enum.
 * Intended to be used internally by Command implementations.
 */
public enum SaveStatus
{
    NotWitnessed                    (Status.NotWitnessed,       Nothing),
    PreAccepted                     (Status.PreAccepted,        Definition),
    AcceptedInvalidate              (Status.AcceptedInvalidate, Nothing), // may or may not have witnessed
    AcceptedInvalidateWithDefinition(Status.AcceptedInvalidate, Definition), // may or may not have witnessed
    Accepted                        (Status.Accepted,           Nothing), // may or may not have witnessed
    AcceptedWithDefinition          (Status.Accepted,           Definition), // may or may not have witnessed
    Committed                       (Status.Committed,          ExecutionOrder),
    ReadyToExecute                  (Status.ReadyToExecute,     ExecutionOrder),
    PreApplied                      (Status.PreApplied,         Outcome),
    Applied                         (Status.Applied,            Outcome),
    Invalidated                     (Status.Invalidated,        Invalidation);
    
    public final Status status;
    public final Phase phase;
    public final Known known;
    public final ExecutionStatus execution;

    SaveStatus(Status status, Known known)
    {
        this.status = status;
        this.phase = status.phase;
        this.known = known;
        this.execution = status.execution;
    }

    public boolean isAtLeast(Phase phase)
    {
        return this.phase.compareTo(phase) >= 0;
    }

    public boolean hasBeen(Status status)
    {
        return this.status.compareTo(status) >= 0;
    }

    public static SaveStatus get(Status status, Known known)
    {
        switch (status)
        {
            default: throw new AssertionError();
            case NotWitnessed: return NotWitnessed;
            case PreAccepted: return PreAccepted;
            case AcceptedInvalidate: return known.compareTo(Definition) >= 0 ? AcceptedInvalidateWithDefinition : AcceptedInvalidate;
            case Accepted: return known.compareTo(Definition) >= 0 ? AcceptedWithDefinition : Accepted;
            case Committed: return Committed;
            case ReadyToExecute: return ReadyToExecute;
            case PreApplied: return PreApplied;
            case Applied: return Applied;
            case Invalidated: return Invalidated;
        }
    }
}
