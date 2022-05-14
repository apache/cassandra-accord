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

import static accord.local.Status.ExecutionStatus.*;
import static accord.local.Status.Phase.*;
import static accord.local.Status.Known.*;

public enum Status
{
    NotWitnessed      (None,      Undecided),
    PreAccepted       (PreAccept, Undecided),
    AcceptedInvalidate(Accept,    Undecided), // may or may not have witnessed
    Accepted          (Accept,    Undecided), // may or may not have witnessed
    Committed         (Commit,    Decided),
    ReadyToExecute    (Commit,    Decided),
    PreApplied        (Persist,   Decided),
    Applied           (Persist,   Done),
    Invalidated       (Persist,   Done);

    /**
     * Represents the phase of a transaction from the perspective of coordination
     * None:       the transaction is not currently being processed by us (it may be known to us, but only transitively)
     * PreAccept:  the transaction is being disseminated and is seeking an execution order
     * Accept:     the transaction did not achieve 1RT consensus and is making durable its execution order
     * Commit:     the transaction's execution order has been durably decided, and is being disseminated
     * Persist:    the transaction has executed, and its outcome is being persisted
     */
    public enum Phase
    {
        // TODO: see if we can rename Commit here, or in ReplicationPhase, so we can have disjoint terms
        None, PreAccept, Accept, Commit, Persist
    }

    /**
     * Represents the phase of a transaction from the perspective of dissemination of knowledge
     * Nothing:        the transaction definition is not known
     * Definition:     the transaction definition is known
     * ExecutionOrder: the execution order decision is known
     * Apply:          the transaction's execution outcome is known
     * Invalidate:     the transaction is known to be invalidated
     */
    public enum Known
    {
        Nothing(false, false),
        Definition(true, false),
        ExecutionOrder(true, true),
        Outcome(true, true),
        Invalidation(false, false);

        public final boolean hasTxn;
        public final boolean hasDeps;

        Known(boolean hasTxn, boolean hasDeps)
        {
            this.hasTxn = hasTxn;
            this.hasDeps = hasDeps;
        }
    }

    /**
     * Represents the coarse phase of a transaction from the perspective of completion of execution
     * Undecided:  the transaction's execution order is being decided
     * Decided:    the transaction's execution order has been decided
     * Done:       the transaction's execution has completed (and been persisted to underlying storage)
     */
    public enum ExecutionStatus
    {
        Undecided(Nothing),
        Decided(ExecutionOrder),
        Done(Outcome);

        final Known requires;
        ExecutionStatus(Known requires)
        {
            this.requires = requires;
        }
    }

    /**
     * Represents the durability of a transaction's Persist phase.
     * NotDurable: the outcome has not been durably recorded
     * Local:      the outcome has been durably recorded at least locally
     * Durable:    the outcome has been durably recorded to the specified level across the cluster
     * Universal:  the outcome has been durably recorded to every participating replica
     */
    public enum Durability
    {
        NotDurable, Local, Durable, Universal;

        public boolean isDurable()
        {
            return compareTo(Durable) >= 0;
        }
    }

    public final Phase phase;
    public final ExecutionStatus execution;

    Status(Phase phase, ExecutionStatus execution)
    {
        this.phase = phase;
        this.execution = execution;
    }

    public static Status max(Status a, Status b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    // TODO: investigate all uses of hasBeen, and migrate as many as possible to testing Phase, ReplicationPhase and ExecutionStatus
    //       where these concepts are inadequate, see if additional concepts can be introduced
    public boolean hasBeen(Status equalOrGreaterThan)
    {
        return compareTo(equalOrGreaterThan) >= 0;
    }

    public boolean isAtLeast(Phase equalOrGreaterThan)
    {
        return phase.compareTo(equalOrGreaterThan) >= 0;
    }
}
