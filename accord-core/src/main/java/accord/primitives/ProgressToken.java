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

import accord.coordinate.Outcome;
import accord.local.Command;
import accord.local.Status;
import accord.local.Status.Durability;
import accord.local.Status.Phase;

import javax.annotation.Nonnull;

import static accord.local.Status.AcceptedInvalidate;

/**
 * A representation of activity on a command, so that peers may monitor a command to ensure it is making progress
 */
public class ProgressToken implements Comparable<ProgressToken>, Outcome
{
    public static final ProgressToken NONE = new ProgressToken(Durability.NotDurable, Status.NotDefined, Ballot.ZERO, false);
    public static final ProgressToken INVALIDATED = new ProgressToken(Durability.MajorityOrInvalidated, Status.Invalidated, Ballot.ZERO, false);
    public static final ProgressToken APPLIED = new ProgressToken(Durability.NotDurable, Status.PreApplied, Ballot.ZERO, false);
    public static final ProgressToken DURABLE = new ProgressToken(Durability.Majority, Status.PreApplied, Ballot.ZERO, false);
    public static final ProgressToken TRUNCATED_DURABLE_OR_INVALIDATED = new ProgressToken(Durability.MajorityOrInvalidated, Status.Truncated, Ballot.ZERO, false);

    public final Durability durability;
    public final Status status;
    public final Ballot promised;
    public final boolean isAccepted; // is the *promised ballot* accepted

    public ProgressToken(Durability durability, Status status, Ballot promised, boolean isAccepted)
    {
        this.durability = durability;
        this.status = status;
        this.promised = promised;
        this.isAccepted = isAccepted;
    }

    public ProgressToken(Durability durability, Status status, Ballot promised, Ballot accepted)
    {
        this.durability = durability;
        this.status = status;
        this.promised = promised;
        this.isAccepted = status.phase.compareTo(Phase.Accept) >= 0 && accepted.equals(promised);
    }

    @Override public int compareTo(@Nonnull ProgressToken that)
    {
        int c = this.durability.compareTo(that.durability);
        if (c == 0) c = this.status.phase.compareTo(that.status.phase);
        if (c == 0) c = this.promised.compareTo(that.promised);
        if (c == 0 && this.isAccepted != that.isAccepted) c = this.isAccepted ? 1 : -1;
        return c;
    }
    
    public int compareTo(@Nonnull Command that)
    {
        int c = this.durability.compareTo(that.durability());
        if (c == 0) c = this.status.phase.compareTo(that.status().phase);
        if (c == 0) c = this.promised.compareTo(that.promised());
        if (c == 0 && this.isAccepted != (that.isAccepted() && that.promised().equals(that.acceptedOrCommitted()))) c = this.isAccepted ? 1 : -1;
        return c;
    }

    public ProgressToken merge(ProgressToken that)
    {
        Durability durability = this.durability.compareTo(that.durability) >= 0 ? this.durability : that.durability;
        Status status = this.status.compareTo(that.status) >= 0 ? this.status : that.status;
        Ballot promised = this.promised.compareTo(that.promised) >= 0 ? this.promised : that.promised;
        boolean isAccepted = (this.isAccepted && this.promised.equals(promised)) || (that.isAccepted && that.promised.equals(promised));
        if (isSame(durability, status, promised, isAccepted))
            return this;
        if (that.isSame(durability, status, promised, isAccepted))
            return that;
        return new ProgressToken(durability, status, promised, isAccepted);
    }

    public ProgressToken merge(Command command)
    {
        Durability durability = command.durability();
        if (this.durability.compareTo(command.durability()) > 0)
            durability = this.durability;

        Status status = command.status();
        if (this.status.compareTo(status) > 0)
            status = this.status;

        Ballot promised = command.promised();
        boolean isAccepted = status.hasBeen(AcceptedInvalidate) && command.acceptedOrCommitted().equals(command.promised());
        if (this.promised.compareTo(promised) >= 0)
        {
            promised = this.promised;
            isAccepted = this.isAccepted || (isAccepted && this.promised.equals(promised));
        }

        if (isSame(durability, status, promised, isAccepted))
            return this;
        return new ProgressToken(durability, status, promised, isAccepted);
    }

    private boolean isSame(Durability durability, Status status, Ballot promised, boolean isAccepted)
    {
        return durability == this.durability && status == this.status && promised.equals(this.promised) && isAccepted == this.isAccepted;
    }

    @Override
    public ProgressToken asProgressToken()
    {
        return this;
    }
}
