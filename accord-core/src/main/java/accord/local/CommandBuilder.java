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

import accord.api.Result.PersistableResult;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static accord.primitives.Status.Durability.NotDurable;

public class CommandBuilder
{
    private TxnId txnId;
    private Status.Durability durability;
    private StoreParticipants participants;
    private Ballot promised = Ballot.ZERO;
    private PartialTxn partialTxn;
    private PartialDeps partialDeps;
    private Timestamp executeAt;
    private Ballot acceptedOrCommitted = Ballot.ZERO;
    private Command.WaitingOn waitingOn;
    private Writes writes;
    private PersistableResult result;

    public CommandBuilder(TxnId txnId)
    {
        this.txnId = txnId;
        this.participants = StoreParticipants.empty(txnId);
        this.durability = NotDurable;
    }

    public CommandBuilder(Command copy)
    {
        this.txnId = copy.txnId();
        this.durability = copy.durability();
        this.participants = Invariants.nonNull(copy.participants());
        this.promised = copy.promised();
        this.partialTxn = copy.partialTxn();
        this.partialDeps = copy.partialDeps();
        this.executeAt = copy.executeAt();
        this.acceptedOrCommitted = copy.acceptedOrCommitted();
        this.waitingOn = copy.waitingOn();
        this.writes = copy.writes();
        this.result = copy.result();
    }

    public TxnId txnId()
    {
        return txnId;
    }

    public CommandBuilder txnId(TxnId txnId)
    {
        this.txnId = txnId;
        return this;
    }

    public Status.Durability durability()
    {
        return durability;
    }

    public CommandBuilder durability(Status.Durability durability)
    {
        this.durability = durability;
        return this;
    }

    public StoreParticipants participants()
    {
        return participants;
    }

    public Ballot promised()
    {
        return promised;
    }

    public CommandBuilder promised(Ballot promised)
    {
        this.promised = promised;
        return this;
    }

    public CommandBuilder participants(StoreParticipants participants)
    {
        this.participants = participants;
        return this;
    }

    public PartialTxn partialTxn()
    {
        return partialTxn;
    }

    public CommandBuilder partialTxn(PartialTxn partialTxn)
    {
        this.partialTxn = partialTxn;
        return this;
    }

    public PartialDeps partialDeps()
    {
        return partialDeps;
    }

    public Timestamp executeAt()
    {
        return executeAt;
    }

    public CommandBuilder executeAt(Timestamp executeAt)
    {
        this.executeAt = executeAt;
        return this;
    }

    public Ballot acceptedOrCommitted()
    {
        return acceptedOrCommitted;
    }

    public Command.WaitingOn waitingOn()
    {
        return waitingOn;
    }

    public CommandBuilder waitingOn(Command.WaitingOn waitingOn)
    {
        this.waitingOn = waitingOn;
        return this;
    }

    public Writes writes()
    {
        return writes;
    }

    public CommandBuilder writes(Writes writes)
    {
        this.writes = writes;
        return this;
    }

    public PersistableResult result()
    {
        return result;
    }

    public CommandBuilder result(PersistableResult result)
    {
        this.result = result;
        return this;
    }

    public CommandBuilder acceptedOrCommitted(Ballot acceptedOrCommitted)
    {
        this.acceptedOrCommitted = acceptedOrCommitted;
        return this;
    }

    public CommandBuilder partialDeps(PartialDeps partialDeps)
    {
        this.partialDeps = partialDeps;
        return this;
    }

    public Command build(SaveStatus saveStatus)
    {
        switch (saveStatus)
        {
            default: throw new UnhandledEnum(saveStatus);
            case Uninitialised:
                return Command.NotDefined.uninitialised(txnId);
            case NotDefined:
                return Command.NotDefined.notDefined(txnId, saveStatus, durability, participants, promised);
            case PreAccepted:
            case PreAcceptedWithDeps:
            case PreAcceptedWithVote:
                return Command.PreAccepted.preaccepted(txnId, saveStatus, durability, participants, promised, executeAt, partialTxn, partialDeps);
            case AcceptedInvalidate:
                return Command.NotAcceptedWithoutDefinition.notAccepted(txnId, saveStatus, durability, participants, promised, acceptedOrCommitted, partialDeps);
            case AcceptedInvalidateWithDefinition:
            case AcceptedMedium:
            case AcceptedMediumWithDefinition:
            case AcceptedMediumWithDefAndVote:
            case AcceptedSlow:
            case AcceptedSlowWithDefinition:
            case AcceptedSlowWithDefAndVote:
            case PreCommitted:
            case PreCommittedWithDeps:
            case PreCommittedWithDefAndDeps:
            case PreCommittedWithDefinition:
            case PreCommittedWithDefAndFixedDeps:
            case PreCommittedWithFixedDeps:
                return Command.Accepted.accepted(txnId, saveStatus, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted);
            case ReadyToExecute:
            case Committed:
            case Stable:
                return Command.Committed.committed(txnId, saveStatus, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted, waitingOn);
            case PreApplied:
            case Applying:
            case Applied:
                return Command.Executed.executed(txnId, saveStatus, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted, waitingOn, writes, result);
            case TruncatedApplyWithOutcome:
            case TruncatedApply:
            case TruncatedUnapplied:
                if (txnId.awaitsOnlyDeps())
                    return Command.Truncated.truncated(txnId, saveStatus, durability, participants, executeAt, partialDeps, writes, result, null);
                return Command.Truncated.truncated(txnId, saveStatus, durability, participants, executeAt, partialDeps, writes, result);
            case Erased:
                return Command.Truncated.erased(txnId);
            case Vestigial:
                return Command.Truncated.vestigial(txnId, participants);
            case Invalidated:
                return Command.Truncated.invalidated(txnId, participants);
        }
    }
}
