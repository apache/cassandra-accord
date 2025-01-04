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

package accord.impl;

import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Agent;
import accord.api.Result;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;

import static accord.api.Journal.Load;
import static accord.api.Journal.Load.ALL;
import static accord.impl.CommandChange.Field.ACCEPTED;
import static accord.impl.CommandChange.Field.CLEANUP;
import static accord.impl.CommandChange.Field.DURABILITY;
import static accord.impl.CommandChange.Field.EXECUTES_AT_LEAST;
import static accord.impl.CommandChange.Field.EXECUTE_AT;
import static accord.impl.CommandChange.Field.FIELDS;
import static accord.impl.CommandChange.Field.PARTIAL_DEPS;
import static accord.impl.CommandChange.Field.PARTIAL_TXN;
import static accord.impl.CommandChange.Field.PARTICIPANTS;
import static accord.impl.CommandChange.Field.PROMISED;
import static accord.impl.CommandChange.Field.RESULT;
import static accord.impl.CommandChange.Field.SAVE_STATUS;
import static accord.impl.CommandChange.Field.WAITING_ON;
import static accord.impl.CommandChange.Field.WRITES;
import static accord.local.Cleanup.NO;
import static accord.local.Cleanup.TRUNCATE_WITH_OUTCOME;
import static accord.primitives.SaveStatus.TruncatedApplyWithOutcome;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.utils.Invariants.illegalState;

public class CommandChange
{
    // This enum is order-dependent
    public enum Field
    {
        PARTICIPANTS, // stored first so we can index it
        SAVE_STATUS,
        PARTIAL_DEPS,
        EXECUTE_AT,
        EXECUTES_AT_LEAST,
        DURABILITY,
        ACCEPTED,
        PROMISED,
        WAITING_ON,
        PARTIAL_TXN,
        WRITES,
        CLEANUP,
        RESULT,
        ;

        public static final Field[] FIELDS = values();
    }

    public static class Builder
    {
        protected final int mask;
        protected int flags;

        protected TxnId txnId;

        protected Timestamp executeAt;
        protected Timestamp executeAtLeast;
        protected SaveStatus saveStatus;
        protected Status.Durability durability;

        protected Ballot acceptedOrCommitted;
        protected Ballot promised;

        protected StoreParticipants participants;
        protected PartialTxn partialTxn;
        protected PartialDeps partialDeps;

        protected byte[] waitingOnBytes;
        protected CommandChange.WaitingOnProvider waitingOn;
        protected Writes writes;
        protected Result result;
        protected Cleanup cleanup;

        protected boolean nextCalled;
        protected int count;

        public Builder(TxnId txnId, Load load)
        {
            this.mask = mask(load);
            init(txnId);
        }

        public Builder(TxnId txnId)
        {
            this(txnId, ALL);
        }

        public Builder(Load load)
        {
            this.mask = mask(load);
        }

        public Builder()
        {
            this(ALL);
        }

        public TxnId txnId()
        {
            return txnId;
        }

        public Timestamp executeAt()
        {
            return executeAt;
        }

        // TODO: why is this unused in BurnTest
        public Timestamp executeAtLeast()
        {
            return executeAtLeast;
        }

        public SaveStatus saveStatus()
        {
            return saveStatus;
        }

        public Status.Durability durability()
        {
            return durability;
        }

        public Ballot acceptedOrCommitted()
        {
            return acceptedOrCommitted;
        }

        public Ballot promised()
        {
            return promised;
        }

        public StoreParticipants participants()
        {
            return participants;
        }

        public PartialTxn partialTxn()
        {
            return partialTxn;
        }

        public PartialDeps partialDeps()
        {
            return partialDeps;
        }

        public CommandChange.WaitingOnProvider waitingOn()
        {
            return waitingOn;
        }

        public Writes writes()
        {
            return writes;
        }

        public Result result()
        {
            return result;
        }

        public void clear()
        {
            flags = 0;
            txnId = null;

            executeAt = null;
            executeAtLeast = null;
            saveStatus = null;
            durability = null;

            acceptedOrCommitted = null;
            promised = null;

            participants = null;
            partialTxn = null;
            partialDeps = null;

            waitingOnBytes = null;
            waitingOn = null;
            writes = null;
            result = null;
            cleanup = null;

            nextCalled = false;
            count = 0;
        }

        public void reset(TxnId txnId)
        {
            clear();
            init(txnId);
        }

        public void init(TxnId txnId)
        {
            this.txnId = txnId;
            durability = NotDurable;
            acceptedOrCommitted = promised = Ballot.ZERO;
            waitingOn = (txn, deps) -> null;
            result = null;
        }

        public boolean isEmpty()
        {
            return !nextCalled;
        }

        public int count()
        {
            return count;
        }

        public Cleanup shouldCleanup(Agent agent, RedundantBefore redundantBefore, DurableBefore durableBefore)
        {
            if (!nextCalled)
                return NO;

            if (saveStatus == null || participants == null)
                return Cleanup.NO;

            Cleanup cleanup = Cleanup.shouldCleanupPartial(agent, txnId, saveStatus, durability, participants, redundantBefore, durableBefore);
            if (this.cleanup != null && this.cleanup.compareTo(cleanup) > 0)
                cleanup = this.cleanup;
            return cleanup;
        }

        public Builder maybeCleanup(Cleanup cleanup)
        {
            if (saveStatus() == null)
                return this;

            switch (cleanup)
            {
                case EXPUNGE:
                case ERASE:
                    return null;

                case EXPUNGE_PARTIAL:
                    return expungePartial(cleanup, saveStatus, true);

                case VESTIGIAL:
                case INVALIDATE:
                    return saveStatusOnly();

                case TRUNCATE_WITH_OUTCOME:
                case TRUNCATE:
                    return expungePartial(cleanup, cleanup.appliesIfNot, cleanup == TRUNCATE_WITH_OUTCOME);

                case NO:
                    return this;
                default:
                    throw new IllegalStateException("Unknown cleanup: " + cleanup);}
        }

        public Builder expungePartial(Cleanup cleanup, SaveStatus saveStatus, boolean includeOutcome)
        {
            Invariants.checkState(txnId != null);
            Builder builder = new Builder(txnId, ALL);

            builder.count++;
            builder.nextCalled = true;

            Invariants.checkState(saveStatus != null);
            builder.flags = setFieldChanged(SAVE_STATUS, builder.flags);
            builder.saveStatus = saveStatus;
            builder.flags = setFieldChanged(CLEANUP, builder.flags);
            builder.cleanup = cleanup;
            if (executeAt != null)
            {
                builder.flags = setFieldChanged(EXECUTE_AT, builder.flags);
                builder.executeAt = executeAt;
            }
            if (durability != null)
            {
                builder.flags = setFieldChanged(DURABILITY, builder.flags);
                builder.durability = durability;
            }
            if (participants != null)
            {
                builder.flags = setFieldChanged(PARTICIPANTS, builder.flags);
                builder.participants = participants;
            }
            if (includeOutcome && builder.writes != null)
            {
                builder.flags = setFieldChanged(WRITES, builder.flags);
                builder.writes = writes;
            }

            return builder;
        }

        public Builder saveStatusOnly()
        {
            Invariants.checkState(txnId != null);
            Builder builder = new Builder(txnId, ALL);

            builder.count++;
            builder.nextCalled = true;

            if (saveStatus != null)
            {
                builder.flags = setFieldChanged(SAVE_STATUS, builder.flags);
                builder.saveStatus = saveStatus;
            }

            return builder;
        }

        public Command.Minimal asMinimal()
        {
            return new Command.Minimal(txnId, saveStatus, participants, durability, executeAt, writes);
        }

        public void forceResult(Result newValue)
        {
            this.result = newValue;
        }

        public Command construct(RedundantBefore redundantBefore)
        {
            if (!nextCalled)
                return null;

            Invariants.checkState(txnId != null);
            CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
            if (partialTxn != null)
                attrs.partialTxn(partialTxn);
            if (durability != null)
                attrs.durability(durability);
            if (participants != null)
                attrs.setParticipants(participants.filter(StoreParticipants.Filter.LOAD, redundantBefore, txnId, saveStatus.known.executeAt.isDecidedAndKnownToExecute() ? executeAt : null));
            if (participants != null)
                attrs.setParticipants(participants);
            if (partialDeps != null && saveStatus.known.deps.hasProposedOrDecidedDeps())
                attrs.partialDeps(partialDeps);

            switch (saveStatus.known.outcome)
            {
                case Erased:
                case WasApply:
                    writes = null;
                    result = null;
                    break;
            }

            Command.WaitingOn waitingOn = null;
            if (this.waitingOn != null)
                waitingOn = this.waitingOn.provide(txnId, partialDeps);

            switch (saveStatus.status)
            {
                case NotDefined:
                    return saveStatus == SaveStatus.Uninitialised ? Command.NotDefined.uninitialised(attrs.txnId())
                                                                  : Command.NotDefined.notDefined(attrs, promised);
                case PreAccepted:
                    return Command.PreAccepted.preAccepted(attrs, executeAt, promised);
                case AcceptedInvalidate:
                    if (!saveStatus.known.isDefinitionKnown())
                        return Command.AcceptedInvalidateWithoutDefinition.acceptedInvalidate(attrs, promised, acceptedOrCommitted);
                case Accepted:
                case PreCommitted:
                    return Command.Accepted.accepted(attrs, saveStatus, executeAt, promised, acceptedOrCommitted);

                case Committed:
                case Stable:
                    return Command.Committed.committed(attrs, saveStatus, executeAt, promised, acceptedOrCommitted, waitingOn);
                case PreApplied:
                case Applied:
                    return Command.Executed.executed(attrs, saveStatus, executeAt, promised, acceptedOrCommitted, waitingOn, writes, result);
                case Truncated:
                case Invalidated:
                    return truncated(attrs, saveStatus, executeAt, executeAtLeast, writes, result);
                default:
                    throw new IllegalStateException();
            }
        }

        private static Command.Truncated truncated(CommonAttributes.Mutable attrs, SaveStatus status, Timestamp executeAt, Timestamp executesAtLeast, Writes writes, Result result)
        {
            switch (status)
            {
                default:
                    throw illegalState("Unhandled SaveStatus: " + status);
                case TruncatedApplyWithOutcome:
                case TruncatedApplyWithDeps:
                case TruncatedApply:
                    if (status != TruncatedApplyWithOutcome)
                        result = null;
                    if (attrs.txnId().kind().awaitsOnlyDeps())
                        return Command.Truncated.truncatedApply(attrs, status, executeAt, writes, result, executesAtLeast);
                    return Command.Truncated.truncatedApply(attrs, status, executeAt, writes, result, null);
                case ErasedOrVestigial:
                    return Command.Truncated.erasedOrVestigial(attrs.txnId(), attrs.participants());
                case Erased:
                    // TODO (expected): why are we saving Durability here for erased commands?
                    return Command.Truncated.erased(attrs.txnId(), attrs.durability(), attrs.participants());
                case Invalidated:
                    return Command.Truncated.invalidated(attrs.txnId(), attrs.participants());
            }
        }

        public String toString()
        {
            return "Builder {" +
                   "txnId=" + txnId +
                   ", executeAt=" + executeAt +
                   ", saveStatus=" + saveStatus +
                   ", durability=" + durability +
                   ", acceptedOrCommitted=" + acceptedOrCommitted +
                   ", promised=" + promised +
                   ", participants=" + participants +
                   ", partialTxn=" + partialTxn +
                   ", partialDeps=" + partialDeps +
                   ", waitingOn=" + waitingOn +
                   ", writes=" + writes +
                   '}';
        }
    }

    /**
     * Helpers
     */

    public interface WaitingOnProvider
    {
        Command.WaitingOn provide(TxnId txnId, PartialDeps deps);
    }

    public static Command.WaitingOn getWaitingOn(Command command)
    {
        if (command instanceof Command.Committed)
            return command.asCommitted().waitingOn();

        return null;
    }

    /**
     * Managing masks
     */

    public static int mask(Field... fields)
    {
        int mask = -1;
        for (Field field : fields)
            mask &= ~(1 << field.ordinal());
        return mask;
    }

    private static final int[] LOAD_MASKS = new int[] {0,
                                                       mask(SAVE_STATUS, PARTICIPANTS, DURABILITY, EXECUTE_AT, WRITES),
                                                       mask(SAVE_STATUS, PARTICIPANTS, EXECUTE_AT)};

    public static int mask(Load load)
    {
        return LOAD_MASKS[load.ordinal()];
    }

    /**
     * Managing flags
     */

    @VisibleForTesting
    public static int getFlags(Command before, Command after)
    {
        int flags = 0;
        if (before == null && after == null)
            return flags;

        flags = collectFlags(before, after, Command::executeAt, true, EXECUTE_AT, flags);
        flags = collectFlags(before, after, Command::executesAtLeast, true, EXECUTES_AT_LEAST, flags);
        flags = collectFlags(before, after, Command::saveStatus, false, SAVE_STATUS, flags);
        flags = collectFlags(before, after, Command::durability, false, DURABILITY, flags);

        flags = collectFlags(before, after, Command::acceptedOrCommitted, false, ACCEPTED, flags);
        flags = collectFlags(before, after, Command::promised, false, PROMISED, flags);

        flags = collectFlags(before, after, Command::participants, true, PARTICIPANTS, flags);
        flags = collectFlags(before, after, Command::partialTxn, false, PARTIAL_TXN, flags);
        flags = collectFlags(before, after, Command::partialDeps, false, PARTIAL_DEPS, flags);

        // TODO: waitingOn vs WaitingOnWithExecutedAt?
        flags = collectFlags(before, after, CommandChange::getWaitingOn, true, WAITING_ON, flags);

        flags = collectFlags(before, after, Command::writes, false, WRITES, flags);

        // Special-cased for Journal BurnTest integration
        if ((before != null && after.result() != before.result()) ||
            (before == null && after.result() != null))
        {
            flags = collectFlags(before, after, Command::writes, false, RESULT, flags);
        }

        return flags;
    }

    private static <OBJ, VAL> int collectFlags(OBJ lo, OBJ ro, Function<OBJ, VAL> convert, boolean allowClassMismatch, Field field, int flags)
    {
        VAL l = null;
        VAL r = null;
        if (lo != null) l = convert.apply(lo);
        if (ro != null) r = convert.apply(ro);

        if (l == r)
            return flags; // no change

        if (r == null)
            flags = setFieldIsNull(field, flags);

        if (l == null || r == null)
            return setFieldChanged(field, flags);

        assert allowClassMismatch || l.getClass() == r.getClass() : String.format("%s != %s", l.getClass(), r.getClass());

        if (l.equals(r))
            return flags; // no change

        return setFieldChanged(field, flags);
    }

    // TODO (required): calculate flags once
    public static boolean anyFieldChanged(int flags)
    {
        return (flags >>> 16) != 0;
    }

    public static int validateFlags(int flags)
    {
        Invariants.checkState(0 == (~(flags >>> 16) & (flags & 0xffff)));
        return flags;
    }

    public static int setFieldChanged(Field field, int oldFlags)
    {
        return oldFlags | (0x10000 << field.ordinal());
    }

    @VisibleForTesting
    public static boolean getFieldChanged(Field field, int oldFlags)
    {
        return (oldFlags & (0x10000 << field.ordinal())) != 0;
    }

    public static int toIterableSetFields(int flags)
    {
        return flags >>> 16;
    }

    public static Field nextSetField(int iterable)
    {
        int i = Integer.numberOfTrailingZeros(Integer.lowestOneBit(iterable));
        return i == 32 ? null : FIELDS[i];
    }

    public static int unsetIterableFields(Field field, int iterable)
    {
        return iterable & ~(1 << field.ordinal());
    }

    @VisibleForTesting
    public static boolean getFieldIsNull(Field field, int oldFlags)
    {
        return (oldFlags & (1 << field.ordinal())) != 0;
    }

    public static int unsetFieldIsNull(Field field, int oldFlags)
    {
        return oldFlags & ~(1 << field.ordinal());
    }

    public static int setFieldIsNull(Field field, int oldFlags)
    {
        return oldFlags | (1 << field.ordinal());
    }

}