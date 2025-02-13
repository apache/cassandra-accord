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

import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Agent;
import accord.api.Result;
import accord.local.Cleanup;
import accord.local.Cleanup.Input;
import accord.local.Command;
import accord.local.Command.WaitingOn;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static accord.api.Journal.Load;
import static accord.api.Journal.Load.ALL;
import static accord.impl.CommandChange.Field.ACCEPTED;
import static accord.impl.CommandChange.Field.CLEANUP;
import static accord.impl.CommandChange.Field.DURABILITY;
import static accord.impl.CommandChange.Field.EXECUTES_AT_LEAST;
import static accord.impl.CommandChange.Field.EXECUTE_AT;
import static accord.impl.CommandChange.Field.FIELDS;
import static accord.impl.CommandChange.Field.MIN_UNIQUE_HLC;
import static accord.impl.CommandChange.Field.PARTIAL_DEPS;
import static accord.impl.CommandChange.Field.PARTIAL_TXN;
import static accord.impl.CommandChange.Field.PARTICIPANTS;
import static accord.impl.CommandChange.Field.PROMISED;
import static accord.impl.CommandChange.Field.RESULT;
import static accord.impl.CommandChange.Field.SAVE_STATUS;
import static accord.impl.CommandChange.Field.WAITING_ON;
import static accord.impl.CommandChange.Field.WRITES;
import static accord.local.Cleanup.NO;
import static accord.local.Command.Accepted.accepted;
import static accord.local.Command.Committed.committed;
import static accord.local.Command.Executed.executed;
import static accord.local.Command.NotAcceptedWithoutDefinition.notAccepted;
import static accord.local.Command.NotDefined.notDefined;
import static accord.local.Command.NotDefined.uninitialised;
import static accord.local.Command.PreAccepted.preaccepted;
import static accord.local.Command.Truncated.erased;
import static accord.local.Command.Truncated.invalidated;
import static accord.local.Command.Truncated.vestigial;
import static accord.local.StoreParticipants.Filter.LOAD;
import static accord.primitives.Known.Definition.DefinitionErased;
import static accord.primitives.Known.KnownDeps.DepsErased;
import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtErased;
import static accord.primitives.Known.Outcome.WasApply;
import static accord.primitives.Status.Durability;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Status.Truncated;

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
        MIN_UNIQUE_HLC,
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

    /**
     * SaveStatus.Known contains information about erased / nullified fields,
     * which we can use in order to mark the corresponding fields as changed
     * and setting them to null when they are erased.
     */
    protected static final int[] eraseKnownFieldsMask;

    public static int eraseKnownFieldsMask(SaveStatus status)
    {
        return eraseKnownFieldsMask[status.ordinal()];
    }

    static
    {
        eraseKnownFieldsMask = new int[SaveStatus.values().length];
        for (int i = 0; i < eraseKnownFieldsMask.length; i++)
        {
            SaveStatus saveStatus = SaveStatus.forOrdinal(i);

            int mask = 0;
            if (forceFieldChangedToNullFlag(saveStatus, saveStatus.known::is, DepsErased))
                mask |= setIsNullAndChanged(PARTIAL_DEPS, mask)
                     |  setIsNullAndChanged(WAITING_ON, mask)
                     |  setIsNullAndChanged(MIN_UNIQUE_HLC, mask);
            if (forceFieldChangedToNullFlag(saveStatus, saveStatus.known::is, ExecuteAtErased))
                mask |= setIsNullAndChanged(EXECUTE_AT, mask)
                     |  setIsNullAndChanged(EXECUTES_AT_LEAST, mask);
            if (forceFieldChangedToNullFlag(saveStatus, saveStatus.known::is, DefinitionErased))
                mask |= setIsNullAndChanged(PARTIAL_TXN, mask);
            if (forceFieldChangedToNullFlag(saveStatus, saveStatus.known::is, WasApply))
                mask |= setIsNullAndChanged(RESULT, mask)
                     |  setIsNullAndChanged(WRITES, mask);
            if (saveStatus.hasBeen(Truncated))
                mask |= setIsNullAndChanged(PROMISED, mask)
                     |  setIsNullAndChanged(ACCEPTED, mask);
            if (saveStatus == SaveStatus.Invalidated || saveStatus == SaveStatus.Vestigial)
                mask |= setIsNullAndChanged(DURABILITY, mask);
            eraseKnownFieldsMask[i] = mask;
        }
    }

    private static <T> boolean forceFieldChangedToNullFlag(SaveStatus saveStatus, Predicate<T> predicate, T erased)
    {
        return saveStatus == SaveStatus.Vestigial || predicate.test(erased);
    }

    public static abstract class Builder
    {
        protected final int mask;
        protected int flags;

        protected TxnId txnId;

        protected Timestamp executeAt;
        protected Timestamp executesAtLeast;
        protected long minUniqueHlc;
        protected SaveStatus saveStatus;
        protected Durability durability;

        protected Ballot acceptedOrCommitted;
        protected Ballot promised;

        protected StoreParticipants participants;
        protected PartialTxn partialTxn;
        protected PartialDeps partialDeps;

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

        public SaveStatus saveStatus()
        {
            return saveStatus;
        }

        public Durability durability()
        {
            return durability;
        }

        public StoreParticipants participants()
        {
            return participants;
        }

        public Object get(Field field)
        {
            switch (field)
            {
                case EXECUTE_AT: return executeAt;
                case EXECUTES_AT_LEAST: return executesAtLeast;
                case MIN_UNIQUE_HLC: return minUniqueHlc;
                case SAVE_STATUS: return saveStatus;
                case DURABILITY: return durability;
                case ACCEPTED: return acceptedOrCommitted;
                case PROMISED: return promised;
                case PARTICIPANTS: return participants;
                case PARTIAL_TXN: return partialTxn;
                case PARTIAL_DEPS: return partialDeps;
                case WAITING_ON: return waitingOn;
                case WRITES: return writes;
                case RESULT: return result;
                default: throw new UnhandledEnum(field);
            }
        }

        public void clear()
        {
            flags = 0;
            txnId = null;

            executeAt = null;
            executesAtLeast = null;
            minUniqueHlc = 0;
            saveStatus = null;
            durability = null;

            acceptedOrCommitted = null;
            promised = null;

            participants = null;
            partialTxn = null;
            partialDeps = null;

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
            this.durability = NotDurable;
            this.acceptedOrCommitted = promised = Ballot.ZERO;
            this.waitingOn = (txn, deps, executeAtLeast, uniqueHlc) -> null;
            this.result = null;
        }

        public boolean isEmpty()
        {
            return !nextCalled;
        }

        public int count()
        {
            return count;
        }

        public Cleanup shouldCleanup(Input input, Agent agent, RedundantBefore redundantBefore, DurableBefore durableBefore)
        {
            if (!nextCalled)
                return NO;

            if (saveStatus == null || participants == null)
                return Cleanup.NO;

            Durability durability = this.durability;
            if (durability == null) durability = NotDurable;
            Cleanup cleanup = Cleanup.shouldCleanup(input, agent, txnId, executeAt, saveStatus, durability, participants, redundantBefore, durableBefore);
            if (this.cleanup != null && this.cleanup.compareTo(cleanup) > 0)
                cleanup = this.cleanup;
            return cleanup;
        }

        public boolean maybeCleanup(Input input, Agent agent, RedundantBefore redundantBefore, DurableBefore durableBefore)
        {
            Cleanup cleanup = shouldCleanup(input, agent, redundantBefore, durableBefore);
            return maybeCleanup(input, cleanup);
        }

        public boolean maybeCleanup(Input input, Cleanup cleanup)
        {
            if (saveStatus == null || cleanup == NO)
                return false;

            SaveStatus newSaveStatus = cleanup.appliesIfNot;
            if (saveStatus.compareTo(newSaveStatus) >= 0)
                return false;

            forceSetNulls(eraseKnownFieldsMask[newSaveStatus.ordinal()]);
            if (input == Input.FULL)
            {
                // TODO (expected): this special-casing shouldn't be necessary
                if (newSaveStatus == SaveStatus.TruncatedApply && !saveStatus.known.is(ApplyAtKnown))
                    newSaveStatus = SaveStatus.TruncatedUnapplied;
                saveStatus = newSaveStatus;
            }
            return true;
        }

        protected void setNulls(int mask)
        {
            mask &= ~(flags >>> 16); // limit ourselves to those fields that have not already been set (high 16 bits are those already-set fields)
            forceSetNulls(mask);
        }

        protected void forceSetNulls(int mask)
        {
            mask &= ~nullMask(flags); // limit ourselves to those fields that are not already null
            mask = nullMask(mask); // limit ourselves to those fields that are now being set to null
            int iterable = toIterableSetFields(mask);
            for (Field next = nextSetField(iterable); next != null; iterable = unsetIterable(next, iterable), next = nextSetField(iterable))
            {
                switch (next)
                {
                    default: throw new UnhandledEnum(next);
                    case PARTICIPANTS:      participants = null;                     break;
                    case SAVE_STATUS:       saveStatus = null;                       break;
                    case PARTIAL_DEPS:      partialDeps = null;                      break;
                    case EXECUTE_AT:        executeAt = null;                        break;
                    case EXECUTES_AT_LEAST: executesAtLeast = null;                  break;
                    case MIN_UNIQUE_HLC:    minUniqueHlc = 0;                        break;
                    case DURABILITY:        durability = null;                       break;
                    case ACCEPTED:          acceptedOrCommitted = null;              break;
                    case PROMISED:          promised = null;                         break;
                    case WAITING_ON:        waitingOn = null;                        break;
                    case PARTIAL_TXN:       partialTxn = null;                       break;
                    case WRITES:            writes = null;                           break;
                    case CLEANUP:           cleanup = null;                          break;
                    case RESULT:            result = null;                           break;
                }
            }
            flags |= mask;
        }

        static int nullMask(int mask)
        {
            return (mask & 0xffff) | (mask << 16);
        }

        public Command.Minimal asMinimal()
        {
            return new Command.Minimal(txnId, saveStatus, participants, durability, executeAt);
        }

        public void forceResult(Result newValue)
        {
            this.result = newValue;
        }

        // TODO (expected): we shouldn't need to filter participants here, we will do it anyway before using in SafeCommandStore
        public Command construct(RedundantBefore redundantBefore)
        {
            if (!nextCalled)
                return null;

            Invariants.require(txnId != null);
            if (participants == null) participants = StoreParticipants.empty(txnId);
            else participants = participants.filter(LOAD, redundantBefore, txnId, saveStatus.known.isExecuteAtKnown() ? executeAt : null);

            if (durability == null)
                durability = NotDurable;

            WaitingOn waitingOn = null;
            if (this.waitingOn != null)
                waitingOn = this.waitingOn.provide(txnId, partialDeps, executesAtLeast, minUniqueHlc);

            switch (saveStatus.status)
            {
                case NotDefined:
                    return saveStatus == SaveStatus.Uninitialised ? uninitialised(txnId)
                                                                  : notDefined(txnId, saveStatus, durability, participants, promised);
                case PreAccepted:
                    return preaccepted(txnId, saveStatus, durability, participants, promised, executeAt, partialTxn, partialDeps);
                case AcceptedInvalidate:
                    if (!saveStatus.known.isDefinitionKnown())
                        return notAccepted(txnId, saveStatus, durability, participants, promised, acceptedOrCommitted, partialDeps);
                case AcceptedMedium:
                case AcceptedSlow:
                case PreCommitted:
                    return accepted(txnId, saveStatus, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted);
                case Committed:
                case Stable:
                    return committed(txnId, saveStatus, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted, waitingOn);
                case PreApplied:
                case Applied:
                    return executed(txnId, saveStatus, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted, waitingOn, writes, result);
                case Truncated:
                case Invalidated:
                    return truncated(txnId, saveStatus, durability, participants, executeAt, partialDeps, executesAtLeast, writes, result);
                default:
                    throw new UnhandledEnum(saveStatus.status);
            }
        }

        private static Command.Truncated truncated(TxnId txnId, SaveStatus status, Durability durability, StoreParticipants participants, Timestamp executeAt, PartialDeps partialDeps, Timestamp executesAtLeast, Writes writes, Result result)
        {
            switch (status)
            {
                default: throw new UnhandledEnum(status);
                case TruncatedApplyWithOutcomeAndDeps:
                case TruncatedApplyWithOutcome:
                case TruncatedApply:
                case TruncatedUnapplied:
                    return Command.Truncated.truncated(txnId, status, durability, participants, executeAt, partialDeps, writes, result, executesAtLeast);
                case Vestigial:
                    return vestigial(txnId, participants);
                case Erased:
                    // TODO (expected): why are we saving Durability here for erased commands?
                    return erased(txnId, durability, participants);
                case Invalidated:
                    return invalidated(txnId, participants);
            }
        }

        public String toString()
        {
            return "Builder {" +
                   "txnId=" + txnId
                   + (isChanged(EXECUTE_AT, flags)        ? ", executeAt=" + executeAt : "")
                   + (isChanged(EXECUTES_AT_LEAST, flags) ? ", executesAtLeast=" + executesAtLeast : "")
                   + (isChanged(MIN_UNIQUE_HLC, flags)    ? ", minUniqueHlc=" + minUniqueHlc : "")
                   + (isChanged(SAVE_STATUS, flags)       ? ", saveStatus=" + saveStatus : "")
                   + (isChanged(DURABILITY, flags)        ? ", durability=" + durability : "")
                   + (isChanged(ACCEPTED, flags)          ? ", acceptedOrCommitted=" + acceptedOrCommitted : "")
                   + (isChanged(PROMISED, flags)          ? ", promised=" + promised : "")
                   + (isChanged(PARTICIPANTS, flags)      ? ", participants=" + participants : "")
                   + (isChanged(PARTIAL_DEPS, flags)      ? ", partialTxn=" + partialTxn : "")
                   + (isChanged(PARTIAL_DEPS, flags)      ? ", partialDeps=" + partialDeps : "")
                   + (isChanged(WAITING_ON, flags)        ? ", waitingOn=" + waitingOn : "")
                   + (isChanged(WRITES, flags)            ? ", writes=" + writes : "")
                   + (isChanged(RESULT, flags)            ? ", result=" + result : "")
                   + (isChanged(CLEANUP, flags)           ? ", cleanup=" + cleanup : "") +
                   '}';
        }
    }

    /**
     * Helpers
     */

    public interface WaitingOnProvider
    {
        WaitingOn provide(TxnId txnId, PartialDeps deps, Timestamp executeAtLeast, long uniqueHlc);
    }

    public static long getMinUniqueHlc(Command command)
    {
        WaitingOn waitingOn = command.waitingOn();
        if (waitingOn == null)
            return 0;
        return waitingOn.minUniqueHlc();
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

        // TODO (expected): derive this from precomputed bit masking on Known, only testing equality of objects we can't infer directly
        flags = collectFlags(before, after, Command::executeAt, Timestamp::equalsStrict, true, EXECUTE_AT, flags);
        flags = collectFlags(before, after, Command::executesAtLeast, true, EXECUTES_AT_LEAST, flags);
        flags = collectFlags(before, after, CommandChange::getMinUniqueHlc, MIN_UNIQUE_HLC, flags);

        flags = collectFlags(before, after, Command::saveStatus, false, SAVE_STATUS, flags);
        flags = collectFlags(before, after, Command::durability, false, DURABILITY, flags);

        flags = collectFlags(before, after, Command::acceptedOrCommitted, false, ACCEPTED, flags);
        flags = collectFlags(before, after, Command::promised, false, PROMISED, flags);

        flags = collectFlags(before, after, Command::participants, true, PARTICIPANTS, flags);
        flags = collectFlags(before, after, Command::partialTxn, false, PARTIAL_TXN, flags);
        flags = collectFlags(before, after, Command::partialDeps, false, PARTIAL_DEPS, flags);
        flags = collectFlags(before, after, Command::waitingOn, WaitingOn::equalBitSets, true, WAITING_ON, flags);

        flags = collectFlags(before, after, Command::writes, false, WRITES, flags);
        flags = collectFlags(before, after, Command::result, false, RESULT, flags);

        // make sure we have enough information to decide whether to expunge timestamps (for unique ApplyAt HLC guarantees)
        if (isChanged(EXECUTE_AT, flags) && after.saveStatus().known.is(ApplyAtKnown))
        {
            flags = setChanged(PARTICIPANTS, flags);
            flags = setChanged(SAVE_STATUS, flags);
        }

        flags |= eraseKnownFieldsMask[after.saveStatus().ordinal()];

        return flags;
    }

    private static <OBJ, VAL> int collectFlags(OBJ lo, OBJ ro, Function<OBJ, VAL> convert, boolean allowClassMismatch, Field field, int flags)
    {
        return collectFlags(lo, ro, convert, Object::equals, allowClassMismatch, field, flags);
    }

    private static <OBJ, VAL> int collectFlags(OBJ lo, OBJ ro, Function<OBJ, VAL> convert, BiPredicate<VAL, VAL> equals, boolean allowClassMismatch, Field field, int flags)
    {
        VAL l = null;
        VAL r = null;
        if (lo != null) l = convert.apply(lo);
        if (ro != null) r = convert.apply(ro);

        if (l == r) return flags; // no change
        if (r == null) return setIsNullAndChanged(field, flags);
        if (l == null) return setChanged(field, flags);
        Invariants.require(allowClassMismatch || l.getClass() == r.getClass(), "%s != %s", l.getClass(), r.getClass());
        if (equals.test(l, r)) return flags; // no change
        return setChanged(field, flags);
    }

    private static <OBJ> int collectFlags(OBJ lo, OBJ ro, ToLongFunction<OBJ> convert, Field field, int flags)
    {
        long l = 0, r = 0;
        if (lo != null) l = convert.applyAsLong(lo);
        if (ro != null) r = convert.applyAsLong(ro);

        return l == r ? flags:
                r == 0 ? setIsNullAndChanged(field, flags)
                       : setChanged(field, flags);
    }

    public static boolean anyFieldChanged(int flags)
    {
        return (flags >>> 16) != 0;
    }

    public static int validateFlags(int flags)
    {
        Invariants.require(0 == (~(flags >>> 16) & (flags & 0xffff)));
        return flags;
    }

    public static int setChanged(Field field, int oldFlags)
    {
        return oldFlags | (0x10000 << field.ordinal());
    }

    @VisibleForTesting
    public static boolean isChanged(Field field, int oldFlags)
    {
        return (oldFlags & (0x10000 << field.ordinal())) != 0;
    }

    public static int toIterableSetFields(int flags)
    {
        return flags >>> 16;
    }

    public static int toIterableNonNullFields(int flags)
    {
        return toIterableSetFields(flags) & ~flags;
    }

    public static Field nextSetField(int iterable)
    {
        int i = Integer.numberOfTrailingZeros(Integer.lowestOneBit(iterable));
        return i == 32 ? null : FIELDS[i];
    }

    public static int unsetIterable(Field field, int iterable)
    {
        return iterable & ~(1 << field.ordinal());
    }

    public static String describeFlags(int flags)
    {
        int iterable = toIterableSetFields(flags);
        StringBuilder builder = new StringBuilder("[");
        for (Field next = nextSetField(iterable) ; next != null; next = nextSetField(iterable = unsetIterable(next, iterable)))
        {
            if (builder.length() > 1)
                builder.append(',');
            builder.append(next);
            if (isNull(next, flags))
                builder.append(":null");
        }
        builder.append(']');
        return builder.toString();
    }

    @VisibleForTesting
    public static boolean isNull(Field field, int oldFlags)
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

    public static int setIsNullAndChanged(Field field, int oldFlags)
    {
        return oldFlags | (0x10001 << field.ordinal());
    }

}