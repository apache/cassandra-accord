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

import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

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
import static accord.local.Cleanup.ERASE;
import static accord.local.Cleanup.EXPUNGE;
import static accord.local.Cleanup.NO;
import static accord.local.Cleanup.VESTIGIAL;
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
        DURABILITY,
        EXECUTE_AT,
        PROMISED,
        ACCEPTED,
        PARTIAL_TXN,
        PARTIAL_DEPS,
        WAITING_ON,
        MIN_UNIQUE_HLC,
        EXECUTES_AT_LEAST,
        WRITES,
        RESULT,
        CLEANUP,
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
        eraseKnownFieldsMask[VESTIGIAL.ordinal()] = eraseKnownFieldsMask[ERASE.ordinal()];
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

        protected StoreParticipants participants;
        protected SaveStatus saveStatus;
        protected Durability durability;
        protected Timestamp executeAt;

        protected Ballot promised;
        protected Ballot acceptedOrCommitted;

        protected PartialTxn partialTxn;
        protected PartialDeps partialDeps;

        protected CommandChange.WaitingOnProvider waitingOn;
        protected long minUniqueHlc;
        protected Timestamp executesAtLeast;

        protected Writes writes;
        protected Result result;

        protected Cleanup cleanup;

        protected boolean hasUpdate;
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

        public Timestamp executeAt()
        {
            return executeAt;
        }

        public Timestamp executesAtLeast()
        {
            return executesAtLeast;
        }

        public PartialTxn partialTxn()
        {
            return partialTxn;
        }

        public PartialDeps partialDeps()
        {
            return partialDeps;
        }

        public Writes writes()
        {
            return writes;
        }

        public Result result()
        {
            return result;
        }

        public StoreParticipants participants()
        {
            return participants;
        }

        public Object get(Field field)
        {
            switch (field)
            {
                case PARTICIPANTS: return participants;
                case SAVE_STATUS: return saveStatus;
                case DURABILITY: return durability;
                case EXECUTE_AT: return executeAt;
                case PROMISED: return promised;
                case ACCEPTED: return acceptedOrCommitted;
                case PARTIAL_TXN: return partialTxn;
                case PARTIAL_DEPS: return partialDeps;
                case WAITING_ON: return waitingOn;
                case MIN_UNIQUE_HLC: return minUniqueHlc;
                case EXECUTES_AT_LEAST: return executesAtLeast;
                case WRITES: return writes;
                case RESULT: return result;
                default: throw new UnhandledEnum(field);
            }
        }

        public void clear()
        {
            flags = 0;
            txnId = null;

            participants = null;
            saveStatus = null;
            durability = null;
            executeAt = null;

            promised = null;
            acceptedOrCommitted = null;

            partialTxn = null;
            partialDeps = null;

            waitingOn = null;
            minUniqueHlc = 0;
            executesAtLeast = null;

            writes = null;
            result = null;
            cleanup = null;

            hasUpdate = false;
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
            return !hasUpdate;
        }

        public int flags()
        {
            return flags;
        }

        public int count()
        {
            return count;
        }

        public Cleanup shouldCleanup(Input input, RedundantBefore redundantBefore, DurableBefore durableBefore)
        {
            // Early return: No cleanup needed when no updates have been made to this command
            if (!hasUpdate)
                return NO;

            // Honor previously set cleanup requirements - cleanup levels are ordered by aggressiveness
            if (cleanup != null)
            {
                switch (cleanup)
                {
                    case EXPUNGE:
                        // Already marked for complete removal from storage
                        return EXPUNGE;
                    case ERASE:
                        // Check if erased command qualifies for full expungement
                        if (EXPUNGE == Cleanup.shouldCleanup(input, txnId, null, SaveStatus.Erased, NotDurable, null, redundantBefore, durableBefore))
                            return EXPUNGE;
                        // Otherwise maintain erase-level cleanup
                        return ERASE;
                }
            }

            Durability durability = this.durability;
            if (durability == null) durability = NotDurable;
            StoreParticipants participants = this.participants;
            // TODO (expected): we need to filter participants to correctly compute doesStillExecute in Cleanup.shouldCleanup;
            //  would be better to break this dependency, or otherwise encode it better.
            //  In particular it would be nice to avoid doing this twice for each command on load, as we also do this in SafeCommandStore.
            //  Perhaps we can special-case loading, and simply update the participants here so we can avoid doing it again on access
            if (input == Input.FULL)
            {
                // During full compaction, commands without save status can be completely expunged
                if (saveStatus == null)
                    return EXPUNGE;
                
                if (participants != null)
                    participants = participants.filter(LOAD, redundantBefore, txnId, saveStatus.known.isExecuteAtKnown() ? executeAt : null);
            }
            Cleanup cleanup = Cleanup.shouldCleanup(input, txnId, executeAt, saveStatus, durability, participants, redundantBefore, durableBefore);
            // Upgrade cleanup level iif a more aggressive cleanup was previously requested
            if (this.cleanup != null && this.cleanup.compareTo(cleanup) > 0)
                cleanup = this.cleanup;
            return cleanup;
        }

        public Cleanup maybeCleanup(boolean clearFields, Input input, RedundantBefore redundantBefore, DurableBefore durableBefore)
        {
            Cleanup cleanup = shouldCleanup(input, redundantBefore, durableBefore);
            return maybeCleanup(clearFields, cleanup);
        }

        public Cleanup maybeCleanup(boolean clearFields, Cleanup cleanup)
        {
            if (cleanup == NO)
                return cleanup;

            forceSetNulls(clearFields, eraseKnownFieldsMask[cleanup.newStatus.ordinal()]);
            if (this.cleanup == null || this.cleanup.compareTo(cleanup) < 0)
            {
                this.hasUpdate = true;
                this.flags |= setChanged(CLEANUP);
                this.cleanup = cleanup;
            }
            return cleanup;
        }

        protected void setNulls(boolean clearFields, int newFlags)
        {
            newFlags &= ~(flags >>> 16); // limit ourselves to those fields that have not already been set (high 16 bits are those already-set fields)
            forceSetNulls(clearFields, newFlags);
        }

        protected boolean forceSetNulls(boolean clearFields, int newFlags)
        {
            newFlags &= ~nulls(flags); // limit ourselves to those fields that are not already null
            newFlags = nulls(newFlags); // limit ourselves to those fields that are now being set to null
            if (newFlags == 0)
                return false;

            if (clearFields)
                clearFields(newFlags);

            flags |= newFlags;
            return true;
        }

        // clears any field with a CHANGED flag NOT limited only to NULL
        private void clearFields(int newFlags)
        {
            newFlags &= notNulls(flags); // limit ourselves to those fields that are not already null
            int iterable = toIterableSetFields(newFlags);
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
        }

        // only populate regular fields that are not already set, but apply any Cleanup if it is stronger than any already present
        public boolean fillInMissingOrCleanup(boolean clearFields, Builder add)
        {
            hasUpdate = true;
            count++;

            int addFlags = notAlreadySet(not(CLEANUP, add.flags), flags);
            if (addFlags == 0)
                return addCleanup(clearFields, add.cleanup);

            setNulls(false, addFlags);
            int iterable = toIterableSetFields(notNulls(addFlags));
            for (Field next = nextSetField(iterable) ; next != null; next = nextSetField(iterable = unsetIterable(next, iterable)))
            {
                switch (next)
                {
                    default: throw new UnhandledEnum(next);
                    case PARTICIPANTS:      participants = add.participants;               break;
                    case SAVE_STATUS:       saveStatus = add.saveStatus;                   break;
                    case DURABILITY:        durability = add.durability;                   break;
                    case EXECUTE_AT:        executeAt = add.executeAt;                     break;
                    case PROMISED:          promised = add.promised;                       break;
                    case ACCEPTED:          acceptedOrCommitted = add.acceptedOrCommitted; break;
                    case PARTIAL_TXN:       partialTxn = add.partialTxn;                   break;
                    case PARTIAL_DEPS:      partialDeps = add.partialDeps;                 break;
                    case WAITING_ON:        waitingOn = add.waitingOn;                     break;
                    case MIN_UNIQUE_HLC:    minUniqueHlc = add.minUniqueHlc;               break;
                    case EXECUTES_AT_LEAST: executesAtLeast = add.executesAtLeast;         break;
                    case WRITES:            writes = add.writes;                           break;
                    case RESULT:            result = add.result;                           break;
                }
            }
            flags |= addFlags;
            addCleanup(clearFields, add.cleanup);
            return true;
        }

        // returns true if we made a material update to the Builder;
        // that is, if we cleared a non-null field or if we are already mask-only
        public boolean clearSuperseded(boolean clearFields, Builder superseding)
        {
            int unset = flags & setFieldsMask(superseding.flags & ~setChanged(CLEANUP));
            if (notNulls(unset) == 0 && notNulls(flags) != 0)
                return false;

            if (clearFields)
                clearFields(unset);
            flags ^= unset;
            return true;
        }

        public boolean addCleanup(boolean clearFields, Cleanup addCleanup)
        {
            if (addCleanup == null || addCleanup == NO)
                return false;

            if (cleanup != null && addCleanup.compareTo(cleanup) <= 0)
            {
                Invariants.require(isChanged(CLEANUP, flags));
                return false;
            }

            hasUpdate = true;
            cleanup = addCleanup;
            flags |= setChanged(CLEANUP);
            return forceSetNulls(clearFields, eraseKnownFieldsMask[cleanup.newStatus.ordinal()]);
        }

        public boolean cleanup(boolean clearFields, Cleanup apply)
        {
            int unsetFields = eraseKnownFieldsMask[apply.newStatus.ordinal()];
            unsetFields &= flags;
            if (unsetFields == 0)
                return false;

            if (clearFields)
                clearFields(unsetFields);
            flags ^= unsetFields;
            return true;
        }

        protected static int nulls(int flags)
        {
            return (flags & 0xffff) | (flags << 16);
        }

        protected static int notNulls(int flags)
        {
            return flags & (~flags << 16);
        }

        protected static int not(Field field, int flags)
        {
            return flags & ~(0x10001 << field.ordinal());
        }

        protected static int notAlreadySet(int newFlags, int oldFlags)
        {
            return newFlags & ~setFieldsMask(oldFlags);
        }

        // result has both null and changed flag bits set for any changed field;
        // can be used to limit another flags to those that were set bu these flags, or if inverted to those not set by these flags
        protected static int setFieldsMask(int flags)
        {
            int mask = flags & 0xffff0000;
            return mask | (mask >>> 16);
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
            if (!hasUpdate)
                return null;

            Invariants.require(txnId != null);
            if (participants != null)
                participants = participants.filter(LOAD, redundantBefore, txnId, saveStatus != null && saveStatus.known.isExecuteAtKnown() ? executeAt : null);

            if (durability == null)
                durability = NotDurable;

            WaitingOn waitingOn = null;
            if (this.waitingOn != null)
                waitingOn = this.waitingOn.provide(txnId, partialDeps, executesAtLeast, minUniqueHlc);

            if (cleanup != null)
            {
                switch (cleanup)
                {
                    default: throw new UnhandledEnum(cleanup);
                    case NO: break;
                    case EXPUNGE: return null;
                    case ERASE: return Command.Truncated.erased(txnId);
                    case INVALIDATE: return Command.Truncated.invalidated(txnId, participants);
                    case VESTIGIAL: return Command.Truncated.vestigial(txnId, participants);
                    case TRUNCATE_WITH_OUTCOME:
                        if (saveStatus.compareTo(SaveStatus.TruncatedApplyWithOutcome) < 0)
                            saveStatus = SaveStatus.TruncatedApplyWithOutcome;
                        break;
                    case TRUNCATE:
                        if (saveStatus.compareTo(SaveStatus.TruncatedApply) < 0)
                            saveStatus = saveStatus.known.is(ApplyAtKnown) ? SaveStatus.TruncatedApply : SaveStatus.TruncatedUnapplied;
                        break;
                }
            }

            Invariants.require(saveStatus != null);
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
                case TruncatedApplyWithOutcome:
                case TruncatedApply:
                case TruncatedUnapplied:
                    return Command.Truncated.truncated(txnId, status, durability, participants, executeAt, partialDeps, writes, result, executesAtLeast);
                case Vestigial:
                    return vestigial(txnId, participants);
                case Erased:
                    // TODO (expected): why are we saving Durability here for erased commands?
                    return erased(txnId);
                case Invalidated:
                    return invalidated(txnId, participants);
            }
        }

        public String toString()
        {
            return toString("");
        }

        public String toString(String separator)
        {
            return "Builder {"
                   + "txnId=" + txnId
                   + safeToString(PARTICIPANTS, flags, separator, participants)
                   + safeToString(SAVE_STATUS, flags, separator, saveStatus)
                   + safeToString(DURABILITY, flags, separator, durability)
                   + safeToString(EXECUTE_AT, flags, separator, executeAt)
                   + safeToString(PROMISED, flags, separator, promised)
                   + safeToString(ACCEPTED, flags, separator, acceptedOrCommitted)
                   + safeToString(PARTIAL_TXN, flags, separator, partialTxn)
                   + safeToString(PARTIAL_DEPS, flags, separator, partialDeps)
                   + safeToString(WAITING_ON, flags, separator, waitingOn)
                   + safeToString(MIN_UNIQUE_HLC, flags, separator, minUniqueHlc)
                   + safeToString(EXECUTES_AT_LEAST, flags, separator, executesAtLeast)
                   + safeToString(WRITES, flags, separator, writes)
                   + safeToString(RESULT, flags, separator, result)
                   + safeToString(CLEANUP, flags, separator, cleanup)
                   + '}';
        }

        private static Object safeToString(Field field, int flags, String separator, Object obj)
        {
            if (!isChanged(field, flags))
                return "";

            return separator + field.name().toLowerCase() + '=' + safeToString(isNull(field, flags), obj);
        }

        private static Object safeToString(boolean isNull, Object obj)
        {
            if (isNull)
            {
                if (obj == null)
                    return "null";

                try
                {
                    return "null<" + obj + '>';
                }
                catch (Throwable t)
                {
                    return "null<err>";
                }
            }

            try
            {
                return obj.toString();
            }
            catch (Throwable t)
            {
                return "<err>";
            }
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
    public static int getFlags(@Nullable Command before, @Nonnull Command after)
    {
        int flags = 0;
        SaveStatus saveStatus = after.saveStatus();
        if (before == null)
        {
            flags |= addIdentityFlags(null, after.participants(), PARTICIPANTS);
            flags |= addIdentityFlags(null, saveStatus, SAVE_STATUS);
            flags |= addIdentityFlags(null, after.durability(), DURABILITY);
            flags |= addIdentityFlags(null, after.executeAt(), EXECUTE_AT);
            flags |= addIdentityFlags(null, after.promised(), PROMISED);
            flags |= addIdentityFlags(null, after.acceptedOrCommitted(), ACCEPTED);
            flags |= addIdentityFlags(null, after.partialTxn(), PARTIAL_TXN);
            flags |= addIdentityFlags(null, after.partialDeps(), PARTIAL_DEPS);
            if (after.waitingOn() != null)
            {
                flags |= setChanged(WAITING_ON, 0);
                flags |= addIdentityFlags(0, getMinUniqueHlc(after), MIN_UNIQUE_HLC);
            }
            flags |= addIdentityFlags(null, after.executesAtLeast(), EXECUTES_AT_LEAST);
            flags |= addIdentityFlags(null, after.writes(), WRITES);
            flags |= addIdentityFlags(null, after.result(), RESULT);
        }
        else
        {
            flags |= addEqualityFlags(before.participants(), after.participants(), PARTICIPANTS);
            flags |= addIdentityFlags(before.saveStatus(), saveStatus, SAVE_STATUS);
            flags |= addIdentityFlags(before.durability(), after.durability(), DURABILITY);
            flags |= addFlags(before.executeAt(), after.executeAt(), Timestamp::equalsStrict, EXECUTE_AT);
            flags |= addEqualityFlags(before.promised(), after.promised(), PROMISED);
            flags |= addEqualityFlags(before.acceptedOrCommitted(), after.acceptedOrCommitted(), ACCEPTED);
            flags |= addIdentityFlags(before.partialTxn(), after.partialTxn(), PARTIAL_TXN);
            flags |= addIdentityFlags(before.partialDeps(), after.partialDeps(), PARTIAL_DEPS);
            if (before.waitingOn() != after.waitingOn())
            {
                flags |= setChanged(WAITING_ON);
                flags |= addIdentityFlags(0, getMinUniqueHlc(before), getMinUniqueHlc(after), MIN_UNIQUE_HLC);
            }
            flags |= addEqualityFlags(before.executesAtLeast(), after.executesAtLeast(), EXECUTES_AT_LEAST);
            flags |= addIdentityFlags(before.writes(), after.writes(), WRITES);
            flags |= addIdentityFlags(before.result(), after.result(), RESULT);
        }

        // make sure we have enough information to decide whether to expunge timestamps (for unique ApplyAt HLC guarantees)
        if (saveStatus.known.is(ApplyAtKnown) && (before == null || !before.saveStatus().known.is(ApplyAtKnown)))
        {
            flags = setChanged(EXECUTE_AT, flags);
            flags = setChanged(PARTICIPANTS, flags);
            flags = setChanged(SAVE_STATUS, flags);
        }

        flags |= eraseKnownFieldsMask[saveStatus.ordinal()];
        if (saveStatus.compareTo(SaveStatus.Erased) >= 0 && (before == null || before.saveStatus() != saveStatus))
            flags |= setChanged(CLEANUP, flags);

        return flags;
    }

    private static int addIdentityFlags(Object l, Object r, Field field)
    {
        if (l == r) return 0;
        if (r == null) return setIsNullAndChanged(field);
        return setChanged(field);
    }

    private static <T> int addEqualityFlags(T l, T r, Field field)
    {
        return addFlags(l, r, Objects::equals, field);
    }

    private static <T> int addFlags(T l, T r, BiPredicate<T, T> equality, Field field)
    {
        if (l == r) return 0;
        if (r == null) return setIsNullAndChanged(field);
        if (l == null) return setChanged(field);
        if (equality.test(l, r)) return 0;
        return setChanged(field);
    }

    private static int addIdentityFlags(long l, long r, Field field)
    {
        if (l == r) return 0;
        return setChanged(field);
    }

    private static int addIdentityFlags(long treatAsNull, long l, long r, Field field)
    {
        if (l == r) return 0;
        if (r == treatAsNull) return setIsNullAndChanged(field);
        return setChanged(field);
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

    public static int setChanged(Field field)
    {
        return 0x10000 << field.ordinal();
    }

    public static int setIsNullAndChanged(Field field)
    {
        return 0x10001 << field.ordinal();
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
    public static boolean isNull(Field field, int flags)
    {
        return (flags & (1 << field.ordinal())) != 0;
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