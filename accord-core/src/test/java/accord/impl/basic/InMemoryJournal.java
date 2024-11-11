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

package accord.impl.basic;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSortedMap;

import accord.api.Journal;
import accord.api.Result;
import accord.impl.InMemoryCommandStore;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Commands;
import accord.local.CommonAttributes;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Known;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import org.agrona.collections.Int2ObjectHashMap;

import static accord.primitives.SaveStatus.NotDefined;
import static accord.primitives.SaveStatus.Stable;
import static accord.primitives.Status.Invalidated;
import static accord.primitives.Status.Truncated;
import static accord.utils.Invariants.illegalState;

public class InMemoryJournal implements Journal
{
    private final Int2ObjectHashMap<NavigableMap<TxnId, List<Diff>>> diffsPerCommandStore = new Int2ObjectHashMap<>();
    private final FieldStates fieldStates;

    private static class FieldStates
    {
        RedundantBefore redundantBefore = RedundantBefore.EMPTY;
        NavigableMap<TxnId, Ranges> bootstrapBeganAt = ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY);
        NavigableMap<Timestamp, Ranges> safeToRead = ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY);
        CommandStores.RangesForEpoch.Snapshot rangesForEpoch = null;
    }

    private final Node.Id id;

    public InMemoryJournal(Node.Id id)
    {
        this.id = id;
        this.fieldStates = new FieldStates();
    }

    @Override
    public Command loadCommand(int commandStoreId, TxnId txnId,
                               // TODO: currently unused!
                               RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        NavigableMap<TxnId, List<Diff>> commandStore = this.diffsPerCommandStore.get(commandStoreId);

        if (commandStore == null)
            return null;

        List<Diff> saved = this.diffsPerCommandStore.get(commandStoreId).get(txnId);
        if (saved == null)
            return null;

        return reconstruct(saved);
    }

    @Override
    public void saveCommand(int store, CommandUpdate diff, Runnable onFlush)
    {
        if (diff == null)
            return;

        diffsPerCommandStore.computeIfAbsent(store, (k) -> new TreeMap<>())
                            .computeIfAbsent(diff.txnId, (k_) -> new ArrayList<>())
                            .add(diff(diff.before, diff.after));

        if (onFlush!= null)
            onFlush.run();
    }

    @Override
    public RedundantBefore loadRedundantBefore(int commandStoreId)
    {
        return fieldStates.redundantBefore;
    }

    @Override
    public NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int commandStoreId)
    {
        return fieldStates.bootstrapBeganAt;
    }

    @Override
    public NavigableMap<Timestamp, Ranges> loadSafeToRead(int commandStoreId)
    {
        return fieldStates.safeToRead;
    }

    @Override
    public CommandStores.RangesForEpoch.Snapshot loadRangesForEpoch(int commandStoreId)
    {
        return fieldStates.rangesForEpoch;
    }

    public void saveStoreState(int store, FieldUpdates fieldUpdates, Runnable onFlush)
    {
        if (fieldUpdates.newRedundantBefore != null)
            fieldStates.redundantBefore = fieldUpdates.newRedundantBefore;
        if (fieldUpdates.newSafeToRead != null)
            fieldStates.safeToRead = fieldUpdates.newSafeToRead;
        if (fieldUpdates.newBootstrapBeganAt != null)
            fieldStates.bootstrapBeganAt = fieldUpdates.newBootstrapBeganAt;
        if (fieldUpdates.newRangesForEpoch != null)
            fieldStates.rangesForEpoch = fieldUpdates.newRangesForEpoch;

        if (onFlush!= null)
            onFlush.run();
    }

    @Override
    public void purge(CommandStores commandStores)
    {
        for (Map.Entry<Integer, NavigableMap<TxnId, List<Diff>>> e : diffsPerCommandStore.entrySet())
        {
            int commandStoreId = e.getKey();
            Map<TxnId, List<Diff>> localJournal = e.getValue();
            CommandStore store = commandStores.forId(commandStoreId);
            if (store == null)
                continue;

            for (Map.Entry<TxnId, List<Diff>> e2 : localJournal.entrySet())
            {
                List<Diff> diffs = e2.getValue();
                if (diffs.isEmpty()) continue;
                Command command =  reconstruct(diffs);
                if (command.status() == Truncated || command.status() == Invalidated)
                    continue; // Already truncated
                Cleanup cleanup = Cleanup.shouldCleanup(store.agent(), command, store.unsafeGetRedundantBefore(), store.durableBefore());
                switch (cleanup)
                {
                    case NO:
                        break;
                    case INVALIDATE:
                    case TRUNCATE_WITH_OUTCOME:
                    case TRUNCATE:
                    case ERASE:
                        command = Commands.purge(command, command.participants(), cleanup);
                        Invariants.checkState(command.saveStatus() != SaveStatus.Uninitialised);
                        Diff diff = diff(null, command);
                        e2.setValue(cleanup == Cleanup.ERASE ? new ErasedList(diff) : new TruncatedList(diff));
                        break;

                    case EXPUNGE:
                        e2.setValue(new PurgedList());
                        break;
                }
            }
        }
    }

    @Override
    public void replay(CommandStores commandStores)
    {
        OnDone sync = new OnDone() {
            public void success() {}
            public void failure(Throwable t) { throw new RuntimeException("Caught an exception during replay", t); }
        };

        for (Map.Entry<Integer, NavigableMap<TxnId, List<Diff>>> diffEntry : diffsPerCommandStore.entrySet())
        {
            int commandStoreId = diffEntry.getKey();
            // copy to avoid concurrent modification when appending to journal
            Map<TxnId, List<Diff>> diffs = new TreeMap<>(diffEntry.getValue());

            InMemoryCommandStore commandStore = (InMemoryCommandStore) commandStores.forId(commandStoreId);
            Loader loader = commandStore.loader();

            for (Map.Entry<TxnId, List<Diff>> e : diffs.entrySet())
                e.setValue(new ArrayList<>(e.getValue()));

            for (Map.Entry<TxnId, List<Diff>> e : diffs.entrySet())
            {
                if (e.getValue().isEmpty()) continue;
                Command command =  reconstruct(e.getValue());
                loader.load(command, sync);
                if (command.saveStatus().compareTo(Stable) >= 0 && !command.hasBeen(Truncated))
                    loader.apply(command, sync);
            }
        }
    }

    static class ErasedList extends AbstractList<Diff>
    {
        final Diff erased;

        ErasedList(Diff erased)
        {
            Invariants.checkArgument(erased.saveStatus.value == SaveStatus.Erased);
            this.erased = erased;
        }

        @Override
        public Diff get(int index)
        {
            if (index != 0)
                throw new IndexOutOfBoundsException();
            return erased;
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public boolean add(Diff diff)
        {
            if (diff.saveStatus != null && diff.saveStatus.value == SaveStatus.Erased)
                return false;
            throw illegalState();
        }
    }

    static class TruncatedList extends ArrayList<Diff>
    {
        TruncatedList(Diff truncated)
        {
            add(truncated);
        }
    }

    static class PurgedList extends AbstractList<Diff>
    {
        PurgedList()
        {
        }

        @Override
        public Diff get(int index)
        {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public boolean add(Diff diff)
        {
            if (diff.saveStatus != null && diff.saveStatus.value == SaveStatus.Erased)
                return false;
            throw illegalState();
        }
    }

    private Command reconstruct(List<Diff> diffs)
    {
        Invariants.checkState(diffs != null && !diffs.isEmpty());

        TxnId txnId = null;
        Timestamp executeAt = null;
        Timestamp executesAtLeast = null;
        SaveStatus saveStatus = NotDefined;
        Status.Durability durability = Status.Durability.NotDurable;

        Ballot acceptedOrCommitted = Ballot.ZERO;
        Ballot promised = Ballot.ZERO;

        StoreParticipants participants = null;
        PartialTxn partialTxn = null;
        PartialDeps partialDeps = null;

        Command.WaitingOn waitingOn = null;
        Writes writes = null;
        Result result = null;

        for (int i = 0; i < diffs.size(); i++)
        {
            Diff diff = diffs.get(i);
            if (diff.txnId != null)
                txnId = diff.txnId;
            if (diff.executeAt != null)
                executeAt = diff.executeAt.get();
            if (diff.executesAtLeast != null)
                executesAtLeast = diff.executesAtLeast.get();
            if (diff.saveStatus != null)
                saveStatus = diff.saveStatus.get();
            if (diff.durability != null)
                durability = diff.durability.get();

            if (diff.acceptedOrCommitted != null)
                acceptedOrCommitted = diff.acceptedOrCommitted.get();
            if (diff.promised != null)
                promised = diff.promised.get();

            if (diff.participants != null)
                participants = diff.participants.get();
            if (diff.partialTxn != null)
                partialTxn = diff.partialTxn.get();
            if (diff.partialDeps != null)
                partialDeps = diff.partialDeps.get();

            if (diff.waitingOn != null)
                waitingOn = diff.waitingOn.get();
            if (diff.writes != null)
                writes = diff.writes.get();
            if (diff.result != null)
                result = diff.result.get();

            try
            {
                if (!txnId.kind().awaitsOnlyDeps())
                    executesAtLeast = null;
            }
            catch (Throwable t)
            {
                t.printStackTrace();
            }

            switch (saveStatus.known.outcome)
            {
                case Erased:
                case WasApply:
                    writes = null;
                    result = null;
                    break;
            }
        }

        CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
        if (partialTxn != null)
            attrs.partialTxn(partialTxn);
        if (durability != null)
            attrs.durability(durability);
        if (participants != null) attrs.setParticipants(participants);
        else attrs.setParticipants(StoreParticipants.empty(txnId));

        // TODO (desired): we can simplify this logic if, instead of diffing, we will infer the diff from the status
        if (partialDeps != null &&
            (saveStatus.known.deps != Known.KnownDeps.NoDeps &&
             saveStatus.known.deps != Known.KnownDeps.DepsErased &&
             saveStatus.known.deps != Known.KnownDeps.DepsUnknown))
            attrs.partialDeps(partialDeps);

        try
        {

            Command current;
            switch (saveStatus.status)
            {
                case NotDefined:
                    current = saveStatus == SaveStatus.Uninitialised ? Command.NotDefined.uninitialised(attrs.txnId())
                                                                     : Command.NotDefined.notDefined(attrs, promised);
                    break;
                case PreAccepted:
                    current = Command.PreAccepted.preAccepted(attrs, executeAt, promised);
                    break;
                case AcceptedInvalidate:
                case Accepted:
                case PreCommitted:
                    if (saveStatus == SaveStatus.AcceptedInvalidate)
                        current = Command.AcceptedInvalidateWithoutDefinition.acceptedInvalidate(attrs, promised, acceptedOrCommitted);
                    else
                        current = Command.Accepted.accepted(attrs, saveStatus, executeAt, promised, acceptedOrCommitted);
                    break;
                case Committed:
                case Stable:
                    current = Command.Committed.committed(attrs, saveStatus, executeAt, promised, acceptedOrCommitted, waitingOn);
                    break;
                case PreApplied:
                case Applied:
                    current = Command.Executed.executed(attrs, saveStatus, executeAt, promised, acceptedOrCommitted, waitingOn, writes, result);
                    break;
                case Invalidated:
                case Truncated:
                    current = truncated(attrs, saveStatus, executeAt, executesAtLeast, writes, result);
                    break;
                default:
                    throw new IllegalStateException("Do not know " + saveStatus.status + " " + saveStatus);
            }

            return current;
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Can not reconstruct from diff:\n" + diffs.stream().map(o -> o.toString())
                                                                                 .collect(Collectors.joining("\n")),
                                       t);
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
                return Command.Truncated.truncatedApply(attrs, status, executeAt, writes, result, executesAtLeast);
            case ErasedOrVestigial:
                return Command.Truncated.erasedOrInvalidOrVestigial(attrs.txnId(), attrs.durability(), attrs.participants());
            case Erased:
                return Command.Truncated.erased(attrs.txnId(), attrs.durability(), attrs.participants());
            case Invalidated:
                return Command.Truncated.invalidated(attrs.txnId());
        }
    }

    private static class Diff
    {
        public final TxnId txnId;

        public final NewValue<Timestamp> executeAt;
        public final NewValue<Timestamp> executesAtLeast;
        public final NewValue<SaveStatus> saveStatus;
        public final NewValue<Status.Durability> durability;

        public final NewValue<Ballot> acceptedOrCommitted;
        public final NewValue<Ballot> promised;

        public final NewValue<StoreParticipants> participants;
        public final NewValue<PartialTxn> partialTxn;
        public final NewValue<PartialDeps> partialDeps;

        public final NewValue<Writes> writes;
        public final NewValue<Command.WaitingOn> waitingOn;

        public final NewValue<Result> result; // temporarily here for sakes for reloads

        public Diff(TxnId txnId,
                    NewValue<Timestamp> executeAt,
                    NewValue<Timestamp> executesAtLeast,
                    NewValue<SaveStatus> saveStatus,
                    NewValue<Status.Durability> durability,

                    NewValue<Ballot> acceptedOrCommitted,
                    NewValue<Ballot> promised,

                    NewValue<StoreParticipants> participants,
                    NewValue<PartialTxn> partialTxn,
                    NewValue<PartialDeps> partialDeps,
                    NewValue<Command.WaitingOn> waitingOn,

                    NewValue<Writes> writes,
                    NewValue<Result> result)
        {
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.executesAtLeast = executesAtLeast;
            this.saveStatus = saveStatus;
            this.durability = durability;

            this.acceptedOrCommitted = acceptedOrCommitted;
            this.promised = promised;

            this.participants = participants;
            this.partialTxn = partialTxn;
            this.partialDeps = partialDeps;

            this.writes = writes;
            this.waitingOn = waitingOn;
            this.result = result;
        }

        public boolean allNulls()
        {
            if (txnId != null) return false;
            if (executeAt != null) return false;
            if (executesAtLeast != null) return false;
            if (saveStatus != null) return false;
            if (durability != null) return false;
            if (acceptedOrCommitted != null) return false;
            if (promised != null) return false;
            if (participants != null) return false;
            if (partialTxn != null) return false;
            if (partialDeps != null) return false;
            if (writes != null) return false;
            if (waitingOn != null) return false;
            if (result != null) return false;
            return true;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder("Diff{");
            if (txnId != null)
                builder.append("txnId = ").append(txnId).append(" ");
            if (executeAt != null)
                builder.append("executeAt = ").append(executeAt).append(" ");
            if (executesAtLeast != null)
                builder.append("executesAtLeast = ").append(executesAtLeast).append(" ");
            if (saveStatus != null)
                builder.append("saveStatus = ").append(saveStatus).append(" ");
            if (durability != null)
                builder.append("durability = ").append(durability).append(" ");
            if (acceptedOrCommitted != null)
                builder.append("acceptedOrCommitted = ").append(acceptedOrCommitted).append(" ");
            if (promised != null)
                builder.append("promised = ").append(promised).append(" ");
            if (participants != null)
                builder.append("participants = ").append(participants).append(" ");
            if (partialTxn != null)
                builder.append("partialTxn = ").append(partialTxn).append(" ");
            if (partialDeps != null)
                builder.append("partialDeps = ").append(partialDeps).append(" ");
            if (writes != null)
                builder.append("writes = ").append(writes).append(" ");
            if (waitingOn != null)
                builder.append("waitingOn = ").append(waitingOn).append(" ");
            if (result != null)
                builder.append("result = ").append(result).append(" ");
            builder.append("}");
            return builder.toString();
        }
    }

    static Diff diff(Command before, Command after)
    {
        if (Objects.equals(before, after))
            return null;

        Diff diff = new Diff(after.txnId(),
                             ifNotEqual(before, after, Command::executeAt, true),
                             ifNotEqual(before, after, Command::executesAtLeast, true),
                             ifNotEqual(before, after, Command::saveStatus, false),

                             ifNotEqual(before, after, Command::durability, false),
                             ifNotEqual(before, after, Command::acceptedOrCommitted, false),
                             ifNotEqual(before, after, Command::promised, false),

                             ifNotEqual(before, after, Command::participants, true),
                             ifNotEqual(before, after, Command::partialTxn, false),
                             ifNotEqual(before, after, Command::partialDeps, false),
                             ifNotEqual(before, after, InMemoryJournal::getWaitingOn, true),
                             ifNotEqual(before, after, Command::writes, false),
                             ifNotEqual(before, after, Command::result, false));
        if (diff.allNulls())
            return null;

        return diff;
    }

    static Command.WaitingOn getWaitingOn(Command command)
    {
        if (command instanceof Command.Committed)
            return command.asCommitted().waitingOn();

        return null;
    }

    private static <OBJ, VAL> NewValue<VAL> ifNotEqual(OBJ lo, OBJ ro, Function<OBJ, VAL> convert, boolean allowClassMismatch)
    {
        VAL l = null;
        VAL r = null;
        if (lo != null) l = convert.apply(lo);
        if (ro != null) r = convert.apply(ro);

        if (l == r)
            return null; // null here means there was no change

        if (l == null || r == null)
            return NewValue.of(r);

        assert allowClassMismatch || l.getClass() == r.getClass() : String.format("%s != %s", l.getClass(), r.getClass());

        if (l.equals(r))
            return null;

        return NewValue.of(r);
    }

    private static class NewValue<T>
    {
        final T value;

        private NewValue(T value)
        {
            this.value = value;
        }

        public T get()
        {
            return value;
        }

        public static <T> NewValue<T> of(T value)
        {
            return new NewValue<>(value);
        }

        public String toString()
        {
            return "" + value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NewValue<?> newValue = (NewValue<?>) o;
            return Objects.equals(value, newValue.value);
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }
    }
}