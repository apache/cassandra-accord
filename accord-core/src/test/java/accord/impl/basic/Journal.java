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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import accord.api.Result;
import accord.impl.InMemoryCommandStore;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.CommonAttributes;
import accord.local.Node;
import accord.primitives.Known;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import org.agrona.collections.Long2ObjectHashMap;

import static accord.primitives.SaveStatus.Stable;
import static accord.primitives.Status.Invalidated;
import static accord.primitives.Status.Truncated;
import static accord.utils.Invariants.illegalState;

public class Journal
{
    private final Long2ObjectHashMap<NavigableMap<TxnId, List<Diff>>> diffsPerCommandStore = new Long2ObjectHashMap<>();

    private final Node.Id id;

    public Journal(Node.Id id)
    {
        this.id = id;
    }

    public void purge(IntFunction<CommandStore> storeSupplier)
    {
        for (Map.Entry<Long, NavigableMap<TxnId, List<Diff>>> e : diffsPerCommandStore.entrySet())
        {
            int commandStoreId = e.getKey().intValue();
            Map<TxnId, List<Diff>> localJournal = e.getValue();
            CommandStore store = storeSupplier.apply(commandStoreId);

            Map<TxnId, List<Diff>> updates = new HashMap<>();
            List<TxnId> removals = new ArrayList<>();
            for (Map.Entry<TxnId, List<Diff>> e2 : localJournal.entrySet())
            {
                TxnId txnId = e2.getKey();
                List<Diff> diffs = e2.getValue();
                Command command = reconstruct(diffs, Reconstruct.Last).get(0);
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
                        command = Commands.purge(command, command.participants(), cleanup);
                        Invariants.checkState(command.saveStatus() != SaveStatus.Uninitialised);
                        List<Diff> arr = new ArrayList<>();
                        arr.add(diff(null, command));
                        updates.put(txnId, arr);
                        break;
                    case ERASE:
                        removals.add(txnId);
                        break;
                }
            }

            for (TxnId removal : removals)
                localJournal.remove(removal);
            localJournal.putAll(updates);
        }
    }

    // TODO (required): this might be a good first approximation, but maybe we need to make a better distinction between
    // when we want to produce side-effects.
    private boolean loading = false;

    public void reconstructAll(InMemoryCommandStore.Loader loader, int commandStoreId)
    {
        Map<TxnId, List<Diff>> diffs = diffsPerCommandStore.get(commandStoreId);

        // Nothing to do here, journal is empty for this command store
        if (diffs == null)
            return;

        loading = true;
        try
        {
            List<Command> toApply = new ArrayList<>();
            for (TxnId txnId : diffs.keySet())
            {
                Command command = reconstruct(commandStoreId, txnId);
                if (command.saveStatus().compareTo(Stable) >= 0 && !command.hasBeen(Truncated))
                    toApply.add(command);
                loader.load(command);
            }

            toApply.sort(Comparator.comparing(Command::executeAt));
            for (Command command : toApply)
                loader.apply(command);
        }
        finally
        {
            loading = false;
        }
    }

    private enum Reconstruct
    {
        Each,
        Last
    }

    public Command reconstruct(int commandStoreId, TxnId txnId)
    {
        List<Diff> diffs = this.diffsPerCommandStore.get(commandStoreId).get(txnId);
        return reconstruct(diffs, Reconstruct.Last).get(0);
    }

    private List<Command> reconstruct(List<Diff> diffs, Reconstruct reconstruct)
    {
        Invariants.checkState(diffs != null && !diffs.isEmpty());

        List<Command> results = new ArrayList<>();

        TxnId txnId = null;
        Timestamp executeAt = null;
        Timestamp executesAtLeast = null;
        SaveStatus saveStatus = null;
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
                txnId = diff.txnId.get();
            if (diff.executeAt != null)
                executeAt = diff.executeAt.get();
            if (diff.executesAtLeast != null)
                executesAtLeast = diff.executesAtLeast.get();
            if (diff.saveStatus != null)
            {
                Set<SaveStatus> allowed = new HashSet<>();
                allowed.add(SaveStatus.TruncatedApply);
                allowed.add(SaveStatus.TruncatedApplyWithOutcome);

                saveStatus = diff.saveStatus.get();
            }
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
            Invariants.checkState(saveStatus != null,
                                  "Save status is null after applying %s", diffs);

            try
            {
                if (reconstruct == Reconstruct.Each ||
                    (reconstruct == Reconstruct.Last && i == diffs.size() - 1))
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

                    results.add(current);
                }
            }
            catch (Throwable t)
            {

                throw new RuntimeException("Can not reconstruct from diff:\n" + diffs.stream().map(o -> o.toString())
                                                                                     .collect(Collectors.joining("\n")),
                                           t);
            }
        }
        return results;
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

    public void onExecute(int commandStoreId, Command before, Command after, boolean isPrimary)
    {
        if (loading || (before == null && after == null))
            return;

        if (after.saveStatus() == SaveStatus.Erased)
        {
            diffsPerCommandStore.computeIfAbsent(commandStoreId, (k) -> new TreeMap<>())
                                .remove(after.txnId());
            return;
        }
        Diff diff = diff(before, after);
        if (!isPrimary)
            diff = diff.asNonPrimary();

        if (diff != null)
        {
            diffsPerCommandStore.computeIfAbsent(commandStoreId, (k) -> new TreeMap<>())
                                .computeIfAbsent(after.txnId(), (k_) -> new ArrayList<>())
                                .add(diff);
        }
    }

    private static class Diff
    {
        public final NewValue<TxnId> txnId;

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

        public Diff(NewValue<TxnId> txnId,
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

        // We allow only save status, and waitingOn to be updated by non-primary transactions
        public Diff asNonPrimary()
        {
            return new Diff(txnId, null, null, saveStatus, null, null, null, null, null, null, waitingOn, null, null);
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

        Diff diff = new Diff(ifNotEqual(before, after, Command::txnId, false),
                             ifNotEqual(before, after, Command::executeAt, true),
                             ifNotEqual(before, after, Command::executesAtLeast, true),
                             ifNotEqual(before, after, Command::saveStatus, false),

                             ifNotEqual(before, after, Command::durability, false),
                             ifNotEqual(before, after, Command::acceptedOrCommitted, false),
                             ifNotEqual(before, after, Command::promised, false),

                             ifNotEqual(before, after, Command::participants, true),
                             ifNotEqual(before, after, Command::partialTxn, false),
                             ifNotEqual(before, after, Command::partialDeps, false),
                             ifNotEqual(before, after, Journal::getWaitingOn, true),
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
    }

}