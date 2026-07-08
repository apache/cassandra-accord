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

import javax.annotation.Nullable;

import accord.api.Journal;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.RedundantBefore;
import accord.local.RedundantStatus;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.Participants;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static accord.impl.AbstractReplayer.Replay.NONE;
import static accord.impl.AbstractReplayer.Replay.TO_BOTH;
import static accord.impl.AbstractReplayer.Replay.TO_COMMAND_STORE;
import static accord.impl.AbstractReplayer.Replay.TO_DATA_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static accord.local.RedundantStatus.Property.LOG_UNAVAILABLE;
import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.SaveStatus.PreApplied;
import static accord.primitives.SaveStatus.TruncatedApplyWithOutcome;
import static accord.primitives.Status.Applied;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.Functions.alwaysFalse;

public abstract class AbstractReplayer implements Journal.Replayer
{
    // TODO (required): NON_DURABLE does not properly account for things like pre-bootstrap
    public enum Mode { ALL, PART_NON_DURABLE, NON_DURABLE }
    public enum Replay
    {
        // warning: behaviour depends on bit pattern of ordinals, so change with care
        NONE, TO_COMMAND_STORE, TO_DATA_STORE, TO_BOTH;

        private static final Replay[] lookup = values();

        public boolean includes(Replay replay)
        {
            return (ordinal() & replay.ordinal()) == replay.ordinal();
        }
        public Replay atLeast(Replay that)
        {
            return lookup[ordinal() | that.ordinal()];
        }
        public Replay atMost(Replay that)
        {
            return lookup[ordinal() & that.ordinal()];
        }
    }

    public final RedundantBefore redundantBefore;
    public final Mode mode;
    public final TxnId minReplay;

    protected AbstractReplayer(CommandStore commandStore, Mode mode, @Nullable TxnId minReplay)
    {
        this.redundantBefore = commandStore.unsafeGetRedundantBefore();
        this.mode = mode;
        Invariants.require(redundantBefore.ranges(Objects::nonNull).containsAll(commandStore.unsafeGetRangesForEpoch().all()));
        if (mode != Mode.ALL)
            minReplay = redundantBefore.foldl((b, v) -> TxnId.nonNullOrMin(v, replayBound(b)), minReplay, alwaysFalse());
        this.minReplay = TxnId.noneIfNull(minReplay);
    }

    protected boolean maybeShouldReplay(TxnId txnId)
    {
        return txnId.compareTo(minReplay) >= 0;
    }

    protected Replay shouldReplay(TxnId txnId, StoreParticipants participants)
    {
        Participants<?> search = participants.route();
        if (search == null) search = participants.hasTouched();
        switch (mode)
        {
            default: throw new UnhandledEnum(mode);
            case ALL: return TO_BOTH;
            case NON_DURABLE: return redundantBefore.foldl(search, (b, v, id) -> v.atMost(replay(b, id)), TO_BOTH, txnId, alwaysFalse());
            case PART_NON_DURABLE: return redundantBefore.foldl(search, (b, v, id) -> v.atLeast(replay(b, id)), NONE, txnId, alwaysFalse());
        }
    }

    private static TxnId replayBound(RedundantBefore.Bounds bounds)
    {
        return TxnId.max(bounds.maxBound(LOG_UNAVAILABLE), bounds.maxBoundBoth(LOCALLY_DURABLE_TO_COMMAND_STORE, LOCALLY_DURABLE_TO_DATA_STORE));
    }

    private static Replay replay(RedundantBefore.Bounds bounds, TxnId txnId)
    {
        RedundantStatus status = bounds.get(txnId, null);
        if (status.any(LOG_UNAVAILABLE))
            return NONE;

        Replay replay = NONE;
        if (!status.all(LOCALLY_DURABLE_TO_COMMAND_STORE))
            replay = TO_COMMAND_STORE;
        if (!status.all(LOCALLY_DURABLE_TO_DATA_STORE))
            replay = replay.atLeast(TO_DATA_STORE);
        return replay;
    }

    protected void replay(SafeCommandStore safeStore, TxnId txnId, Replay replay)
    {
        SafeCommand safeCommand = safeStore.unsafeGet(txnId);
        {
            Command command = safeCommand.current();
            if (command.saveStatus().compareTo(SaveStatus.Stable) >= 0 && command.saveStatus().compareTo(PreApplied) <= 0)
            {
                if (replay.includes(TO_COMMAND_STORE))
                    Commands.maybeExecute(safeStore, safeCommand, command, false, true);
            }
            else if (command.saveStatus().compareTo(Applying) >= 0 && command.saveStatus().compareTo(TruncatedApplyWithOutcome) <= 0)
            {
                if (command.txnId().is(Write) && replay.includes(TO_DATA_STORE))
                {
                    Commands.applyChain(safeStore, command.asExecuted())
                            .begin(safeStore.agent());
                }
                else Invariants.expect(command.hasBeen(Applied), "%s is Applying but is not a Write transaction", txnId);
            }
        }
        if (replay.includes(Replay.TO_COMMAND_STORE))
        {
            safeCommand.update(safeStore, safeCommand.current(), true);
            safeStore.notifyListeners(safeCommand, null);
        }
    }
}
