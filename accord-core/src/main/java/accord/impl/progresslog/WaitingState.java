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

package accord.impl.progresslog;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.AsyncExecutor;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.Tracing;
import accord.coordinate.AsynchronousAwait;
import accord.coordinate.FetchData;
import accord.coordinate.FetchRoute;
import accord.coordinate.Infer;
import accord.local.Command;
import accord.local.CommandStores.IncludingSpecificStoreSelector;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.messages.Await;
import accord.primitives.Known;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.local.StoreParticipants;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.api.ProgressLog.BlockedUntil.HasDecidedExecuteAt;
import static accord.api.ProgressLog.BlockedUntil.Query.HOME;
import static accord.api.ProgressLog.BlockedUntil.Query.SHARD;
import static accord.coordinate.Coordination.CoordinationKind.WaitProgress;
import static accord.impl.progresslog.CallbackInvoker.invokeWaitingCallback;
import static accord.impl.progresslog.PackedKeyTracker.bitSet;
import static accord.impl.progresslog.PackedKeyTracker.clearRoundState;
import static accord.impl.progresslog.PackedKeyTracker.initialiseBitSet;
import static accord.impl.progresslog.PackedKeyTracker.roundCallbackBitSet;
import static accord.impl.progresslog.PackedKeyTracker.roundIndex;
import static accord.impl.progresslog.PackedKeyTracker.roundSize;
import static accord.impl.progresslog.PackedKeyTracker.setBitSet;
import static accord.impl.progresslog.PackedKeyTracker.setMaxRoundIndexAndClearBitSet;
import static accord.impl.progresslog.PackedKeyTracker.setRoundIndexAndClearBitSet;
import static accord.impl.progresslog.Progress.Awaiting;
import static accord.impl.progresslog.Progress.NoneExpected;
import static accord.impl.progresslog.Progress.Querying;
import static accord.impl.progresslog.Progress.Queued;
import static accord.impl.progresslog.TxnStateKind.Waiting;
import static accord.impl.progresslog.WaitingState.CallbackKind.AwaitHome;
import static accord.impl.progresslog.WaitingState.CallbackKind.AwaitSlice;
import static accord.impl.progresslog.WaitingState.CallbackKind.Fetch;
import static accord.local.SafeCommand.maxParticipants;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtErased;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;

/**
 * This represents a simple state machine encoded in a small number of bits for efficiently gathering
 * distributed state we require locally to make progress.
 * <p>
 * The state machine consists of the following packed registers:
 *  - target BlockedUntil
 *  - The BlockUntil we know at least one home shard replica is able to satisfy
 *  - A packed bitset/counter for enumerating the relevant keys and awaiting
 *    remote replicas for the keys to be ready to satisfy our local requirements
 *
 */
@SuppressWarnings("CodeBlock2Expr")
abstract class WaitingState extends BaseTxnState
{
    private static final int PROGRESS_SHIFT = 0;
    private static final long PROGRESS_MASK = 0x3;
    private static final int BLOCKED_UNTIL_SHIFT = 2;
    private static final long BLOCKED_UNTIL_MASK = 0x3;
    private static final int QUERYING_SHIFT = 4;
    private static final long QUERYING_MASK = 0x3;
    private static final int HOME_SATISFIES_SHIFT = 6;
    private static final long HOME_SATISFIES_MASK = 0x3;
    private static final int QUERY_SHARDS_NOT_HOME_SHIFT = HOME_SATISFIES_SHIFT + Long.bitCount(HOME_SATISFIES_MASK);
    private static final long QUERY_SHARDS_NOT_HOME_BIT = 1L << QUERY_SHARDS_NOT_HOME_SHIFT;
    private static final int AWAIT_STARTED_SHIFT = QUERY_SHARDS_NOT_HOME_SHIFT + 1;
    private static final int AWAIT_STARTED_BIT = 1 << AWAIT_STARTED_SHIFT;
    private static final int AWAIT_SHIFT = AWAIT_STARTED_SHIFT + 1;
    private static final int AWAIT_BITS = 26;
    private static final long AWAIT_MASK = (1L << AWAIT_BITS) - 1;
    private static final int AWAIT_EPOCH_SHIFT = AWAIT_SHIFT + AWAIT_BITS;
    private static final int AWAIT_EPOCH_BITS = 4;
    private static final long SET_MASK = ~((PROGRESS_MASK << PROGRESS_SHIFT) | (QUERYING_MASK << QUERYING_SHIFT));
    private static final long INITIALISED_MASK = (PROGRESS_MASK << PROGRESS_SHIFT) | (BLOCKED_UNTIL_MASK << BLOCKED_UNTIL_SHIFT) | (HOME_SATISFIES_MASK << HOME_SATISFIES_SHIFT);

    private static final int RETRY_COUNTER_SHIFT = AWAIT_EPOCH_SHIFT + AWAIT_EPOCH_BITS;
    private static final long RETRY_COUNTER_MASK = 0x7;
    static final int WAITING_STATE_END_SHIFT = RETRY_COUNTER_SHIFT + 3;
    static
    {
        Invariants.require(BLOCKED_UNTIL_SHIFT == PROGRESS_SHIFT + Long.bitCount(PROGRESS_MASK));
        Invariants.require(QUERYING_SHIFT == BLOCKED_UNTIL_SHIFT + Long.bitCount(BLOCKED_UNTIL_MASK));
        Invariants.require(HOME_SATISFIES_SHIFT == QUERYING_SHIFT + Long.bitCount(QUERYING_MASK));
        Invariants.require(QUERY_SHARDS_NOT_HOME_SHIFT == HOME_SATISFIES_SHIFT + Long.bitCount(HOME_SATISFIES_MASK));
        Invariants.require(AWAIT_STARTED_SHIFT == 1 + QUERY_SHARDS_NOT_HOME_SHIFT);
    }

    // when awaiting shards we register callbacks numbered by the keys we're processing;
    // we want to special-case the home key callback and its easiest to pick the highest integer
    // so that we know it won't clash
    public static final int AWAITING_HOME_KEY_CALLBACKID = Integer.MAX_VALUE;

    WaitingState(TxnId txnId)
    {
        super(txnId);
    }

    private void set(@Nullable SafeCommandStore safeStore, DefaultProgressLog owner, BlockedUntil newQuerying, Progress newProgress)
    {
        encodedState &= SET_MASK;
        encodedState |= ((long) newQuerying.ordinal() << QUERYING_SHIFT) | ((long) newProgress.ordinal() << PROGRESS_SHIFT);
        updateScheduling(safeStore, owner, Waiting, newQuerying, newProgress);
    }

    private void setBlockedUntil(BlockedUntil blockedUntil)
    {
        encodedState &= ~(BLOCKED_UNTIL_MASK << BLOCKED_UNTIL_SHIFT);
        encodedState |= (long) blockedUntil.ordinal() << BLOCKED_UNTIL_SHIFT;
    }

    private void setHomeSatisfies(BlockedUntil homeStatus)
    {
        encodedState &= ~(HOME_SATISFIES_MASK << HOME_SATISFIES_SHIFT);
        encodedState |= (long) homeStatus.ordinal() << HOME_SATISFIES_SHIFT;
    }

    boolean isUninitialised()
    {
        return 0 == (encodedState & INITIALISED_MASK);
    }

    @Nonnull BlockedUntil blockedUntil()
    {
        return blockedUntil(encodedState);
    }

    @Nonnull BlockedUntil querying()
    {
        return querying(encodedState);
    }

    @Nonnull BlockedUntil homeSatisfies()
    {
        return homeSatisfies(encodedState);
    }

    final @Nonnull Progress waitingProgress()
    {
        return waitingProgress(encodedState);
    }

    final long waitingKeyTrackerBits()
    {
        return (encodedState >>> AWAIT_SHIFT) & (-1L >>> (64 - AWAIT_BITS));
    }

    private static @Nonnull BlockedUntil blockedUntil(long encodedState)
    {
        return BlockedUntil.forOrdinal((int) ((encodedState >>> BLOCKED_UNTIL_SHIFT) & BLOCKED_UNTIL_MASK));
    }

    private static @Nonnull BlockedUntil querying(long encodedState)
    {
        return BlockedUntil.forOrdinal((int) ((encodedState >>> QUERYING_SHIFT) & QUERYING_MASK));
    }

    private static @Nonnull BlockedUntil homeSatisfies(long encodedState)
    {
        return BlockedUntil.forOrdinal((int) ((encodedState >>> HOME_SATISFIES_SHIFT) & HOME_SATISFIES_MASK));
    }

    private static @Nonnull Progress waitingProgress(long encodedState)
    {
        return Progress.forOrdinal((int) ((encodedState >>> PROGRESS_SHIFT) & PROGRESS_MASK));
    }

    private static int awaitRoundSize(Route<?> slicedRoute)
    {
        // TODO (required): for testing, introduce some deterministic mechanism for picking some value smaller than we can support,
        //  so we can better exercise this without breaking the infrequent much larger routes for sync points
        return roundSize(slicedRoute.size(), AWAIT_BITS);
    }

    private void clearAwaitState()
    {
        encodedState &= ~AWAIT_STARTED_BIT;
        encodedState = clearRoundState(encodedState, AWAIT_SHIFT, AWAIT_MASK);
    }

    private int awaitBitSet(int roundSize)
    {
        return bitSet(encodedState, roundSize, AWAIT_SHIFT);
    }

    private void initialiseAwaitBitSet(Route<?> route, Unseekables<?> notReady, int roundIndex, int roundSize)
    {
        encodedState = initialiseBitSet(encodedState, route, notReady, roundIndex, roundSize, AWAIT_SHIFT);
    }

    private void setAwaitBitSet(int bitSet, int roundSize)
    {
        encodedState = setBitSet(encodedState, bitSet, roundSize, AWAIT_SHIFT);
    }

    private boolean hasAwaitStarted()
    {
        return 0 != (encodedState & AWAIT_STARTED_BIT);
    }

    private void setAwaitStarted()
    {
        encodedState |= AWAIT_STARTED_BIT;
    }

    private int awaitRoundIndex(int roundSize)
    {
        return roundIndex(encodedState, roundSize, AWAIT_SHIFT, AWAIT_MASK);
    }

    private void updateAwaitRound(int newRoundIndex, int roundSize)
    {
        Invariants.requireArgument(roundSize <= AWAIT_BITS);
        encodedState = setRoundIndexAndClearBitSet(encodedState, newRoundIndex, roundSize, AWAIT_SHIFT, AWAIT_BITS, AWAIT_MASK);
    }

    private void setAwaitDone(int roundSize)
    {
        Invariants.requireArgument(roundSize <= AWAIT_BITS);
        encodedState = setMaxRoundIndexAndClearBitSet(encodedState, roundSize, AWAIT_SHIFT, AWAIT_BITS, AWAIT_MASK);
    }

    final int waitingRunCounter()
    {
        return (int) ((encodedState >>> RETRY_COUNTER_SHIFT) & RETRY_COUNTER_MASK);
    }

    final void incrementWaitingRunCounter()
    {
        long shiftedMask = RETRY_COUNTER_MASK << RETRY_COUNTER_SHIFT;
        long current = encodedState & shiftedMask;
        long updated = Math.min(shiftedMask, current + (1L << RETRY_COUNTER_SHIFT));
        encodedState &= ~shiftedMask;
        encodedState |= updated;
    }

    final void clearWaitingRunCounter()
    {
        long shiftedMask = RETRY_COUNTER_MASK << RETRY_COUNTER_SHIFT;
        encodedState &= ~shiftedMask;
    }

    Topologies contact(DefaultProgressLog owner, Unseekables<?> forKeys, long epoch)
    {
        Node node = owner.node();
        Topologies topologies = node.topology().forEpoch(forKeys, epoch, SHARE);
        return node.agent().selectPreferred(node.id(), topologies);
    }

    boolean queryShardsNotHome()
    {
        return 0 != (encodedState & QUERY_SHARDS_NOT_HOME_BIT);
    }

    void markHomeShardErased()
    {
        setHomeSatisfies(CanApply);
        encodedState |= QUERY_SHARDS_NOT_HOME_BIT;
    }

    /*
     * Ranges may have moved between command stores locally so extend to a later epoch to invoke those command stores
     */
    long updateLowEpoch(SafeCommandStore safeStore, TxnId txnId, Command command)
    {
        long lowEpoch = computeLowEpoch(safeStore, txnId, command);
        int offset = safeStore.ranges().indexOffset(lowEpoch, txnId.epoch());
        if (offset >= 3)
        {
            offset = 3;
            lowEpoch = safeStore.ranges().latestEarlierEpochThatFullyCovers(lowEpoch, command.maxParticipants());
        }
        encodedState = encodedState & ~(0x3L << AWAIT_EPOCH_SHIFT);
        encodedState |= ((long)offset) << AWAIT_EPOCH_SHIFT;
        return lowEpoch;
    }

    static long computeLowEpoch(SafeCommandStore safeStore, TxnId txnId, Command command)
    {
        return StoreParticipants.computeFetchLowEpoch(safeStore, txnId, command);
    }

    long readLowEpoch(SafeCommandStore safeStore, TxnId txnId, Route<?> route)
    {
        int offset = (int) ((encodedState >>> AWAIT_EPOCH_SHIFT) & 0x3);
        if (offset == 0)
            return txnId.epoch();

        RangesForEpoch ranges = safeStore.ranges();
        int i = ranges.floorIndex(txnId.epoch()) - (offset - 1);
        long epoch = ranges.epochAtIndex(Math.max(0, i)) - 1;
        if (offset < 3)
            return epoch;
        return safeStore.ranges().latestEarlierEpochThatFullyCovers(epoch, route);
    }

    boolean hasNewLowEpoch(SafeCommandStore safeStore, TxnId txnId, long prevLowEpoch, long newLowEpoch)
    {
        if (prevLowEpoch == newLowEpoch)
            return false;
        RangesForEpoch ranges = safeStore.ranges();
        int prevOffset = Math.min(3, ranges.indexOffset(prevLowEpoch, txnId.epoch()));
        int newOffset = Math.min(3, ranges.indexOffset(newLowEpoch, txnId.epoch()));
        return prevOffset != newOffset;
    }

    long updateHighEpoch(SafeCommandStore safeStore, TxnId txnId, BlockedUntil blockedUntil, Command command, Timestamp executeAt)
    {
        long highEpoch = computeHighEpoch(safeStore, txnId, blockedUntil, command, executeAt);
        int offset = safeStore.ranges().indexOffset(txnId.epoch(), highEpoch);
        if (offset >= 3)
        {
            offset = 3;
            highEpoch = safeStore.ranges().earliestLaterEpochThatFullyCovers(highEpoch, command.maxParticipants());
        }
        encodedState = encodedState & ~(0xCL << AWAIT_EPOCH_SHIFT);
        encodedState |= ((long)offset) << (AWAIT_EPOCH_SHIFT + 2);
        return highEpoch;
    }

    static long computeHighEpoch(SafeCommandStore safeStore, TxnId txnId, BlockedUntil blockedUntil, Command command, Timestamp executeAt)
    {
        long epoch = blockedUntil.fetchEpoch(txnId, executeAt);
        return Math.max(epoch, safeStore.ranges().earliestLaterEpochThatFullyCovers(epoch, command.participants().hasTouched()));
    }

    long readHighEpoch(SafeCommandStore safeStore, TxnId txnId, Route<?> route)
    {
        RangesForEpoch ranges = safeStore.ranges();
        int offset = (int) ((encodedState >>> (AWAIT_EPOCH_SHIFT + 2)) & 0x3);
        if (offset == 0)
            return Math.max(txnId.epoch(), ranges.epochAtIndex(0));
        long epoch = ranges.epochAtIndex(Math.max(0, ranges.floorIndex(txnId.epoch())) + offset);
        if (offset < 3)
            return epoch;
        return safeStore.ranges().earliestLaterEpochThatFullyCovers(epoch, route);
    }

    boolean hasNewHighEpoch(SafeCommandStore safeStore, TxnId txnId, long prevHighEpoch, long newHighEpoch)
    {
        if (prevHighEpoch == newHighEpoch)
            return false;
        RangesForEpoch ranges = safeStore.ranges();
        int prevOffset = Math.min(3, ranges.indexOffset(txnId.epoch(), prevHighEpoch));
        int newOffset = Math.min(3, ranges.indexOffset(txnId.epoch(), newHighEpoch));
        return prevOffset != newOffset;
    }

    private Route<?> slicedRoute(SafeCommandStore safeStore, TxnId txnId, Route<?> route, long fromLocalEpoch, long toLocalEpoch)
    {
        Route<?> result = StoreParticipants.touches(safeStore, fromLocalEpoch, txnId, toLocalEpoch, route);
        if (result.isEmpty()) // if home shard is erased and we don't touch anything for the epochs, just contact everyone
            result = queryShardsNotHome() ? route : result.homeKeyOnlyRoute();
        return result;
    }

    private Route<?> awaitRoute(Route<?> slicedRoute, BlockedUntil blockedUntil)
    {
        return blockedUntil.waitsOn == HOME && !queryShardsNotHome() ? slicedRoute.homeKeyOnlyRoute() : slicedRoute;
    }

    private Route<?> fetchRoute(Route<?> slicedRoute, Route<?> awaitRoute, BlockedUntil blockedUntil, SafeCommandStore safeStore, long lowEpoch, TxnId txnId, long highEpoch, Route<?> route)
    {
        if (lowEpoch < txnId.epoch())
            return StoreParticipants.touches(safeStore, lowEpoch, txnId, highEpoch, route);
        if (blockedUntil.waitsOn == blockedUntil.fetchFrom || queryShardsNotHome())
            return awaitRoute;
        return slicedRoute;
    }

    void setWaitingDone(DefaultProgressLog owner)
    {
        setBlockedUntil(CanApply);
        set(null, owner, CanApply, NoneExpected);
        owner.clearPendingAndActive(Waiting, txnId);
        clearWaitingRunCounter();
    }

    void setBlockedUntil(SafeCommandStore safeStore, DefaultProgressLog owner, BlockedUntil newBlockedUntil)
    {
        BlockedUntil currentlyBlockedUntil = blockedUntil();
        BlockedUntil currentlyQuerying = querying();
        if (newBlockedUntil.compareTo(currentlyBlockedUntil) <= 0)
            return;

        setBlockedUntil(newBlockedUntil);
        // no point clearing any in progress work to reach a lower status, since we advance sequentially anyway
        if (waitingProgress() == NoneExpected)
            set(safeStore, owner, currentlyQuerying, Queued);
    }

    void record(DefaultProgressLog owner, SaveStatus newSaveStatus)
    {
        BlockedUntil currentlyBlockedUntil = blockedUntil();
        if (currentlyBlockedUntil.unblockedFrom.compareTo(newSaveStatus) <= 0)
        {
            boolean isDone = newSaveStatus.hasBeen(Status.PreApplied);
            if (isDone)
            {
                setWaitingDone(owner);
                maybeRemove(owner);
            }
            else
            {
                set(null, owner, currentlyBlockedUntil, NoneExpected);
                owner.clearPendingAndActive(Waiting, txnId);
            }
        }
    }

    final void runWaiting(DefaultProgressLog owner, SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        runInternal(safeStore, safeCommand, owner, owner.node.agent().trace(txnId, maxParticipants(safeCommand), WaitProgress));
    }

    private void runInternal(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, @Nullable Tracing tracing)
    {
        Invariants.require(!owner.hasPending(Waiting, txnId));
        incrementWaitingRunCounter();
        BlockedUntil blockedUntil = blockedUntil();
        Command command = safeCommand.current();
        if (command.saveStatus().compareTo(SaveStatus.Erased) >= 0 // TODO (expected): improve progress log clearing to guarantee we don't encounter this status
            || !Invariants.expect(command.saveStatus().compareTo(blockedUntil.unblockedFrom) < 0,
                           "Command has met desired criteria (%s) but progress log entry has not been cancelled: %s", blockedUntil.unblockedFrom, command))
        {
            setWaitingDone(owner);
            return;
        }
        TxnId txnId = safeCommand.txnId();
        // first make sure we have enough information to obtain the command locally
        Timestamp executeAt = command.executeAtIfKnown();
        Participants<?> maxContactable = Invariants.nonNull(command.maxParticipants());

        if (!Route.isRoute(maxContactable))
        {
            if (tracing != null)
                tracing.trace(owner.commandStore, "Blocked until %s. Fetching route from %s", blockedUntil, maxContactable);
            set(safeStore, owner, HasDecidedExecuteAt, Querying);
            // TODO (required): pass in InvalidIf
            fetchRoute(owner, HasDecidedExecuteAt, txnId, maxContactable);
            return;
        }
        Route<?> route = Route.castToRoute(maxContactable);

        BlockedUntil satisfied = BlockedUntil.forSaveStatus(command.saveStatus());
        BlockedUntil next = Invariants.nonNull(satisfied.next());
        if (homeSatisfies().compareTo(satisfied) <= 0)
        {
            // TODO (expected): we should wait until Durability indicates a Quorum on all shards for the relevant status
            // first wait until the homeKey has progressed to a point where it can answer our query; we don't expect our shards to know until then anyway
            if (tracing != null)
                tracing.trace(owner.commandStore, "Blocked until %s. Waiting for home key %s to satisfy %s.", blockedUntil, route.homeKey(), next);
            clearAwaitState();
            set(safeStore, owner, next, Querying);
            awaitHomeKey(owner, next, txnId, executeAt, route, tracing);
            return;
        }

        BlockedUntil querying = command.hasBeen(Status.PreCommitted) ? homeSatisfies() : HasDecidedExecuteAt;
        set(safeStore, owner, querying, Querying);
        long prevLowEpoch = readLowEpoch(safeStore, txnId, route);
        long prevHighEpoch = readHighEpoch(safeStore, txnId, route);
        long lowEpoch = updateLowEpoch(safeStore, txnId, command);
        long highEpoch = updateHighEpoch(safeStore, txnId, querying, command, executeAt);
        // TODO (expected): split into txn and deps sources
        Route<?> slicedRoute = slicedRoute(safeStore, txnId, route, lowEpoch, highEpoch);
        Invariants.require(!slicedRoute.isEmpty());

        // the awaitRoute may be only the home shard, if that is sufficient to indicate the fetchRoute will be able to answer our query;
        // the fetchRoute may also be only the home shard, if that is sufficient to answer our query (e.g. for executeAt)
        // TODO (expected): this handles range transactions very poorly, as a range may be split amongst multiple shards but we cannot wait for them async independently
        //   we probably want to deterministically split ranges into shards on the epochs we care about and treat them separately
        Route<?> awaitRoute = awaitRoute(slicedRoute, querying);
        Route<?> fetchRoute = fetchRoute(slicedRoute, awaitRoute, querying, safeStore, lowEpoch, txnId, highEpoch, route);
        if (awaitRoute.isHomeKeyOnlyRoute())
        {
            // at this point we can switch to polling as we know someone has the relevant state
            if (tracing != null)
                tracing.trace(owner.commandStore, "Blocked until %s. Fetching %s%s for epochs [%d..%d].", querying, slicedRoute, slicedRoute == fetchRoute ? "" : " from " + fetchRoute, lowEpoch, highEpoch);
            fetch(owner, querying, txnId, invalidIf(), executeAt, slicedRoute, fetchRoute, route);
            return;
        }

        int roundSize = awaitRoundSize(awaitRoute);
        if (hasAwaitStarted() && (hasNewLowEpoch(safeStore, txnId, prevLowEpoch, lowEpoch) || hasNewHighEpoch(safeStore, txnId, prevHighEpoch, highEpoch)))
        {
            if (tracing != null)
                tracing.trace(owner.commandStore, "Epoch bounds changed between invocations from [%d...%d] to [%d...%d]", prevLowEpoch, prevHighEpoch, lowEpoch, highEpoch);

            // update round counters because we have changed the epochs involved
            Route<?> prevSlicedRoute = slicedRoute(safeStore, txnId, route, prevLowEpoch, prevHighEpoch);
            Route<?> prevAwaitRoute = awaitRoute(prevSlicedRoute, querying);
            int prevRoundSize = awaitRoundSize(prevAwaitRoute);
            int prevRoundIndex = awaitRoundIndex(prevRoundSize);
            int prevRoundStart = prevRoundIndex * prevRoundSize;
            int newRoundIndex = -1;
            if (prevRoundStart < prevAwaitRoute.size())
                newRoundIndex = (int)awaitRoute.findNextSameKindIntersection(0, (Unseekables)prevAwaitRoute, prevRoundStart + prevRoundIndex);
            if (newRoundIndex < 0)
                newRoundIndex = awaitRoute.size();
            updateAwaitRound(newRoundIndex, roundSize);
        }

        int roundIndex = awaitRoundIndex(roundSize);
        int roundStart = roundIndex * roundSize;
        if (roundStart >= awaitRoute.size())
        {
            if (tracing != null)
                tracing.trace(owner.commandStore, "Blocked until %s, querying %s%s for %s in epochs [%d..%d].", blockedUntil, slicedRoute, slicedRoute == fetchRoute ? "" : " from " + fetchRoute, querying, lowEpoch, highEpoch);

            // all of the shards we are awaiting have been processed and found at least one replica that has the state needed to answer our query
            // at this point we can switch to polling as we know someone has the relevant state
            fetch(owner, querying, txnId, invalidIf(), executeAt, slicedRoute, fetchRoute, route);
            return;
        }

        int roundEnd = Math.min(roundStart + roundSize, awaitRoute.size());
        awaitRoute = awaitRoute.slice(roundStart, roundEnd);
        // TODO (desired): use some mechanism (e.g. random chance or another counter)
        //   to either periodically fetch the whole remaining route or gradually increase the slice length
        if (tracing != null)
            tracing.trace(owner.commandStore, "Blocked until %s. Waiting for %s to satisfy %s; round %d of %d.", blockedUntil, awaitRoute, querying, roundIndex, (awaitRoute.size() + (roundSize - 1))/roundSize);
        setAwaitStarted();
        awaitSlice(owner, querying, txnId, executeAt, awaitRoute, (roundIndex << 1) | 1, tracing);
    }

    // note that ready and notReady may include keys not requested by this progressLog
    static void awaitOrFetchCallback(CallbackKind kind, SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil querying, Unseekables<?> ready, @Nullable BlockedUntil upgrade, @Nullable Known foundAtLeast, Throwable fail)
    {
        WaitingState state = owner.get(txnId);
        Invariants.require(state != null, "State has been cleared but callback was not cancelled");

        Invariants.require(state.waitingProgress() == Querying);
        Invariants.require(state.querying() == querying);

        Command command = safeCommand.current();
        Route<?> route = command.route();
        Tracing tracing = owner.node.agent().trace(txnId, command.participants().max(), WaitProgress);

        if (fail == null)
        {
            if (route == null)
            {
                if (tracing != null)
                    tracing.trace(owner.commandStore, "Callback success, but route not found.");
                Invariants.require(kind == CallbackKind.FetchRoute);
                Invariants.require(ready == null);
                state.retry(safeStore, safeCommand, owner, querying, tracing);
                return;
            }

            if (ready.contains(route.homeKey()) && querying.compareTo(state.homeSatisfies()) > 0)
            {
                // TODO (expected): we can introduce an additional home state check that waits until DURABLE for execution;
                //  at this point it would even be redundant to wait for each separate shard for the WaitingState? Freeing up bits and simplifying.
                BlockedUntil newHomeSatisfies = querying;
                if (upgrade != null && upgrade.compareTo(newHomeSatisfies) > 0)
                    newHomeSatisfies = upgrade;
                state.setHomeSatisfies(newHomeSatisfies);
            }

            long fromLocalEpoch = state.readLowEpoch(safeStore, txnId, route);
            long toLocalEpoch = state.readHighEpoch(safeStore, txnId, route);
            Route<?> slicedRoute = state.slicedRoute(safeStore, txnId, route, fromLocalEpoch, toLocalEpoch); // the actual local keys we care about
            Route<?> awaitRoute = state.awaitRoute(slicedRoute, querying); // either slicedRoute or just the home key
            // TODO (expected): expand conditions under which we escalate to querying shards directly
            if (foundAtLeast != null && foundAtLeast.is(ExecuteAtErased) && awaitRoute != slicedRoute)
                state.markHomeShardErased();

            int roundSize = awaitRoundSize(awaitRoute);
            int roundIndex = state.awaitRoundIndex(roundSize);
            int roundStart = roundIndex * roundSize;

            switch (kind)
            {
                default: throw new UnhandledEnum(kind);

                case AwaitHome:
                    if (ready.contains(route.homeKey()))
                    {
                        if (tracing != null)
                            tracing.trace(owner.commandStore, "Callback success. Home key ready.");
                        // the home shard was found to already have the necessary state, with no distributed await;
                        // we can immediately progress the state machine
                        Invariants.expect(0 == state.awaitRoundIndex(roundSize));
                        Invariants.expect(0 == state.awaitBitSet(roundSize));
                        state.runInternal(safeStore, safeCommand, owner, tracing);
                    }
                    else
                    {
                        if (tracing != null)
                            tracing.trace(owner.commandStore, "Callback success. Home key not ready; waiting async.");
                        // the home shard is not ready to answer our query, but we have registered our remote callback so can wait for it to contact us
                        state.set(safeStore, owner, querying, Awaiting);
                    }
                    break;

                case AwaitSlice:
                    Invariants.expect(state.hasAwaitStarted());
                    Invariants.require(awaitRoute.equals(slicedRoute));
                    // In a production system it is safe for the roundIndex to get corrupted as we will just start polling a bit early,
                    // but for testing we would like to know it has happened.
                    if (Invariants.expect(roundStart < slicedRoute.size()))
                    {
                        Route<?> round = slicedRoute.slice(roundStart, Math.min(slicedRoute.size(), roundStart + roundSize));
                        Participants<?> notReady = round.without(ready);

                        if (notReady.isEmpty())
                        {
                            Invariants.expect((int) slicedRoute.findNextSameKindIntersection(roundStart, (Unseekables) ready, 0) / roundSize == roundIndex);
                            if (roundStart + roundSize >= slicedRoute.size()) state.setAwaitDone(roundSize);
                            else state.updateAwaitRound(roundIndex + 1, roundSize);
                            state.runInternal(safeStore, safeCommand, owner, tracing);
                        }
                        else
                        {
                            Invariants.expect((int) awaitRoute.findNextSameKindIntersection(roundStart, (Unseekables) notReady, 0) / roundSize == roundIndex);
                            // TODO (desired): would be nice to validate this is 0 in cases where we are starting a fresh round
                            //  but have to be careful as cannot zero when we restart as we may have an async callback arrive while we're waiting that then advances state machine
                            state.initialiseAwaitBitSet(awaitRoute, notReady, roundIndex, roundSize);
                            state.set(safeStore, owner, querying, Awaiting);
                        }
                        break;
                    }

                case FetchRoute:
                    if (state.homeSatisfies().compareTo(querying) < 0)
                    {
                        if (tracing != null)
                            tracing.trace(owner.commandStore, "Successfully fetched route; invoking runInternal");
                        state.runInternal(safeStore, safeCommand, owner, tracing);
                        return;
                    }

                case Fetch:
                {
                    Participants<?> notReady = slicedRoute.without(ready);
                    if (!awaitRoute.equals(slicedRoute))
                    {
                        Invariants.expect(awaitRoute.isHomeKeyOnlyRoute());
                        Invariants.expect(state.homeSatisfies().compareTo(querying) >= 0);
                        // nothing to do, fall through and retry
                    }
                    else if (notReady.isEmpty())
                    {
                        if (tracing != null)
                            tracing.trace(owner.commandStore, "BlockedUntil %s, achieved %s; continuing.", state.blockedUntil(), querying);
                        Invariants.expect(state.blockedUntil() != querying, "Fetch %s was successful for all keys, but the WaitingState has not been cleared", querying);
                        BlockedUntil satisfies = BlockedUntil.forSaveStatus(command.saveStatus());
                        if (Invariants.expect(satisfies.compareTo(querying) >= 0, "Fetch %s was successful for all keys, but the Command %s does not reflect the expected state", querying, command))
                        {
                            state.setAwaitDone(roundSize);
                            state.runInternal(safeStore, safeCommand, owner, tracing);
                            return;
                        }
                        // otherwise fall through to delayed retry
                    }
                    else
                    {
                        if (roundStart < slicedRoute.size())
                        {
                            int nextIndex;
                            if (slicedRoute.equals(awaitRoute)) nextIndex = (int) slicedRoute.findNextSameKindIntersection(roundStart, (Unseekables) notReady, 0);
                            else
                            {
                                if (Invariants.expect(!state.hasAwaitStarted())) nextIndex = 0;
                                else nextIndex = -1;
                            }
                            state.setAwaitStarted();

                            if (nextIndex >= 0)
                            {
                                if (tracing != null)
                                    tracing.trace(owner.commandStore, "Found %s notReady %s", notReady);

                                Invariants.require(nextIndex >= roundStart);
                                Invariants.require(roundStart < slicedRoute.size());
                                roundIndex = nextIndex / roundSize;
                                Invariants.require(roundIndex * roundSize < slicedRoute.size());
                                state.updateAwaitRound(roundIndex, roundSize);
                                state.initialiseAwaitBitSet(slicedRoute, notReady, roundIndex, roundSize);
                                state.runInternal(safeStore, safeCommand, owner, tracing);
                                return;
                            }
                        }
                    }

                    if (tracing != null)
                        tracing.trace(owner.commandStore, "Found %s notReady, but no more intersections; marking await done and continuing.", notReady);

                    // we don't think we have anything to wait for, but we have encountered some notReady responses; queue up a retry
                    state.setAwaitDone(roundSize);
                    state.incrementWaitingRunCounter();
                    state.set(safeStore, owner, querying, Queued);
                }
            }
        }
        else
        {
            if (tracing != null)
                tracing.trace(owner.commandStore, "Fai");
            safeStore.agent().onCaughtException(fail, "Failed fetching data for " + state);
            state.retry(safeStore, safeCommand, owner, querying, tracing);
        }
    }

    static void fetchRouteCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, Route<?> found, Throwable fail)
    {
        if (found == null)
            found = safeCommand.current().route();
        Participants<?> ready = found != null ? found.slice(0, 0) : null;
        awaitOrFetchCallback(CallbackKind.FetchRoute, safeStore, safeCommand, owner, txnId, blockedUntil, ready, null, null, fail);
    }

    static void fetchCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil querying, FetchData.FetchResult fetchResult, Throwable fail)
    {
        fetchCallback(Fetch, safeStore, safeCommand, owner, txnId, querying, fetchResult, fail);
    }

    static void fetchCallback(CallbackKind kind, SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil querying, FetchData.FetchResult fetchResult, Throwable fail)
    {
        Invariants.require(fetchResult != null || fail != null);
        Unseekables<?> ready = fetchResult == null ? null : fetchResult.achievedTarget;
        BlockedUntil upgrade = fetchResult == null ? null : BlockedUntil.forSaveStatus(safeCommand.current().saveStatus());
        Known foundAtLeast = fetchResult == null ? null : fetchResult.foundAtLeast;
        awaitOrFetchCallback(kind, safeStore, safeCommand, owner, txnId, querying, ready, upgrade, foundAtLeast, fail);
    }

    static void synchronousAwaitHomeCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, AsynchronousAwait.SynchronousResult awaitResult, Throwable fail)
    {
        synchronousAwaitCallback(AwaitHome, safeStore, safeCommand, owner, txnId, blockedUntil, awaitResult, fail);
    }

    static void synchronousAwaitSliceCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, AsynchronousAwait.SynchronousResult awaitResult, Throwable fail)
    {
        synchronousAwaitCallback(AwaitSlice, safeStore, safeCommand, owner, txnId, blockedUntil, awaitResult, fail);
    }

    static void synchronousAwaitCallback(CallbackKind kind, SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, AsynchronousAwait.SynchronousResult awaitResult, Throwable fail)
    {
        Unseekables<?> ready = awaitResult == null ? null : awaitResult.ready;
        // TODO (desired): extract "upgrade" info from AsynchronousAwait
        awaitOrFetchCallback(kind, safeStore, safeCommand, owner, txnId, blockedUntil, ready, null, null, fail);
    }
    
    void asynchronousAwaitCallback(DefaultProgressLog owner, SafeCommandStore safeStore, SaveStatus newStatus, Node.Id from, int callbackId)
    {
        if ((callbackId & 1) != 1)
            return;

        SafeCommand safeCommand = safeStore.unsafeGet(txnId);
        Tracing tracing = owner.node.agent().trace(txnId, maxParticipants(safeCommand), WaitProgress);

        BlockedUntil querying = querying();
        if (callbackId == AWAITING_HOME_KEY_CALLBACKID)
        {
            // homeKey reply
            BlockedUntil currentHomeStatus = homeSatisfies();
            BlockedUntil newHomeStatus = BlockedUntil.forSaveStatus(newStatus);
            if (newHomeStatus.compareTo(currentHomeStatus) > 0)
                setHomeSatisfies(newHomeStatus);

            if (waitingProgress() != Awaiting)
            {
                if (tracing != null)
                    tracing.trace(owner.commandStore, "Received async home key callback %d but no longer Awaiting", callbackId);
                return;
            }

            if (newHomeStatus.compareTo(querying) < 0 || currentHomeStatus.compareTo(querying) >= 0)
            {
                if (tracing != null)
                    tracing.trace(owner.commandStore, "Received redundant async home key callback %d. Blocked until %s, querying %s, home key now %s (previously %s)", callbackId, blockedUntil(), querying, newHomeStatus, currentHomeStatus);
                return;
            }

            if (safeCommand != null)
            {
                if (tracing != null)
                    tracing.trace(owner.commandStore, "Received async home key callback %d. Blocked until %s, querying %s, home key now %s.", callbackId, blockedUntil(), querying, newHomeStatus);
                runInternal(safeStore, safeCommand, owner, tracing);
            }
        }
        else
        {
            if (waitingProgress() != Awaiting)
            {
                if (tracing != null)
                    tracing.trace(owner.commandStore, "Received async callback %d but no longer Awaiting", callbackId);
                return;
            }

            if (newStatus.compareTo(querying.unblockedFrom) < 0)
            {
                if (tracing != null)
                    tracing.trace(owner.commandStore, "Received async callback %d with %s, insufficient for %s", callbackId, newStatus, querying);
                return;
            }

            callbackId >>= 1;
            Invariants.nonNull(safeCommand);
            Route<?> route = Route.castToRoute(safeCommand.current().maxParticipants());
            long lowEpoch = readLowEpoch(safeStore, txnId, route);
            long highEpoch = readHighEpoch(safeStore, txnId, route);
            Route<?> slicedRoute = slicedRoute(safeStore, txnId, route, lowEpoch, highEpoch);

            int roundSize = awaitRoundSize(slicedRoute);
            int roundIndex = awaitRoundIndex(roundSize);
            int updateBitSet = roundCallbackBitSet(owner, txnId, from, slicedRoute, callbackId, roundIndex, roundSize);
            if (updateBitSet == 0)
            {
                if (tracing != null)
                    tracing.trace(owner.commandStore, "Received async callback %d for already ready keys.", callbackId, querying);
                return;
            }

            int bitSet = awaitBitSet(roundSize);
            bitSet &= ~updateBitSet;
            setAwaitBitSet(bitSet, roundSize);

            if (bitSet == 0)
            {
                if (tracing != null)
                    tracing.trace(owner.commandStore, "Blocked until %s. Received async callback %d for waiting keys. Round complete.", querying, callbackId);

                runInternal(safeStore, safeCommand, owner, tracing);
            }
            else
            {
                if (tracing != null)
                    tracing.trace(owner.commandStore, "Blocked until %s. Received async callback %d for waiting keys. %d keys still waiting this round.", querying, callbackId, Integer.bitCount(bitSet));
            }
        }
    }

    // TODO (expected): use back-off counter here
    private void retry(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, BlockedUntil querying, @Nullable Tracing tracing)
    {
        if (!contactEveryone())
        {
            if (tracing != null)
                tracing.trace(owner.commandStore, "Retrying immediately, without contact restrictions");
            setContactEveryone(true);
            // try again immediately with a query to all eligible replicas
            runInternal(safeStore, safeCommand, owner, tracing);
        }
        else
        {
            if (tracing != null)
                tracing.trace(owner.commandStore, "Retry queued for later.");
            incrementWaitingRunCounter();
            set(safeStore, owner, querying, Queued);
        }
    }

    static void fetchRoute(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Participants<?> contactable)
    {
        // TODO (desired): fetch only the route
        // we MUSt allocate before calling withEpoch to register cancellation, as async
        CallbackInvoker<BlockedUntil, Route<?>> invoker = invokeWaitingCallback(owner, txnId, blockedUntil, WaitingState::fetchRouteCallback);
        owner.start(invoker, FetchRoute.fetchRoute(owner.node(), txnId, contactable, new IncludingSpecificStoreSelector(owner.commandStore.id()), invoker));
    }

    static void fetch(DefaultProgressLog owner, BlockedUntil querying, TxnId txnId, Infer.InvalidIf invalidIf, Timestamp executeAt, Route<?> slicedRoute, Route<?> fetchRoute, Route<?> maxRoute)
    {
        Invariants.require(!slicedRoute.isEmpty());
        // we MUSt allocate before calling withEpoch to register cancellation, as async
        CallbackInvoker<BlockedUntil, FetchData.FetchResult> invoker = invokeWaitingCallback(owner, txnId, querying, WaitingState::fetchCallback);
        owner.start(invoker, FetchData.fetchSpecific(querying.unblockedFrom.known, owner.node(), txnId, invalidIf, executeAt, fetchRoute, maxRoute, new IncludingSpecificStoreSelector(owner.commandStore.id()), invoker));
    }

    void awaitHomeKey(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, Route<?> route, @Nullable Tracing tracing)
    {
        // TODO (expected): special-case when this shard is home key to avoid remote messages
        await(owner, blockedUntil, txnId, executeAt, route.homeKeyOnlyRoute(), AWAITING_HOME_KEY_CALLBACKID, WaitingState::synchronousAwaitHomeCallback, tracing);
    }

    void awaitSlice(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, Route<?> route, int callbackId, @Nullable Tracing tracing)
    {
        Invariants.require(blockedUntil.waitsOn == SHARD);
        // TODO (expected): special-case when this shard is home key to avoid remote messages
        await(owner, blockedUntil, txnId, executeAt, route, callbackId, WaitingState::synchronousAwaitSliceCallback, tracing);
    }

    void await(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, Route<?> route, int callbackId, Callback<BlockedUntil, AsynchronousAwait.SynchronousResult> callback, @Nullable Tracing tracing)
    {
        long epoch = blockedUntil.fetchEpoch(txnId, executeAt);
        Await.Until awaitUntil = blockedUntil.toAwait();
        // we MUST allocate the invoker before invoking withEpoch as this may be asynchronous and we must first register our callback for cancellation
        CallbackInvoker<BlockedUntil, AsynchronousAwait.SynchronousResult> invoker = invokeWaitingCallback(owner, txnId, blockedUntil, callback);
        owner.start(invoker, owner.node().withEpochAtLeast(epoch, (AsyncExecutor)null, invoker, () -> {
            AsynchronousAwait.awaitAny(owner.node(), contact(owner, route, epoch), txnId, route, awaitUntil, callbackId, invoker);
        }));
    }

    String toStateString()
    {
        BlockedUntil querying = querying();
        BlockedUntil blockedUntil = blockedUntil();
        Progress progress = waitingProgress();
        switch (progress)
        {
            default:
                throw new UnhandledEnum(progress);
            case NoneExpected:
                return blockedUntil == CanApply ? "Done" : "NotWaiting";
            case Queued:
                return "Queued(" + querying + "/" + blockedUntil + ")";
            case Querying:
                return "Querying(" + querying + "/" + blockedUntil + ")";
            case Awaiting:
                return "Awaiting(" + querying + "/" + blockedUntil + ")";
        }
    }

    boolean isWaitingDone()
    {
        return waitingProgress() == NoneExpected && blockedUntil() == CanApply;
    }

    enum CallbackKind
    {
        Fetch, FetchRoute, AwaitHome, AwaitSlice
    }
}
