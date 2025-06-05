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

import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.ProgressLog.BlockedUntil;
import accord.coordinate.AsynchronousAwait;
import accord.coordinate.FetchData;
import accord.local.Command;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
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
import static accord.api.ProgressLog.BlockedUntil.Query.HOME;
import static accord.api.ProgressLog.BlockedUntil.Query.SHARD;
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
import static accord.impl.progresslog.WaitingState.CallbackKind.FetchRoute;
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
    private static final long BLOCKED_UNTIL_MASK = 0x7;
    private static final int HOME_SATISFIES_SHIFT = 5;
    private static final long HOME_SATISFIES_MASK = 0x7;
    private static final int AWAIT_SHIFT = 8;
    private static final int AWAIT_BITS = 28;
    private static final long AWAIT_MASK = (1L << AWAIT_BITS) - 1;
    private static final int AWAIT_EPOCH_SHIFT = AWAIT_SHIFT + AWAIT_BITS;
    private static final int AWAIT_EPOCH_BITS = 4;
    private static final long SET_MASK = ~((PROGRESS_MASK << PROGRESS_SHIFT) | (BLOCKED_UNTIL_MASK << BLOCKED_UNTIL_SHIFT));
    private static final long INITIALISED_MASK = (PROGRESS_MASK << PROGRESS_SHIFT) | (BLOCKED_UNTIL_MASK << BLOCKED_UNTIL_SHIFT) | (HOME_SATISFIES_MASK << HOME_SATISFIES_SHIFT);

    private static final int RETRY_COUNTER_SHIFT = AWAIT_EPOCH_SHIFT + AWAIT_EPOCH_BITS;
    private static final long RETRY_COUNTER_MASK = 0x7;
    static final int WAITING_STATE_END_SHIFT = RETRY_COUNTER_SHIFT + 3;

    // when awaiting shards we register callbacks numbered by the keys we're processing;
    // we want to special-case the home key callback and its easiest to pick the highest integer
    // so that we know it won't clash
    public static final int AWAITING_HOME_KEY_CALLBACKID = Integer.MAX_VALUE;

    WaitingState(TxnId txnId)
    {
        super(txnId);
    }

    private void set(@Nullable SafeCommandStore safeStore, DefaultProgressLog owner, BlockedUntil newBlockedUntil, Progress newProgress)
    {
        encodedState &= SET_MASK;
        encodedState |= ((long) newBlockedUntil.ordinal() << BLOCKED_UNTIL_SHIFT) | ((long) newProgress.ordinal() << PROGRESS_SHIFT);
        updateScheduling(safeStore, owner, Waiting, newBlockedUntil, newProgress);
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

    @Nonnull BlockedUntil homeSatisfies()
    {
        return homeSatisfies(encodedState);
    }

    final @Nonnull Progress waitingProgress()
    {
        return waitingProgress(encodedState);
    }

    final @Nonnull long waitingKeyTrackerBits()
    {
        return (encodedState >>> AWAIT_SHIFT) & (-1L >>> (64 - AWAIT_BITS));
    }

    private static @Nonnull BlockedUntil blockedUntil(long encodedState)
    {
        return BlockedUntil.forOrdinal((int) ((encodedState >>> BLOCKED_UNTIL_SHIFT) & BLOCKED_UNTIL_MASK));
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
        return roundSize(slicedRoute.size(), AWAIT_BITS);
    }

    private void clearAwaitState()
    {
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

    final int waitingRetryCounter()
    {
        return (int) ((encodedState >>> RETRY_COUNTER_SHIFT) & RETRY_COUNTER_MASK);
    }

    final void incrementWaitingRetryCounter()
    {
        long shiftedMask = RETRY_COUNTER_MASK << RETRY_COUNTER_SHIFT;
        long current = encodedState & shiftedMask;
        long updated = Math.min(shiftedMask, current + (1L << RETRY_COUNTER_SHIFT));
        encodedState &= ~shiftedMask;
        encodedState |= updated;
    }

    final void clearWaitingRetryCounter()
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
            lowEpoch = safeStore.ranges().latestEarlierEpochThatFullyCovers(safeStore, lowEpoch, command.maxContactable());
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
        return safeStore.ranges().latestEarlierEpochThatFullyCovers(safeStore, epoch, route);
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
            highEpoch = safeStore.ranges().earliestLaterEpochThatFullyCovers(highEpoch, command.maxContactable());
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
        int offset = (int) ((encodedState >>> (AWAIT_EPOCH_SHIFT + 2)) & 0x3);
        RangesForEpoch ranges = safeStore.ranges();
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

    private static Route<?> slicedRoute(SafeCommandStore safeStore, TxnId txnId, Route<?> route, long fromLocalEpoch, long toLocalEpoch)
    {
        Route<?> result = StoreParticipants.touches(safeStore, fromLocalEpoch, txnId, toLocalEpoch, route);
        if (result.isEmpty())
            result = result.homeKeyOnlyRoute();
        return result;
    }

    private static Route<?> awaitRoute(Route<?> slicedRoute, BlockedUntil blockedUntil)
    {
        return blockedUntil.waitsOn == HOME ? slicedRoute.homeKeyOnlyRoute() : slicedRoute;
    }

    private static Route<?> fetchRoute(Route<?> slicedRoute, Route<?> awaitRoute, BlockedUntil blockedUntil, SafeCommandStore safeStore, long lowEpoch, TxnId txnId, long highEpoch, Route<?> route)
    {
        if (lowEpoch < txnId.epoch())
            return StoreParticipants.touches(safeStore, lowEpoch, txnId, highEpoch, route);
        if (blockedUntil.waitsOn == blockedUntil.fetchFrom)
            return awaitRoute;
        return slicedRoute;
    }

    void setWaitingDone(DefaultProgressLog owner)
    {
        set(null, owner, CanApply, NoneExpected);
        owner.clearActive(Waiting, txnId);
        clearWaitingRetryCounter();
    }

    void setBlockedUntil(SafeCommandStore safeStore, DefaultProgressLog owner, BlockedUntil blockedUntil)
    {
        BlockedUntil currentlyBlockedUntil = blockedUntil();
        if (blockedUntil.compareTo(currentlyBlockedUntil) > 0 || isUninitialised())
        {
            clearAwaitState();
            clearWaitingRetryCounter();
            owner.clearActive(Waiting, txnId);
            set(safeStore, owner, blockedUntil, Queued);
        }
    }

    void record(DefaultProgressLog owner, SaveStatus newSaveStatus)
    {
        BlockedUntil currentlyBlockedUntil = blockedUntil();
        if (currentlyBlockedUntil.unblockedFrom.compareTo(newSaveStatus) <= 0)
        {
            boolean isDone = newSaveStatus.hasBeen(Status.PreApplied);
            set(null, owner, isDone ? CanApply : currentlyBlockedUntil, NoneExpected);
            if (isDone)
                maybeRemove(owner);
            owner.clearActive(Waiting, txnId);
        }
    }

    final void runWaiting(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner)
    {
        runInternal(safeStore, safeCommand, owner);
    }

    private void runInternal(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner)
    {
        BlockedUntil blockedUntil = blockedUntil();
        Command command = safeCommand.current();
        Invariants.require(!owner.hasActive(Waiting, txnId));
        Invariants.require(command.saveStatus().compareTo(blockedUntil.unblockedFrom) < 0,
                           "Command has met desired criteria (%s) but progress log entry has not been cancelled: %s", blockedUntil.unblockedFrom, command);

        set(safeStore, owner, blockedUntil, Querying);
        TxnId txnId = safeCommand.txnId();
        // first make sure we have enough information to obtain the command locally
        Timestamp executeAt = command.executeAtIfKnown();
        Participants<?> fetchKeys = Invariants.nonNull(command.maxContactable());

        if (!Route.isRoute(fetchKeys))
        {
            long lowEpoch = updateLowEpoch(safeStore, txnId, command);
            long highEpoch = updateHighEpoch(safeStore, txnId, blockedUntil, command, executeAt);
            fetchRoute(owner, blockedUntil, txnId, executeAt, lowEpoch, highEpoch, fetchKeys);
            return;
        }

        Route<?> route = Route.castToRoute(fetchKeys);
        if (homeSatisfies().compareTo(blockedUntil) < 0)
        {
            // first wait until the homeKey has progressed to a point where it can answer our query; we don't expect our shards to know until then anyway
            awaitHomeKey(owner, blockedUntil, txnId, executeAt, route);
            return;
        }

        long prevLowEpoch = readLowEpoch(safeStore, txnId, route);
        long prevHighEpoch = readHighEpoch(safeStore, txnId, route);
        long lowEpoch = updateLowEpoch(safeStore, txnId, command);
        long highEpoch = updateHighEpoch(safeStore, txnId, blockedUntil, command, executeAt);
        // TODO (expected): split into txn and deps sources
        Route<?> slicedRoute = slicedRoute(safeStore, txnId, route, lowEpoch, highEpoch);
        Invariants.require(!slicedRoute.isEmpty());
        if (!command.hasBeen(Status.PreCommitted))
        {
            // we know it has been decided one way or the other by the home shard at least, so we attempt a fetch
            // including the home shard to get us to at least PreCommitted where we can safely wait on individual shards
            fetch(owner, blockedUntil, txnId, executeAt, lowEpoch, highEpoch, slicedRoute, slicedRoute.withHomeKey(), route);
            return;
        }

        // the awaitRoute may be only the home shard, if that is sufficient to indicate the fetchRoute will be able to answer our query;
        // the fetchRoute may also be only the home shard, if that is sufficient to answer our query (e.g. for executeAt)
        Route<?> awaitRoute = awaitRoute(slicedRoute, blockedUntil);
        Route<?> fetchRoute = fetchRoute(slicedRoute, awaitRoute, blockedUntil, safeStore, lowEpoch, txnId, highEpoch, route);

        if (awaitRoute.isHomeKeyOnlyRoute())
        {
            // at this point we can switch to polling as we know someone has the relevant state
            fetch(owner, blockedUntil, txnId, executeAt, lowEpoch, highEpoch, slicedRoute, fetchRoute, route);
            return;
        }

        int roundSize = awaitRoundSize(awaitRoute);
        if (hasNewLowEpoch(safeStore, txnId, prevLowEpoch, lowEpoch) || hasNewHighEpoch(safeStore, txnId, prevHighEpoch, highEpoch))
        {
            // update round counters because we have changed the epochs involved
            Route<?> prevSlicedRoute = slicedRoute(safeStore, txnId, route, prevLowEpoch, prevHighEpoch);
            Route<?> prevAwaitRoute = awaitRoute(prevSlicedRoute, blockedUntil);
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
            // all of the shards we are awaiting have been processed and found at least one replica that has the state needed to answer our query
            // at this point we can switch to polling as we know someone has the relevant state
            fetch(owner, blockedUntil, txnId, executeAt, lowEpoch, highEpoch, slicedRoute, fetchRoute, route);
            return;
        }

        int roundEnd = Math.min(roundStart + roundSize, awaitRoute.size());
        awaitRoute = awaitRoute.slice(roundStart, roundEnd);
        // TODO (desired): use some mechanism (e.g. random chance or another counter)
        //   to either periodically fetch the whole remaining route or gradually increase the slice length
        awaitSlice(owner, blockedUntil, txnId, executeAt, awaitRoute, (roundIndex << 1) | 1);
    }

    // note that ready and notReady may include keys not requested by this progressLog
    static void awaitOrFetchCallback(CallbackKind kind, SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, Unseekables<?> ready, @Nullable Unseekables<?> notReady, @Nullable BlockedUntil upgrade, Throwable fail)
    {
        WaitingState state = owner.get(txnId);
        Invariants.require(state != null, "State has been cleared but callback was not cancelled");

        Invariants.require(state.waitingProgress() == Querying);
        Invariants.require(state.blockedUntil() == blockedUntil);

        Command command = safeCommand.current();
        Route<?> route = command.route();

        if (fail == null)
        {
            if (route == null)
            {
                Invariants.require(kind == FetchRoute);
                state.retry(safeStore, safeCommand, owner, blockedUntil);
                return;
            }

            if (ready.contains(route.homeKey()) && blockedUntil.compareTo(state.homeSatisfies()) > 0)
            {
                // TODO (expected): we can introduce an additional home state check that waits until DURABLE for execution;
                //  at this point it would even be redundant to wait for each separate shard for the WaitingState? Freeing up bits and simplifying.
                BlockedUntil newHomeSatisfies = blockedUntil;
                if (upgrade != null && upgrade.compareTo(newHomeSatisfies) > 0)
                    newHomeSatisfies = upgrade;
                state.setHomeSatisfies(newHomeSatisfies);
            }

            long fromLocalEpoch = state.readLowEpoch(safeStore, txnId, route);
            long toLocalEpoch = state.readHighEpoch(safeStore, txnId, route);
            Route<?> slicedRoute = slicedRoute(safeStore, txnId, route, fromLocalEpoch, toLocalEpoch); // the actual local keys we care about
            Route<?> awaitRoute = awaitRoute(slicedRoute, blockedUntil); // either slicedRoute or just the home key

            int roundSize = awaitRoundSize(awaitRoute);
            int roundIndex = state.awaitRoundIndex(roundSize);
            int roundStart = roundIndex * roundSize;

            switch (kind)
            {
                default: throw new UnhandledEnum(kind);

                case AwaitHome:
                    if (notReady == null)
                    {
                        // the home shard was found to already have the necessary state, with no distributed await;
                        // we can immediately progress the state machine
                        Invariants.require(0 == state.awaitRoundIndex(roundSize));
                        Invariants.require(0 == state.awaitBitSet(roundSize));
                        state.runInternal(safeStore, safeCommand, owner);
                    }
                    else
                    {
                        // the home shard is not ready to answer our query, but we have registered our remote callback so can wait for it to contact us
                        state.set(safeStore, owner, blockedUntil, Awaiting);
                    }
                    break;

                case AwaitSlice:
                    Invariants.require(awaitRoute == slicedRoute);
                    // In a production system it is safe for the roundIndex to get corrupted as we will just start polling a bit early,
                    // but for testing we would like to know it has happened.
                    if (Invariants.expect(roundStart < roundSize))
                    {
                        if (notReady == null)
                        {
                            Invariants.expect((int) awaitRoute.findNextSameKindIntersection(roundStart, (Unseekables) ready, 0) / roundSize == roundIndex);
                            // TODO (desired): in this case perhaps upgrade to fetch for next round?
                            state.updateAwaitRound(roundIndex + 1, roundSize);
                            state.runInternal(safeStore, safeCommand, owner);
                        }
                        else
                        {
                            Invariants.expect((int) awaitRoute.findNextSameKindIntersection(roundStart, (Unseekables) notReady, 0) / roundSize == roundIndex);
                            // TODO (desired): would be nice to validate this is 0 in cases where we are starting a fresh round
                            //  but have to be careful as cannot zero when we restart as we may have an async callback arrive while we're waiting that then advances state machine
                            state.initialiseAwaitBitSet(awaitRoute, notReady, roundIndex, roundSize);
                            state.set(safeStore, owner, blockedUntil, Awaiting);
                        }
                        break;
                    }

                case FetchRoute:
                    if (state.homeSatisfies().compareTo(blockedUntil) < 0)
                    {
                        state.runInternal(safeStore, safeCommand, owner);
                        return;
                    }
                    // we may not have requested everything since we didn't have a Route, so calculate our own notReady and fall-through
                    notReady = slicedRoute.without(ready);

                case Fetch:
                {
                    Invariants.require(notReady != null, "Fetch was successful for all keys, but the WaitingState has not been cleared");
                    Invariants.require(notReady.intersects(slicedRoute), "Fetch was successful for all keys, but the WaitingState has not been cleared");
                    int nextIndex;
                    if (roundStart >= awaitRoute.size()) nextIndex = -1;
                    else if (slicedRoute == awaitRoute) nextIndex = (int) awaitRoute.findNextSameKindIntersection(roundStart, (Unseekables) notReady, 0);
                    else
                    {
                        Invariants.require(roundIndex == 0);
                        nextIndex = 0;
                    }

                    if (nextIndex < 0)
                    {
                        // we don't think we have anything to wait for, but we have encountered some notReady responses; queue up a retry
                        state.setAwaitDone(roundSize);
                        state.retry(safeStore, safeCommand, owner, blockedUntil);
                    }
                    else
                    {
                        Invariants.require(nextIndex >= roundStart);
                        roundIndex = nextIndex / roundSize;
                        state.updateAwaitRound(roundIndex, roundSize);
                        state.initialiseAwaitBitSet(awaitRoute, notReady, roundIndex, roundSize);
                        state.runInternal(safeStore, safeCommand, owner);
                    }
                }
            }
        }
        else
        {
            safeStore.agent().onCaughtException(fail, "Failed fetching data for " + state);
            state.retry(safeStore, safeCommand, owner, blockedUntil);
        }
    }

    static void fetchRouteCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, FetchData.FetchResult fetchResult, Throwable fail)
    {
        fetchCallback(FetchRoute, safeStore, safeCommand, owner, txnId, blockedUntil, fetchResult, fail);
    }

    static void fetchCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, FetchData.FetchResult fetchResult, Throwable fail)
    {
        fetchCallback(Fetch, safeStore, safeCommand, owner, txnId, blockedUntil, fetchResult, fail);
    }

    static void fetchCallback(CallbackKind kind, SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, BlockedUntil blockedUntil, FetchData.FetchResult fetchResult, Throwable fail)
    {
        Invariants.require(fetchResult != null || fail != null);
        Unseekables<?> ready = fetchResult == null ? null : fetchResult.achievedTarget;
        Unseekables<?> notReady = fetchResult == null ? null : fetchResult.didNotAchieveTarget;
        BlockedUntil upgrade = fetchResult == null ? null : BlockedUntil.forSaveStatus(fetchResult.achieved.propagatesSaveStatus());
        awaitOrFetchCallback(kind, safeStore, safeCommand, owner, txnId, blockedUntil, ready, notReady, upgrade, fail);
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
        Unseekables<?> notReady = awaitResult == null ? null : awaitResult.notReady;
        // TODO (desired): extract "upgrade" info from AsynchronousAwait
        awaitOrFetchCallback(kind, safeStore, safeCommand, owner, txnId, blockedUntil, ready, notReady, null, fail);
    }
    
    void asynchronousAwaitCallback(DefaultProgressLog owner, SafeCommandStore safeStore, SaveStatus newStatus, Node.Id from, int callbackId)
    {
        if ((callbackId & 1) != 1)
            return;

        BlockedUntil blockedUntil = blockedUntil();
        if (callbackId == AWAITING_HOME_KEY_CALLBACKID)
        {
            // homeKey reply
            BlockedUntil currentHomeStatus = homeSatisfies();
            BlockedUntil newHomeStatus = BlockedUntil.forSaveStatus(newStatus);
            if (newHomeStatus.compareTo(currentHomeStatus) > 0)
                setHomeSatisfies(newHomeStatus);

            if (waitingProgress() != Awaiting)
                return;

            if (newHomeStatus.compareTo(blockedUntil) < 0 || currentHomeStatus.compareTo(blockedUntil) >= 0)
                return;

            SafeCommand safeCommand = safeStore.unsafeGet(txnId);
            if (safeCommand != null)
                runInternal(safeStore, safeCommand, owner);
        }
        else
        {
            if (waitingProgress() != Awaiting)
                return;

            callbackId >>= 1;
            SafeCommand safeCommand = Invariants.nonNull(safeStore.unsafeGet(txnId));
            Route<?> route = Route.castToRoute(safeCommand.current().maxContactable());
            long lowEpoch = readLowEpoch(safeStore, txnId, route);
            long highEpoch = readLowEpoch(safeStore, txnId, route);
            Route<?> slicedRoute = slicedRoute(safeStore, txnId, route, lowEpoch, highEpoch);

            int roundSize = awaitRoundSize(slicedRoute);
            int roundIndex = awaitRoundIndex(roundSize);
            int updateBitSet = roundCallbackBitSet(owner, txnId, from, slicedRoute, callbackId, roundIndex, roundSize);
            if (updateBitSet == 0)
                return;

            int bitSet = awaitBitSet(roundSize);
            bitSet &= ~updateBitSet;
            setAwaitBitSet(bitSet, roundSize);

            if (bitSet == 0)
                runInternal(safeStore, safeCommand, owner);
        }
    }

    // TODO (expected): use back-off counter here
    private void retry(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, BlockedUntil blockedUntil)
    {
        if (!contactEveryone())
        {
            setContactEveryone(true);
            // try again immediately with a query to all eligible replicas
            runInternal(safeStore, safeCommand, owner);
        }
        else
        {
            // queue a retry
            set(safeStore, owner, blockedUntil, Queued);
        }
    }

    static void fetchRoute(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, long lowEpoch, long highEpoch, Participants<?> fetchKeys)
    {
        // TODO (desired): fetch only the route
        // we MUSt allocate before calling withEpoch to register cancellation, as async
        BiConsumer<FetchData.FetchResult, Throwable> invoker = invokeWaitingCallback(owner, txnId, blockedUntil, WaitingState::fetchRouteCallback);
        FetchData.fetch(blockedUntil.unblockedFrom.known, owner.node(), txnId, executeAt, fetchKeys, lowEpoch, highEpoch, invoker);
    }

    static void fetch(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, long lowEpoch, long highEpoch, Route<?> slicedRoute, Route<?> fetchRoute, Route<?> maxRoute)
    {
        Invariants.require(!slicedRoute.isEmpty());
        // we MUSt allocate before calling withEpoch to register cancellation, as async
        BiConsumer<FetchData.FetchResult, Throwable> invoker = invokeWaitingCallback(owner, txnId, blockedUntil, WaitingState::fetchCallback);
        FetchData.fetchSpecific(blockedUntil.unblockedFrom.known, owner.node(), txnId, executeAt, fetchRoute, maxRoute, slicedRoute, lowEpoch, highEpoch, invoker);
    }

    void awaitHomeKey(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, Route<?> route)
    {
        // TODO (expected): special-case when this shard is home key to avoid remote messages
        await(owner, blockedUntil, txnId, executeAt, route.homeKeyOnlyRoute(), AWAITING_HOME_KEY_CALLBACKID, WaitingState::synchronousAwaitHomeCallback);
    }

    void awaitSlice(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, Route<?> route, int callbackId)
    {
        Invariants.require(blockedUntil.waitsOn == SHARD);
        // TODO (expected): special-case when this shard is home key to avoid remote messages
        await(owner, blockedUntil, txnId, executeAt, route, callbackId, WaitingState::synchronousAwaitSliceCallback);
    }

    void await(DefaultProgressLog owner, BlockedUntil blockedUntil, TxnId txnId, Timestamp executeAt, Route<?> route, int callbackId, Callback<BlockedUntil, AsynchronousAwait.SynchronousResult> callback)
    {
        long epoch = blockedUntil.fetchEpoch(txnId, executeAt);
        // we MUST allocate the invoker before invoking withEpoch as this may be asynchronous and we must first register our callback for cancellation
        BiConsumer<AsynchronousAwait.SynchronousResult, Throwable> invoker = invokeWaitingCallback(owner, txnId, blockedUntil, callback);
        owner.node().withEpoch(epoch, invoker, () -> {
            AsynchronousAwait.awaitAny(owner.node(), contact(owner, route, epoch), txnId, route, blockedUntil, callbackId, invoker);
        });
    }

    String toStateString()
    {
        BlockedUntil blockedUntil = blockedUntil();
        Progress progress = waitingProgress();
        switch (progress)
        {
            default:
                throw new UnhandledEnum(progress);
            case NoneExpected:
                return blockedUntil == CanApply ? "Done" : "NotWaiting";
            case Queued:
                return "Queued(" + blockedUntil + ")";
            case Querying:
                return "Querying(" + blockedUntil + ")";
            case Awaiting:
                return "Awaiting(" + blockedUntil + ")";
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
