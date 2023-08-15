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

package accord.coordinate;

import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import accord.local.Command;
import accord.local.Commands;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.messages.CheckStatus;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import static accord.local.PreLoadContext.contextFor;
import static accord.local.Status.Durability.Majority;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.PreApplied;
import static accord.local.Status.PreCommitted;
import static accord.messages.CheckStatus.WithQuorum.HasQuorum;
import static accord.primitives.Route.castToRoute;
import static accord.primitives.Route.isRoute;

public class Infer
{
    private static abstract class CleanupAndCallback<T> implements MapReduceConsume<SafeCommandStore, Void>
    {
        final Node node;
        final TxnId txnId;
        final Unseekables<?> someUnseekables;
        final T param;
        final BiConsumer<T, Throwable> callback;

        private CleanupAndCallback(Node node, TxnId txnId, Unseekables<?> someUnseekables, T param, BiConsumer<T, Throwable> callback)
        {
            this.node = node;
            this.txnId = txnId;
            this.someUnseekables = someUnseekables;
            this.param = param;
            this.callback = callback;
        }

        void start()
        {
            PreLoadContext loadContext = contextFor(txnId);
            Unseekables<?> propagateTo = isRoute(someUnseekables) ? castToRoute(someUnseekables).withHomeKey() : someUnseekables;
            node.mapReduceConsumeLocal(loadContext, propagateTo, txnId.epoch(), txnId.epoch(), this);
        }

        @Override
        public Void apply(SafeCommandStore safeStore)
        {
            // we're applying an invalidation, so the record will not be cleaned up until the whole range is truncated
            return apply(safeStore, safeStore.get(txnId, txnId, someUnseekables));
        }

        abstract Void apply(SafeCommandStore safeStore, SafeCommand safeCommand);

        @Override
        public Void reduce(Void o1, Void o2)
        {
            return null;
        }

        @Override
        public void accept(Void result, Throwable failure)
        {
            callback.accept(param, failure);
        }
    }

    static class InvalidateAndCallback<T> extends CleanupAndCallback<T>
    {
        private InvalidateAndCallback(Node node, TxnId txnId, Unseekables<?> someUnseekables, T param, BiConsumer<T, Throwable> callback)
        {
            super(node, txnId, someUnseekables, param, callback);
        }

        public static <T> void locallyInvalidateAndCallback(Node node, TxnId txnId, Unseekables<?> someUnseekables, T param, BiConsumer<T, Throwable> callback)
        {
            new InvalidateAndCallback<T>(node, txnId, someUnseekables, param, callback).start();
        }

        @Override
        Void apply(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            // we're applying an invalidation, so the record will not be cleaned up until the whole range is truncated
            Command command = safeCommand.current();
            // TODO (required): consider the !command.hasBeen(PreCommitted) condition
            Invariants.checkState(!command.hasBeen(PreCommitted) || command.hasBeen(Status.Truncated));
            Commands.commitInvalidate(safeStore, safeCommand, someUnseekables);
            return null;
        }
    }

    static class EraseNonParticipatingAndCallback<T> extends CleanupAndCallback<T>
    {
        private EraseNonParticipatingAndCallback(Node node, TxnId txnId, Unseekables<?> someUnseekables, T param, BiConsumer<T, Throwable> callback)
        {
            super(node, txnId, someUnseekables, param, callback);
        }

        public static <T> void eraseNonParticipatingAndCallback(Node node, TxnId txnId, Unseekables<?> someUnseekables, T param, BiConsumer<T, Throwable> callback)
        {
            if (!Route.isRoute(someUnseekables)) callback.accept(param, null);
            else new EraseNonParticipatingAndCallback<>(node, txnId, someUnseekables, param, callback).start();
        }

        @Override
        Void apply(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            // we're applying an invalidation, so the record will not be cleaned up until the whole range is truncated
            Command command = safeCommand.current();
            if (!command.hasBeen(PreApplied) && safeToCleanup(safeStore, command, Route.castToRoute(someUnseekables), null))
                Commands.setErased(safeStore, safeCommand);
            return null;
        }
    }

    public static Status invalidIfNotAtLeast(SafeCommandStore safeStore, TxnId txnId, Unseekables<?> query)
    {
        if (safeStore.commandStore().globalDurability(txnId).compareTo(Majority) >= 0)
        {
            Unseekables<?> preacceptsWith = isRoute(query) ? castToRoute(query).withHomeKey() : query;
            return safeStore.commandStore().isRejectedIfNotPreAccepted(txnId, preacceptsWith) ? PreAccepted : PreCommitted;
        }

        // TODO (expected, consider): should we force this to be a Route or a Participants?
        if (isRoute(query))
        {
            Participants<?> participants = castToRoute(query).participants();
            // TODO (desired): limit to local participants to avoid O(n2) work across cluster
            if (safeStore.commandStore().durableBefore().isSomeShardDurable(txnId, participants, Majority))
                return PreCommitted;
        }

        if (safeStore.commandStore().durableBefore().isUniversal(txnId, safeStore.ranges().allAt(txnId.epoch())))
            return PreCommitted;

        return Status.NotDefined;
    }

    public static boolean inferInvalidated(CheckStatus.WithQuorum withQuorum, Status invalidIfNotAtLeast, SaveStatus saveStatus, SaveStatus maxSaveStatus)
    {
        if (saveStatus == SaveStatus.Invalidated)
            return true;

        if (withQuorum != HasQuorum)
            return false;

        // should not be possible to reach a quorum without finding the definition unless cleanup is in progress
        Invariants.checkState(saveStatus != SaveStatus.Accepted || maxSaveStatus.phase == Status.Phase.Cleanup);

        if (saveStatus.status.compareTo(invalidIfNotAtLeast) < 0)
            return true;

        return invalidIfNotAtLeast.compareTo(PreAccepted) >= 0 && saveStatus == SaveStatus.AcceptedInvalidate;
    }

    public static boolean safeToCleanup(SafeCommandStore safeStore, Command command, Route<?> fetchedWith, @Nullable Timestamp executeAt)
    {
        Invariants.checkArgument(fetchedWith != null || command.route() != null);
        TxnId txnId = command.txnId();
        if (command.is(Status.NotDefined))
            return !command.saveStatus().isUninitialised();

        if (command.route() != null || fetchedWith.covers(safeStore.ranges().allAt(txnId.epoch())))
        {
            Route<?> route = command.route();
            if (route == null) route = fetchedWith;

            executeAt = command.executeAtIfKnown(Timestamp.nonNullOrMax(executeAt, txnId));
            Ranges coordinateRanges = safeStore.ranges().coordinates(txnId);
            Ranges acceptRanges = executeAt.epoch() == txnId.epoch() ? coordinateRanges : safeStore.ranges().allBetween(txnId, executeAt);
            return !route.participatesIn(coordinateRanges) && !route.participatesIn(acceptRanges);
        }
        return false;
    }
}
