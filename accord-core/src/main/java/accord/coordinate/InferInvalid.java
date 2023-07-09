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
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import static accord.local.PreLoadContext.contextFor;
import static accord.local.Status.Durability.Majority;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.PreCommitted;
import static accord.messages.CheckStatus.WithQuorum.HasQuorum;
import static accord.primitives.Route.castToRoute;
import static accord.primitives.Route.isRoute;

public class InferInvalid
{
    static class InvalidateAndCallback implements MapReduceConsume<SafeCommandStore, Void>
    {
        final Node node;
        final TxnId txnId;
        final Unseekables<?> someUnseekables;
        final Status.Known achieved;
        final BiConsumer<Status.Known, Throwable> callback;

        private InvalidateAndCallback(Node node, TxnId txnId, Unseekables<?> someUnseekables, Status.Known achieved, BiConsumer<Status.Known, Throwable> callback)
        {
            this.node = node;
            this.txnId = txnId;
            this.someUnseekables = someUnseekables;
            this.achieved = achieved;
            this.callback = callback;
        }

        public static void invalidateAndCallback(Node node, TxnId txnId, Unseekables<?> someUnseekables, Status.Known achieved, BiConsumer<Status.Known, Throwable> callback)
        {
            new InvalidateAndCallback(node, txnId, someUnseekables, achieved, callback).start();
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
            SafeCommand safeCommand = safeStore.get(txnId, someUnseekables);
            Command command = safeCommand.current();
            // TODO (required): consider the !command.hasBeen(PreCommitted) condition
            Invariants.checkState(!command.hasBeen(PreCommitted) || command.hasBeen(Status.Truncated));
            Commands.commitInvalidate(safeStore, safeCommand);
            return null;
        }

        @Override
        public Void reduce(Void o1, Void o2)
        {
            return null;
        }

        @Override
        public void accept(Void result, Throwable failure)
        {
            callback.accept(achieved, failure);
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

    public static boolean inferInvalidated(Unseekables<?> fetchedWith, CheckStatus.WithQuorum withQuorum, Status invalidIfNotAtLeast, SaveStatus saveStatus, SaveStatus maxSaveStatus)
    {
        if (saveStatus == SaveStatus.Invalidated)
            return true;

        if (withQuorum != HasQuorum)
            return false;

        // should not be possible to reach a quorum without finding the definition unless cleanup is in progress
        Invariants.checkState(saveStatus != SaveStatus.Accepted || maxSaveStatus.phase == Status.Phase.Cleanup);

        if (saveStatus.status.compareTo(invalidIfNotAtLeast) < 0)
            return true;

        if (invalidIfNotAtLeast.compareTo(PreAccepted) >= 0 && saveStatus == SaveStatus.AcceptedInvalidate)
            return true;

        if (maxSaveStatus.phase != Status.Phase.Cleanup)
            return false;

        // TODO (now): clear the coordination progress log state if we've reached a majority
        if (Route.isRoute(fetchedWith) && !Route.castToRoute(fetchedWith).hasParticipants())
        {
            // TODO (now): it might be better to require that every home shard records a transaction outcome before truncating
            // not safe to invalidate; the home shard may have truncated because the outcome is durable at a majority.
            // however, even if we are only the home and do not participate in the current epoch, we cannot be sure we
            // do not participate in whatever the (unknown) execution epoch is
            return false;
        }

        return true;
    }
}
