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

import accord.api.Tracing;
import accord.local.Command;
import accord.local.CommandStores.StoreFinder;
import accord.local.CommandStores.StoreSelector;
import accord.local.Commands;
import accord.local.MapReduceConsumeCommandStores;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.Status;
import accord.primitives.Known;
import accord.local.StoreParticipants;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.primitives.Status.PreCommitted;

// TODO (testing): dedicated randomised testing of all inferences
public class Infer
{
    public enum InvalidIf
    {
        /**
         * There is no information to suggest the command is invalid
         */
        NotKnownToBeInvalid,

        /**
         * If the command has not had its execution timestamp committed on any shard.
         */
        IfUncommitted,

        /**
         * This command is known to be decided, so it is a logic bug if it is inferred elsewhere to be invalid.
         */
        IsNotInvalid,

        /**
         * This command is known to be invalid.
         */
        IsInvalid;

        public InvalidIf atLeast(InvalidIf that)
        {
            if (this == that)
                return this;
            Invariants.require(this.compareTo(IsNotInvalid) < 0 || that.compareTo(IsNotInvalid) < 0);
            return this.compareTo(that) >= 0 ? this : that;
        }

        public InvalidIf inferWithQuorum(Known minMaxKnown, Known maxKnown)
        {
            if (this != IfUncommitted)
                return this;

            if (minMaxKnown.isDecided())
            {
                // could be invalidated or committed, not important as should be derivable from minKnown
                // if we can't, it's erased everywhere, so we already know the outcome else we are stale
                return NotKnownToBeInvalid;
            }

            if (maxKnown.executeAt().hasDecision())
            {
                // could be invalidated or committed, but we definitely know which so we don't need to infer anything
                return NotKnownToBeInvalid;
            }

            return IfUncommitted;
        }

        public InvalidIf inferWithNewQuorum(InvalidIf previouslyKnownToBeInvalidIf, Known minMaxKnownByNewQuorum)
        {
            if (previouslyKnownToBeInvalidIf != IfUncommitted || minMaxKnownByNewQuorum.isDecided())
                return this.atLeast(previouslyKnownToBeInvalidIf);

            return IsInvalid;
        }
    }

    // TODO (required): audit all use cases
    private static abstract class CleanupAndCallback<T> extends MapReduceConsumeCommandStores<Participants<?>, Void>
    {
        final Node node;
        final TxnId txnId;
        // TODO (expected): more consistent handling of transactions that only MAY intersect a commandStore
        //  (e.g. dependencies from an earlier epoch that have not yet committed, or commands that are proposed to execute in a later epoch than eventually agreed)
        final StoreSelector reportTo;
        final T param;
        final BiConsumer<T, Throwable> callback;
        final @Nullable Tracing tracing;

        private CleanupAndCallback(Node node, TxnId txnId, StoreSelector reportTo, Participants<?> participants, T param, BiConsumer<T, Throwable> callback, @Nullable Tracing tracing)
        {
            super(participants);
            this.node = node;
            this.txnId = txnId;
            this.reportTo = reportTo;
            this.param = param;
            this.callback = callback;
            this.tracing = tracing;
        }

        void start()
        {
            node.commandStores().mapReduceConsume(reportTo.refine(txnId, null, scope), this);
        }

        @Override
        public Void applyInternal(SafeCommandStore safeStore)
        {
            // we're applying an invalidation, so the record will not be cleaned up until the whole range is truncated
            StoreParticipants participants = StoreParticipants.notAccept(safeStore, scope, txnId);
            return apply(safeStore, safeStore.get(txnId, participants));
        }

        @Override
        protected Void refuseInternal(SafeCommandStore safeStore)
        {
            return null;
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

        @Nullable
        @Override
        public TxnId primaryTxnId()
        {
            return txnId;
        }
    }

    static class InvalidateAndCallback<T> extends CleanupAndCallback<T>
    {
        private InvalidateAndCallback(Node node, TxnId txnId, StoreSelector selector, Participants<?> participants, T param, BiConsumer<T, Throwable> callback, @Nullable Tracing tracing)
        {
            super(node, txnId, selector, participants, param, callback, tracing);
        }

        public static <T> void locallyInvalidateAndCallback(Node node, TxnId txnId, long lowEpoch, long highEpoch, Participants<?> participants, T param, BiConsumer<T, Throwable> callback, @Nullable Tracing tracing)
        {
            new InvalidateAndCallback<>(node, txnId, StoreFinder.selector(participants, lowEpoch, highEpoch), participants, param, callback, tracing).start();
        }

        public static <T> void locallyInvalidateAndCallback(Node node, TxnId txnId, StoreSelector selector, Participants<?> participants, T param, BiConsumer<T, Throwable> callback, @Nullable Tracing tracing)
        {
            new InvalidateAndCallback<>(node, txnId, selector, participants, param, callback, tracing).start();
        }

        @Override
        Void apply(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            // we're applying an invalidation, so the record will not be cleaned up until the whole range is truncated
            Command command = safeCommand.current();
            if (tracing != null)
                tracing.trace(safeStore.commandStore(), "Invalidating (from %s)", command.saveStatus());
            Invariants.require(!command.hasBeen(PreCommitted) || command.hasBeen(Status.Truncated), "Unexpected status for %s", command);
            Commands.commitInvalidate(safeStore, safeCommand, scope);
            return null;
        }

        @Override
        public String reason()
        {
            return "Invalidate";
        }
    }
}
