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
import accord.primitives.Status;
import accord.primitives.Known;
import accord.local.StoreParticipants;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import static accord.local.PreLoadContext.contextFor;
import static accord.primitives.Status.PreCommitted;
import static accord.primitives.Route.castToRoute;
import static accord.primitives.Route.isRoute;

// TODO (required): dedicated randomised testing of all inferences
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
        IsNotInvalid;

        public static boolean isMax(InvalidIf that)
        {
            return that == IsNotInvalid;
        }

        public InvalidIf atLeast(InvalidIf that)
        {
            return this.compareTo(that) >= 0 ? this : that;
        }

        public boolean inferInvalidWithQuorum(Known known)
        {
            return this == IfUncommitted && !known.isDecided();
        }
    }

    private static abstract class CleanupAndCallback<T> implements MapReduceConsume<SafeCommandStore, Void>
    {
        final Node node;
        final TxnId txnId;
        // TODO (expected): more consistent handling of transactions that only MAY intersect a commandStore
        //  (e.g. dependencies from an earlier epoch that have not yet committed, or commands that are proposed to execute in a later epoch than eventually agreed)
        final EpochSupplier lowEpoch, highEpoch;
        final Participants<?> participants;
        final T param;
        final BiConsumer<T, Throwable> callback;

        private CleanupAndCallback(Node node, TxnId txnId, EpochSupplier lowEpoch, EpochSupplier highEpoch, Participants<?> participants, T param, BiConsumer<T, Throwable> callback)
        {
            this.node = node;
            this.txnId = txnId;
            this.lowEpoch = lowEpoch;
            this.highEpoch = highEpoch;
            this.participants = participants;
            this.param = param;
            this.callback = callback;
        }

        void start()
        {
            PreLoadContext loadContext = contextFor(txnId);
            Unseekables<?> propagateTo = isRoute(participants) ? castToRoute(participants).withHomeKey() : participants;
            node.mapReduceConsumeLocal(loadContext, propagateTo, lowEpoch.epoch(), highEpoch.epoch(), this);
        }

        @Override
        public Void apply(SafeCommandStore safeStore)
        {
            // we're applying an invalidation, so the record will not be cleaned up until the whole range is truncated
            StoreParticipants participants = StoreParticipants.invalidate(safeStore, this.participants, txnId);
            return apply(safeStore, safeStore.get(txnId, participants));
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

    // TODO (required, consider): low and high bounds are correct?
    static class InvalidateAndCallback<T> extends CleanupAndCallback<T>
    {
        private InvalidateAndCallback(Node node, TxnId txnId, EpochSupplier lowEpoch, EpochSupplier highEpoch, Participants<?> someUnseekables, T param, BiConsumer<T, Throwable> callback)
        {
            super(node, txnId, lowEpoch, highEpoch, someUnseekables, param, callback);
        }

        public static <T> void locallyInvalidateAndCallback(Node node, TxnId txnId, EpochSupplier lowEpoch, EpochSupplier highEpoch, Participants<?> someUnseekables, T param, BiConsumer<T, Throwable> callback)
        {
            new InvalidateAndCallback<T>(node, txnId, lowEpoch, highEpoch, someUnseekables, param, callback).start();
        }

        @Override
        Void apply(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            // we're applying an invalidation, so the record will not be cleaned up until the whole range is truncated
            Command command = safeCommand.current();
            // TODO (required, consider): consider the !command.hasBeen(PreCommitted) condition
            Invariants.checkState(!command.hasBeen(PreCommitted) || command.hasBeen(Status.Truncated), "Unexpected status for %s", command);
            Commands.commitInvalidate(safeStore, safeCommand, participants);
            return null;
        }
    }
}
