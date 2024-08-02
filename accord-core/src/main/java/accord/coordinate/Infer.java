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

import accord.local.Cleanup;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.RedundantStatus;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.local.Status.Known;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import static accord.coordinate.Infer.InvalidIf.IfPreempted;
import static accord.coordinate.Infer.InvalidIf.IfQuorum;
import static accord.coordinate.Infer.InvalidIf.NotKnown;
import static accord.coordinate.Infer.InvalidIfNot.IfUndecided;
import static accord.coordinate.Infer.InvalidIfNot.IfUnknown;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.Status.Durability.Majority;
import static accord.local.Status.PreApplied;
import static accord.local.Status.PreCommitted;
import static accord.primitives.Route.castToRoute;
import static accord.primitives.Route.isRoute;

public class Infer
{
    public enum InvalidIfNot
    {
        /**
         * There is no information to suggest the command is invalid
         */
        NotKnownToBeInvalid(NotKnown, NotKnown),

        /**
         * If the command has not been preaccepted on a majority of any shard and
         * the command's original coordinator had been preempted prior to all responses we rely upon
         * (so we are not racing with it)
         */
        IfUnknownAndPreempted(IfPreempted, NotKnown),

        /**
         * If the command has not had its execution timestamp agreed on any shard and
         * the command's original coordinator had been preempted prior to all responses we rely upon
         * (so we are not racing with it)
         */
        IfUndecidedAndPreempted(IfPreempted, IfPreempted),

        /**
         * If the command has not been preaccepted on a majority of any shard
         */
        IfUnknown(IfQuorum, NotKnown),

        /**
         * If the command has not had its execution timestamp agreed on any shard and
         * the command's original coordinator had been preempted prior to all responses we rely upon
         * (so we are not racing with it)
         */
        IfUnknownOrIfUndecidedAndPreempted(IfQuorum, IfPreempted),

        /**
         * If the command has not had its execution timestamp agreed on any shard
         */
        IfUndecided(IfQuorum, IfQuorum);

        final InvalidIf unknown, undecided;
        private static final InvalidIfNot[] LOOKUP;
        private static final int invalidIfs = InvalidIf.values().length;

        static
        {
            LOOKUP = new InvalidIfNot[invalidIfs * invalidIfs];
            InvalidIfNot[] invalidIfNot = InvalidIfNot.values();
            for (InvalidIfNot ifNot : invalidIfNot)
                LOOKUP[ifNot.unknown.ordinal() * invalidIfs + ifNot.undecided.ordinal()] = ifNot;
        }

        InvalidIfNot(InvalidIf unknown, InvalidIf undecided)
        {
            this.unknown = unknown;
            this.undecided = undecided;
        }

        public static boolean isMax(InvalidIfNot that)
        {
            return that == IfUndecided;
        }

        public InvalidIfNot atLeast(InvalidIfNot that)
        {
            return lookup(atLeast(this.unknown, that.unknown), atLeast(this.undecided, that.undecided));
        }

        public InvalidIfNot reduce(InvalidIfNot that)
        {
            return lookup(reduce(this.unknown, that.unknown), reduce(this.undecided, that.undecided));
        }

        private InvalidIfNot lookup(InvalidIf unknown, InvalidIf undecided)
        {
            return LOOKUP[unknown.ordinal() * invalidIfs + undecided.ordinal()];
        }

        private static InvalidIf atLeast(InvalidIf a, InvalidIf b)
        {
            if (a == b) return a;
            return IfPreempted;
        }

        private static InvalidIf reduce(InvalidIf a, InvalidIf b)
        {
            return a.compareTo(b) <= 0 ? a : b;
        }

        public boolean inferInvalidWithQuorum(IsPreempted isPreempted, Known minKnown)
        {
            return inferInvalidWithQuorum(undecided, isPreempted, !minKnown.isDecided())
                   || inferInvalidWithQuorum(unknown, isPreempted, !minKnown.hasDefinitionBeenKnown());
        }

        private static boolean inferInvalidWithQuorum(InvalidIf invalidIf, IsPreempted isPreempted, boolean hasCondition)
        {
            if (!hasCondition)
                return false;

            switch (invalidIf)
            {
                default: throw new AssertionError("Unhandled InvalidIf: " + invalidIf);
                case NotKnown: break;
                case IfQuorum: return true;
                case IfPreempted:
                    if (isPreempted == IsPreempted.Preempted)
                        return true;
            }

            return false;
        }
    }

    enum InvalidIf
    {
        NotKnown,

        /**
         * We did not have a quorum of responses with the associated lower bound, so we require that the command has been preempted at a quorum
         */
        IfPreempted,

        /**
         * If we obtain a quorum of responses with the associated lower bound, we can infer the command is invalidated if it has not been witnessed at the lower bound
         */
        IfQuorum
    }

    // only valid with a quorum of responses
    public enum IsPreempted
    {
        NotPreempted, MaybePreempted, Preempted;

        public IsPreempted merge(IsPreempted that)
        {
            if (this == that) return this;
            return MaybePreempted;
        }

        public IsPreempted validForBoth(IsPreempted that)
        {
            return this.compareTo(that) <= 0 ? this : that;
        }
    }

    private static abstract class CleanupAndCallback<T> implements MapReduceConsume<SafeCommandStore, Void>
    {
        final Node node;
        final TxnId txnId;
        // TODO (expected): more consistent handling of transactions that only MAY intersect a commandStore
        //  (e.g. dependencies from an earlier epoch that have not yet committed, or commands that are proposed to execute in a later epoch than eventually agreed)
        final EpochSupplier untilEpoch;
        final Unseekables<?> someUnseekables;
        final T param;
        final BiConsumer<T, Throwable> callback;

        private CleanupAndCallback(Node node, TxnId txnId, EpochSupplier untilEpoch, Unseekables<?> someUnseekables, T param, BiConsumer<T, Throwable> callback)
        {
            this.node = node;
            this.txnId = txnId;
            this.untilEpoch = untilEpoch;
            this.someUnseekables = someUnseekables;
            this.param = param;
            this.callback = callback;
        }

        void start()
        {
            PreLoadContext loadContext = contextFor(txnId);
            Unseekables<?> propagateTo = isRoute(someUnseekables) ? castToRoute(someUnseekables).withHomeKey() : someUnseekables;
            node.mapReduceConsumeLocal(loadContext, propagateTo, txnId.epoch(), untilEpoch.epoch(), this);
        }

        @Override
        public Void apply(SafeCommandStore safeStore)
        {
            // we're applying an invalidation, so the record will not be cleaned up until the whole range is truncated
            return apply(safeStore, safeStore.get(txnId, untilEpoch, someUnseekables));
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
        private InvalidateAndCallback(Node node, TxnId txnId, EpochSupplier untilEpoch, Unseekables<?> someUnseekables, T param, BiConsumer<T, Throwable> callback)
        {
            super(node, txnId, untilEpoch, someUnseekables, param, callback);
        }

        public static <T> void locallyInvalidateAndCallback(Node node, TxnId txnId, EpochSupplier untilEpoch, Unseekables<?> someUnseekables, T param, BiConsumer<T, Throwable> callback)
        {
            new InvalidateAndCallback<T>(node, txnId, untilEpoch, someUnseekables, param, callback).start();
        }

        @Override
        Void apply(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            // we're applying an invalidation, so the record will not be cleaned up until the whole range is truncated
            Command command = safeCommand.current();
            // TODO (required): consider the !command.hasBeen(PreCommitted) condition
            Invariants.checkState(!command.hasBeen(PreCommitted) || command.hasBeen(Status.Truncated), "Unexpected status for %s", command);
            Commands.commitInvalidate(safeStore, safeCommand, someUnseekables);
            return null;
        }
    }

    /**
     * Erase if it is safe to do so, i.e. if Infer.safeToCleanup permits it.
     */
    static class SafeEraseAndCallback<T> extends CleanupAndCallback<T>
    {
        private SafeEraseAndCallback(Node node, TxnId txnId, EpochSupplier untilEpoch, Unseekables<?> someUnseekables, T param, BiConsumer<T, Throwable> callback)
        {
            super(node, txnId, untilEpoch, someUnseekables, param, callback);
        }

        public static <T> void safeEraseAndCallback(Node node, TxnId txnId, EpochSupplier untilEpoch, Unseekables<?> someUnseekables, T param, BiConsumer<T, Throwable> callback)
        {
            if (!Route.isRoute(someUnseekables)) callback.accept(param, null);
            else new SafeEraseAndCallback<>(node, txnId, untilEpoch, someUnseekables, param, callback).start();
        }

        @Override
        Void apply(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            Command command = safeCommand.current();
            // TODO (required): introduce a special form of Erased where we do not imply the phase is "Cleanup"
            if (!command.hasBeen(PreApplied) && safeToCleanup(safeStore, command, Route.castToRoute(someUnseekables), null))
                Commands.setErased(safeStore, safeCommand);
            return null;
        }
    }

    public static InvalidIfNot invalidIfNot(SafeCommandStore safeStore, TxnId txnId, Unseekables<?> query)
    {
        if (safeStore.commandStore().globalDurability(txnId).compareTo(Majority) >= 0)
        {
            Unseekables<?> preacceptsWith = isRoute(query) ? castToRoute(query).withHomeKey() : query;
            return safeStore.commandStore().isRejectedIfNotPreAccepted(txnId, preacceptsWith) ? IfUnknown : IfUndecided;
        }

        // TODO (expected, consider): should we force this to be a Route or a Participants?
        if (isRoute(query))
        {
            Participants<?> participants = castToRoute(query).participants();
            // TODO (desired): limit to local participants to avoid O(n2) work across cluster
            if (safeStore.commandStore().durableBefore().isSomeShardDurable(txnId, participants, Majority))
                return IfUndecided;
        }

        if (Cleanup.isSafeToCleanup(safeStore.commandStore().durableBefore(), txnId, safeStore.ranges().allAt(txnId.epoch())))
            return IfUndecided;

        return InvalidIfNot.NotKnownToBeInvalid;
    }

    public static boolean safeToCleanup(SafeCommandStore safeStore, Command command, Route<?> fetchedWith, @Nullable Timestamp executeAt)
    {
        Invariants.checkArgument(fetchedWith != null || command.route() != null);
        TxnId txnId = command.txnId();
        if (command.route() == null || !fetchedWith.covers(safeStore.ranges().allAt(txnId.epoch())))
            return false;

        Route<?> route = command.route();
        if (route == null) route = fetchedWith;

        // TODO (required): is it safe to cleanup without an executeAt? We don't know for sure which ranges it might participate in.
        //    We can infer the upper bound of execution by the "execution" of any ExclusiveSyncPoint used to infer the invalidation.
        //    We should begin evaluating and tracking this.
        executeAt = command.executeAtIfKnown(Timestamp.nonNullOrMax(executeAt, txnId));
        Ranges coordinateRanges = safeStore.ranges().coordinates(txnId);
        Ranges acceptRanges = executeAt.epoch() == txnId.epoch() ? coordinateRanges : safeStore.ranges().allBetween(txnId, executeAt);
        if (!route.participatesIn(coordinateRanges) && !route.participatesIn(acceptRanges))
            return true;

        RedundantStatus status = safeStore.commandStore().redundantBefore().status(txnId, executeAt, route.participants());
        switch (status)
        {
            default: throw new AssertionError("Unhandled RedundantStatus: " + status);
            case NOT_OWNED:
            case LIVE:
            case REDUNDANT_PRE_BOOTSTRAP_OR_STALE:
            case PARTIALLY_PRE_BOOTSTRAP_OR_STALE:
                return false;
            case LOCALLY_REDUNDANT:
            case SHARD_REDUNDANT:
                Invariants.checkState(!command.hasBeen(PreCommitted));
            case PRE_BOOTSTRAP_OR_STALE:
                return true;
        }
    }
}
