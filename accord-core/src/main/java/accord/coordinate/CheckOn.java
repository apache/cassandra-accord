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

import accord.local.*;
import accord.primitives.*;
import accord.utils.MapReduceConsume;
import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.local.Command.ApplyOutcome;
import accord.local.Command.CommitOutcome;
import accord.local.Node.Id;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import com.google.common.collect.Iterables;

import java.util.Collections;

import static accord.local.PreLoadContext.contextFor;
import static accord.local.SaveStatus.NotWitnessed;
import static accord.local.Status.*;
import static accord.local.Status.Known.*;

/**
 * Check on the status of a transaction. Returns early once enough information has been achieved to meet the requested
 * status for the requested {@code route}.
 *
 * If a command is durable (i.e. executed on a majority on all shards) this is sufficient to replicate the command locally.
 */
public class CheckOn extends CheckShards
{
    final BiConsumer<? super CheckStatusOkFull, Throwable> callback;
    /**
     * The epoch until which we want to persist any response for locally
     */
    final Known sufficient;
    final long untilLocalEpoch;
    final AbstractRoute route;

    CheckOn(Node node, Known sufficient, TxnId txnId, AbstractRoute route, long srcEpoch, long untilLocalEpoch, BiConsumer<? super CheckStatusOkFull, Throwable> callback)
    {
        this(node, sufficient, txnId, route, route.with(route.homeKey), srcEpoch, untilLocalEpoch, callback);
    }

    CheckOn(Node node, Known sufficient, TxnId txnId, AbstractRoute route, RoutingKeys routeWithHomeKey, long srcEpoch, long untilLocalEpoch, BiConsumer<? super CheckStatusOkFull, Throwable> callback)
    {
        // TODO (soon): restore behaviour of only collecting info if e.g. Committed or Executed
        super(node, txnId, routeWithHomeKey, srcEpoch, IncludeInfo.All);
        Preconditions.checkArgument(routeWithHomeKey.contains(route.homeKey));
        this.sufficient = sufficient;
        this.route = route;
        this.callback = callback;
        this.untilLocalEpoch = untilLocalEpoch;
    }

    // TODO: many callers only need to consult precisely executeAt.epoch remotely
    public static CheckOn checkOn(Known sufficientStatus, Node node, TxnId txnId, AbstractRoute route, long srcEpoch, long untilLocalEpoch, BiConsumer<? super CheckStatusOkFull, Throwable> callback)
    {
        CheckOn checkOn = new CheckOn(node, sufficientStatus, txnId, route, srcEpoch, untilLocalEpoch, callback);
        checkOn.start();
        return checkOn;
    }

    protected AbstractRoute route()
    {
        return route;
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        KeyRanges rangesForNode = topologies().computeRangesForNode(from);
        PartialRoute scope = this.route.slice(rangesForNode);
        return isSufficient(scope, ok);
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return isSufficient(route, ok);
    }

    protected boolean isSufficient(AbstractRoute scope, CheckStatusOk ok)
    {
        return ((CheckStatusOkFull)ok).sufficientFor(scope).compareTo(sufficient) >= 0;
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        Preconditions.checkState((success == null) != (failure == null));
        if (failure != null)
        {
            callback.accept(null, failure);
        }
        else
        {
            if (success == Success.Success)
                Preconditions.checkState(isSufficient(merged));

            if (merged.saveStatus == NotWitnessed)
                callback.accept(CheckStatusOkFull.NOT_WITNESSED, null);
            else
                new OnDone().start();
        }
    }

    class OnDone implements MapReduceConsume<SafeCommandStore, Void>
    {
        final AbstractRoute maxRoute;
        final RoutingKey progressKey;
        final CheckStatusOkFull full;
        final Known sufficientTo;
        final PartialTxn partialTxn;
        final PartialDeps partialDeps;

        public OnDone()
        {
            KeyRanges localRanges = node.topology().localRangesForEpochs(txnId.epoch, untilLocalEpoch);
            PartialRoute selfRoute = route().slice(localRanges);
            full = (CheckStatusOkFull) merged;
            sufficientTo = full.sufficientFor(selfRoute);
            maxRoute = Route.merge(route(), full.route);
            progressKey = node.trySelectProgressKey(txnId, maxRoute);

            PartialTxn partialTxn = null;
            if (sufficientTo.hasTxn)
                partialTxn = full.partialTxn.slice(localRanges, true).reconstitutePartial(selfRoute);
            this.partialTxn = partialTxn;

            PartialDeps partialDeps = null;
            if (sufficientTo.hasDeps)
                partialDeps = full.committedDeps.slice(localRanges).reconstitutePartial(selfRoute);
            this.partialDeps = partialDeps;
        }

        void start()
        {
            Keys keys = Keys.EMPTY;
            if (sufficientTo.hasTxn)
                keys = partialTxn.keys();

            Iterable<TxnId> txnIds = Collections.singleton(txnId);
            if (sufficientTo.hasDeps)
                txnIds = Iterables.concat(txnIds, partialDeps.txnIds());

            PreLoadContext loadContext = contextFor(txnIds, keys);
            node.mapReduceConsumeLocal(loadContext, route, txnId.epoch, untilLocalEpoch, this);
        }

        @Override
        public Void apply(SafeCommandStore safeStore)
        {
            Command command = safeStore.command(txnId);
            switch (sufficientTo)
            {
                default: throw new IllegalStateException();
                case Invalidation:
                    command.commitInvalidate(safeStore);
                    break;
                case Outcome:
                    if (untilLocalEpoch >= full.executeAt.epoch)
                    {
                        confirm(command.commit(safeStore, maxRoute, progressKey, partialTxn, full.executeAt, partialDeps));
                        confirm(command.apply(safeStore, untilLocalEpoch, maxRoute, full.executeAt, partialDeps, full.writes, full.result));
                        break;
                    }
                case ExecutionOrder:
                    confirm(command.commit(safeStore, maxRoute, progressKey, partialTxn, full.executeAt, partialDeps));
                    break;
                case Definition:
                    command.preaccept(safeStore, partialTxn, maxRoute, progressKey);
                case Nothing:
                    break;
            }

            RoutingKey homeKey = merged.homeKey;
            if (!merged.durability.isDurable() || homeKey == null)
                return null;

            if (!safeStore.ranges().owns(txnId.epoch, homeKey))
                return null;

            if (!safeStore.commandStore().hashIntersects(homeKey))
                return null;

            Timestamp executeAt = merged.saveStatus.isAtLeast(Phase.Commit) ? merged.executeAt : null;
            command.setDurability(safeStore, merged.durability, homeKey, executeAt);
            safeStore.progressLog().durable(command, null);
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
            callback.accept(failure != null ? null : full, failure);
        }
    }

    private static void confirm(CommitOutcome outcome)
    {
        switch (outcome)
        {
            default: throw new IllegalStateException();
            case Redundant:
            case Success:
                return;
            case Insufficient: throw new IllegalStateException("Should have enough information");
        }
    }

    private static void confirm(ApplyOutcome outcome)
    {
        switch (outcome)
        {
            default: throw new IllegalStateException();
            case Redundant:
            case Success:
                return;
            case Insufficient: throw new IllegalStateException("Should have enough information");
        }
    }
}
