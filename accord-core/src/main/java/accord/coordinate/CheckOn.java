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
import accord.local.Commands.ApplyOutcome;
import accord.local.Commands.CommitOutcome;
import accord.primitives.*;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import accord.api.RoutingKey;
import accord.local.Node.Id;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import com.google.common.collect.Iterables;

import java.util.Collections;

import static accord.local.PreLoadContext.contextFor;
import static accord.local.SaveStatus.NotWitnessed;
import static accord.local.Status.*;
import static accord.primitives.Routables.Slice.Minimal;

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
    final Route<?> route;

    CheckOn(Node node, Known sufficient, TxnId txnId, Route<?> route, long srcEpoch, long untilLocalEpoch, BiConsumer<? super CheckStatusOkFull, Throwable> callback)
    {
        this(node, sufficient, txnId, route, route.with(route.homeKey()), srcEpoch, untilLocalEpoch, callback);
    }

    CheckOn(Node node, Known sufficient, TxnId txnId, Route<?> route, Unseekables<?, ?> routeWithHomeKey, long srcEpoch, long untilLocalEpoch, BiConsumer<? super CheckStatusOkFull, Throwable> callback)
    {
        // TODO (desired, efficiency): restore behaviour of only collecting info if e.g. Committed or Executed
        super(node, txnId, routeWithHomeKey, srcEpoch, IncludeInfo.All);
        Invariants.checkArgument(routeWithHomeKey.contains(route.homeKey()));
        this.sufficient = sufficient;
        this.route = route;
        this.callback = callback;
        this.untilLocalEpoch = untilLocalEpoch;
    }

    // TODO (required, consider): many callers only need to consult precisely executeAt.epoch remotely
    public static CheckOn checkOn(Known sufficientStatus, Node node, TxnId txnId, Route<?> route, long srcEpoch, long untilLocalEpoch, BiConsumer<? super CheckStatusOkFull, Throwable> callback)
    {
        CheckOn checkOn = new CheckOn(node, sufficientStatus, txnId, route, srcEpoch, untilLocalEpoch, callback);
        checkOn.start();
        return checkOn;
    }

    protected Route<?> route()
    {
        return route;
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        Ranges rangesForNode = topologies().computeRangesForNode(from);
        PartialRoute<?> scope = this.route.slice(rangesForNode);
        return isSufficient(scope, ok);
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return isSufficient(route, ok);
    }

    protected boolean isSufficient(Route<?> scope, CheckStatusOk ok)
    {
        return sufficient.isSatisfiedBy(((CheckStatusOkFull)ok).sufficientFor(scope));
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        Invariants.checkState((success == null) != (failure == null));
        if (failure != null)
        {
            callback.accept(null, failure);
        }
        else
        {
            if (success == Success.Success)
                Invariants.checkState(isSufficient(merged));

            if (merged.saveStatus == NotWitnessed)
                callback.accept(CheckStatusOkFull.NOT_WITNESSED, null);
            else
                new OnDone().start();
        }
    }

    class OnDone implements MapReduceConsume<SafeCommandStore, Void>
    {
        final Route<?> maxRoute;
        final RoutingKey progressKey;
        final CheckStatusOkFull full;
        final Known sufficientFor;
        final PartialTxn partialTxn;
        final PartialDeps partialDeps;

        public OnDone()
        {
            Ranges sliceRanges = node.topology().localRangesForEpochs(txnId.epoch(), untilLocalEpoch);
            Ranges covering = route().sliceCovering(sliceRanges, Minimal);
            Unseekables<?, ?> intersectingKeys = route().slice(covering, Minimal);

            full = (CheckStatusOkFull) merged;
            sufficientFor = full.sufficientFor(intersectingKeys);
            maxRoute = Route.merge((Route)route(), full.route);
            progressKey = node.trySelectProgressKey(txnId, maxRoute);

            PartialTxn partialTxn = null;
            if (sufficientFor.definition.isKnown())
                partialTxn = full.partialTxn.slice(sliceRanges, true).reconstitutePartial(covering);
            this.partialTxn = partialTxn;

            PartialDeps partialDeps = null;
            if (sufficientFor.deps.hasDecidedDeps())
                partialDeps = full.committedDeps.slice(sliceRanges).reconstitutePartial(covering);
            this.partialDeps = partialDeps;
        }

        void start()
        {
            Seekables<?, ?> keys = Keys.EMPTY;
            if (sufficientFor.definition.isKnown())
                keys = partialTxn.keys();

            Iterable<TxnId> txnIds = Collections.singleton(txnId);
            if (sufficientFor.deps.hasDecidedDeps())
                txnIds = Iterables.concat(txnIds, partialDeps.txnIds());

            PreLoadContext loadContext = contextFor(txnIds, keys);
            node.mapReduceConsumeLocal(loadContext, route, txnId.epoch(), untilLocalEpoch, this);
        }

        @Override
        public Void apply(SafeCommandStore safeStore)
        {
            switch (sufficientFor.propagate())
            {
                default: throw new IllegalStateException();
                case Accepted:
                case AcceptedInvalidate:
                    // we never "propagate" accepted statuses as these are essentially votes,
                    // and contribute nothing to our local state machine
                    throw new IllegalStateException("Invalid states to propagate");

                case Invalidated:
                    Commands.commitInvalidate(safeStore, txnId);
                    break;

                case Applied:
                case PreApplied:
                    if (untilLocalEpoch >= full.executeAt.epoch())
                    {
                        confirm(Commands.commit(safeStore, txnId, maxRoute, progressKey, partialTxn, full.executeAt, partialDeps));
                        confirm(Commands.apply(safeStore, txnId, untilLocalEpoch, maxRoute, full.executeAt, partialDeps, full.writes, full.result));
                        break;
                    }

                case Committed:
                case ReadyToExecute:
                    confirm(Commands.commit(safeStore, txnId, maxRoute, progressKey, partialTxn, full.executeAt, partialDeps));
                    break;

                case PreCommitted:
                    Commands.precommit(safeStore, txnId, full.executeAt);
                    if (!sufficientFor.definition.isKnown())
                        break;

                case PreAccepted:
                    if (!safeStore.ranges().at(txnId.epoch()).isEmpty())
                        Commands.preaccept(safeStore, txnId, partialTxn, maxRoute, progressKey);
                    break;

                case NotWitnessed:
                    break;
            }

            RoutingKey homeKey = merged.homeKey;
            if (!merged.durability.isDurable() || homeKey == null)
                return null;

            if (!safeStore.ranges().at(txnId.epoch()).contains(homeKey))
                return null;

            Timestamp executeAt = merged.saveStatus.known.executeAt.hasDecidedExecuteAt() ? merged.executeAt : null;
            Commands.setDurability(safeStore, txnId, merged.durability, homeKey, executeAt);
            safeStore.progressLog().durable(safeStore.command(txnId).current(), null);
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
