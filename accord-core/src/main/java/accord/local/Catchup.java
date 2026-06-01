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

package accord.local;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.coordinate.FetchDurableBefore;
import accord.local.durability.DurabilityService.SyncLocal;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.EpochReady;
import accord.utils.Reduce;
import accord.utils.ReducingRangeMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.local.ExecutionContext.Empty;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.durability.DurabilityService.SyncReadable.UnknownReadable;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Functions.alwaysFalse;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class Catchup
{
    private static final Logger logger = LoggerFactory.getLogger(Catchup.class);
    static Ranges removeRedundant(Ranges waitingOn, ReducingRangeMap<TxnId> target, RedundantBefore redundantBefore, BiConsumer<Ranges, Ranges> removedAndRemaining)
    {
        return target.foldlWithBounds(waitingOn, (TxnId targetTxnId, Ranges ranges, RoutingKey targetStart, RoutingKey targetEnd) -> {
            if (targetTxnId == null)
                return ranges;

            Ranges targetRanges = Ranges.of(targetStart.rangeFactory().newRange(targetStart, targetEnd));
            return redundantBefore.foldlWithBounds(targetRanges, (RedundantBefore.Bounds bounds, Ranges rs, RoutingKey boundStart, RoutingKey boundEnd) -> {
                TxnId locallyRedundant = bounds.maxBound(LOCALLY_REDUNDANT);
                if (locallyRedundant.compareTo(targetTxnId) >= 0)
                {
                    Ranges boundRanges = Ranges.of(Range.of(boundStart, boundEnd));
                    Ranges caughtUp = rs.slice(boundRanges, Minimal);
                    if (!caughtUp.isEmpty())
                    {
                        rs = rs.without(boundRanges);
                        removedAndRemaining.accept(caughtUp, rs);
                    }
                }
                return rs;
            }, ranges, alwaysFalse());
        }, waitingOn, alwaysFalse());
    }

    public static class Unsuccessful
    {
        public final Ranges ranges;
        public Unsuccessful(Ranges ranges)
        {
            this.ranges = ranges;
        }

        static Unsuccessful merge(Unsuccessful a, Unsuccessful b)
        {
            if (a == null || b == null)
                return a == null ? b : a;
            return new Unsuccessful(a.ranges.with(b.ranges));
        }
    }

    public static AsyncResult<Unsuccessful> catchup(Node node, long deadline, TimeUnit units)
    {
        return catchup(node, deadline, units, Arrays.asList(node.commandStores().all()));
    }

    /**
     * Catch up with our peers by agreeing a <em>new</em> sync point over the ranges we own and waiting until we have
     * applied it locally: by construction, when it applies locally every transaction that executes before it has applied
     * locally too, so we are caught up as of now.
     *
     * We previously adopted a sync point our peers had *already* agreed and applied, learned from their durability
     * watermarks. That bound is only as fresh as the durability cycle (minutes), so catchup could report success while
     * we were still missing every transaction since - which then had to be recovered one at a time by the progress log,
     * far more expensively, and on a busy key possibly not at all. Measured before this change: catchup "finished" in
     * 14s with ~20,000 transactions still outstanding on the node.
     *
     * Note this is only safe because plain catchup does not mark anything unready or discard any data - waiting for a
     * newer bound is strictly more conservative. {@link #rebootstrapIfBehind}, which does affect readiness, continues to
     * use the quorum-durable bound.
     */
    public static AsyncResult<Unsuccessful> catchup(Node node, long deadline, TimeUnit units, List<CommandStore> commandStores)
    {
        List<AsyncChain<Unsuccessful>> chains = new ArrayList<>(commandStores.size());
        for (CommandStore commandStore : commandStores)
            chains.add(catchup(node, commandStore, deadline, units));

        if (chains.isEmpty())
            return AsyncResults.success(null);

        return AsyncChains.reduce(chains, Unsuccessful::merge).beginAsResult();
    }

    private static AsyncChain<Unsuccessful> catchup(Node node, CommandStore commandStore, long deadline, TimeUnit units)
    {
        return commandStore.chain((Empty)() -> "Catchup", safeStore -> {
            Ranges ranges = safeStore.ranges().all().mergeTouching();
            return safeStore.redundantBefore().removeLostOrStale(ranges);
        }).flatMap(ranges -> {
            if (ranges.isEmpty())
            {
                logger.info("{}: nothing to catch up", commandStore);
                return AsyncChains.success(null);
            }

            long timeoutMicros = Math.max(1, units.toMicros(deadline) - node.elapsed(MICROSECONDS));
            logger.info("{}: catching-up {} by agreeing a new sync point", commandStore, ranges);
            return node.durability()
                       .sync("Catchup " + commandStore, null, ranges, SyncLocal.Self, SyncRemote.NoRemote, UnknownReadable, timeoutMicros, MICROSECONDS)
                       .chain()
                       .map(ignore -> {
                           logger.info("{}: caught-up {}", commandStore, ranges);
                           return (Unsuccessful) null;
                       })
                       .recover(failure -> {
                           logger.info("{}: could not catch up {}: {}", commandStore, ranges, failure.toString());
                           return AsyncChains.success(new Unsuccessful(ranges));
                       });
        });
    }

    private static EpochReady rebootstrapIfBehind(Node node, SafeCommandStore safeStore, DurableBefore durableBefore)
    {
        RedundantBefore redundantBefore = safeStore.redundantBefore();
        Ranges catchUp;
        {
            Ranges tmp = safeStore.ranges().all().slice(durableBefore.ranges(Objects::nonNull), Minimal).mergeTouching();
            tmp = redundantBefore.removeLostOrStale(tmp);
            catchUp = Catchup.removeRedundant(tmp, quorumDurableTarget(durableBefore), redundantBefore, (caughtUp, remaining) -> {});
        }

        if (catchUp.isEmpty())
        {
            logger.info("No ranges to rebootstrap");
            return EpochReady.done(node.epoch());
        }

        logger.info("Rebootstrapping {} with quorums", catchUp);
        return safeStore.commandStore().rebootstrap(node, catchUp, BootstrapReason.CATCHUP);
    }

    private static ReducingRangeMap<TxnId> quorumDurableTarget(DurableBefore durableBefore)
    {
        ReducingRangeMap<TxnId> target = new ReducingRangeMap<>();
        for (DurableBefore.Entry entry : durableBefore)
        {
            if (entry == null)
                continue;
            ReducingRangeMap<TxnId> quorumDurable = ReducingRangeMap.create(Ranges.of(entry.toPlainRange()), entry.quorum.withoutNonIdentityFlags());
            target = ReducingRangeMap.merge(target, quorumDurable, Timestamp::nonNullOrMax);
        }
        return target;
    }

    public static AsyncChain<?> rebootstrapIfBehind(Node node)
    {
        return rebootstrapIfBehind(node, Arrays.asList(node.commandStores().all()));
    }

    public static AsyncChain<?> rebootstrapIfBehind(Node node, List<CommandStore> commandStores)
    {
        return FetchDurableBefore.catchup(node).flatMap(durableBefore -> {
            List<AsyncChain<?>> chains = new ArrayList<>();
            for (CommandStore commandStore : commandStores)
            {
                chains.add(commandStore.chain((Empty)() -> "Catchup", safeStore -> {
                    return rebootstrapIfBehind(node, safeStore, durableBefore);
                }).flatMapResult(i -> i.reads));  // CATCHUP does not interfere with existing coordination guarantees, so coordinate is not needed here
            }
            return AsyncChains.reduce(chains, Reduce.toNull());
        });
    }
}
