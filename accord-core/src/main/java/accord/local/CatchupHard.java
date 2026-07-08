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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.DataStore.FetchRanges;
import accord.coordinate.FetchDurableBefore;
import accord.local.ExecutionContext.Empty;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Reduce;
import accord.utils.ReducingRangeMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.local.RedundantStatus.Property.LOG_UNAVAILABLE;
import static accord.local.RedundantStatus.SomeStatus.LOG_UNAVAILABLE_ONLY;
import static accord.primitives.Routables.Slice.Minimal;

/**
 * Catch up this data store to the point of any quorum
 */
public class CatchupHard
{
    private static final Logger logger = LoggerFactory.getLogger(CatchupHard.class);

    static class CatchupAttempt extends FetchAttempt
    {
        final ReducingRangeMap<TxnId> bounds;
        final SettableResult<Void> done;
        final CommandStore commandStore;
        CatchupAttempt(Ranges ranges, int attempt, ReducingRangeMap<TxnId> bounds, SettableResult<Void> done, CommandStore commandStore)
        {
            super(ranges, attempt);
            this.bounds = bounds;
            this.done = done;
            this.commandStore = commandStore;
        }

        @Override
        protected AsyncResult<Void> markSafeToRead(Ranges ranges, Timestamp safeToReadAt)
        {
            List<AsyncResult<Void>> results = bounds.foldlWithBounds(ranges, (bound, rs, start, end) -> {
                Timestamp safe = safeToReadAt.compareTo(bound) > 0 ? safeToReadAt : bound;
                rs.add(commandStore.markSafeToRead(bound, safe, Ranges.of(Range.of(start, end))));
                return rs;
            }, new ArrayList<>());
            return AsyncResults.reduce(results, Reduce.toNull());
        }

        @Override
        protected void markUnsafeToRead(Ranges ranges)
        {
            commandStore.markUnsafeToRead(ranges);
        }

        @Override
        protected void complete(Ranges missing)
        {
            if (missing.isEmpty()) done.trySuccess(null);
            else done.tryFailure(new RuntimeException(missing + " could not be caught-up", fetchOutcome));
        }
    }

    private static AsyncResult<Void> start(Node node, SafeCommandStore safeStore, DurableBefore durableBefore)
    {
        RedundantBefore redundantBefore = safeStore.redundantBefore();
        Ranges catchUp;
        {
            Ranges tmp = safeStore.ranges().all().slice(durableBefore.ranges(Objects::nonNull), Minimal).mergeTouching();
            tmp = redundantBefore.removeLostOrStale(tmp);
            catchUp = Catchup.removeRedundant(tmp, durableBefore, redundantBefore, (caughtUp, remaining) -> {});
        }

        if (catchUp.isEmpty())
        {
            logger.info("No ranges to catchup with quorums");
            return AsyncResults.success(null);
        }

        logger.info("Catching up {} with quorums", catchUp);
        RedundantBefore upsert = durableBefore.foldl(catchUp, (e, builder, p1, p2) -> {
            Ranges ranges = Ranges.of(e.toPlainRange()).slice(catchUp, Minimal);
            for (Range range : ranges)
                builder.append(range.start(), range.end(), RedundantBefore.Bounds.create(range, e.quorum, LOG_UNAVAILABLE_ONLY, null));
            return builder;
        }, new RedundantBefore.Builder(catchUp.size()), null, null).build();

        ReducingRangeMap<TxnId> bounds = upsert.map(b -> b.maxBound(LOG_UNAVAILABLE), TxnId[]::new);

        Map<TxnId, Ranges> rangesByBound = bounds.foldlWithBounds((t, map, start, end) -> {
            map.merge(t, Ranges.of(Range.of(start, end)), Ranges::with);
            return map;
        }, new HashMap<>());

        safeStore.commandStore().markUnsafeToRead(catchUp);
        safeStore.upsertRedundantBefore(upsert);

        AsyncResults.SettableResult<Void> synced = new SettableResult<>();
        FetchRanges fetch = new CatchupAttempt(catchUp, 1, bounds, synced, safeStore.commandStore());
        safeStore.dataStore().sync(node, safeStore, rangesByBound, fetch);

        CommandStore commandStore = safeStore.commandStore();
        return synced.flatMap(ignore -> maybeExecuteBounds(commandStore, rangesByBound.keySet()).beginAsResult());
    }

    private static AsyncChain<Void> maybeExecuteBounds(CommandStore commandStore, Collection<TxnId> bounds)
    {
        logger.info("Refreshing and maybe executing hard catchup bounds {}", bounds);
        List<AsyncChain<Void>> chains = new ArrayList<>(bounds.size());
        for (TxnId txnId : bounds)
        {
            chains.add(commandStore.chain(ExecutionContext.unsequenced(txnId, "Mark CatchupHard bounds applied"), safeStore -> {
                SafeCommand safeCommand = safeStore.get(txnId);
                Command command = safeCommand.current();
                if (command.saveStatus() == SaveStatus.PreApplied)
                {
                    Commands.removeRedundantDependencies(safeStore, safeCommand);
                    Commands.maybeExecute(safeStore, safeCommand, false, true);
                }
            }));
        }
        return AsyncChains.reduce(chains, Reduce.toNull());
    }

    public static AsyncChain<Void> catchup(Node node)
    {
        return catchup(node, Arrays.asList(node.commandStores().all()));
    }

    public static AsyncChain<Void> catchup(Node node, List<CommandStore> commandStores)
    {
        return FetchDurableBefore.catchup(node).flatMap(durableBefore -> {
            List<AsyncChain<Void>> chains = new ArrayList<>();
            for (CommandStore commandStore : commandStores)
            {
                chains.add(commandStore.chain((Empty)() -> "Catchup", safeStore -> {
                    return start(node, safeStore, durableBefore);
                }).flatMapResult(i -> i));
            }
            return AsyncChains.reduce(chains, Reduce.toNull());
        });
    }
}
