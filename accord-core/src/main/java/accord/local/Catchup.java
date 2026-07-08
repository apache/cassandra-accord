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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.coordinate.FetchDurableBefore;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.TxnId;
import accord.utils.Reduce;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Functions.alwaysFalse;

public class Catchup
{
    private static final Logger logger = LoggerFactory.getLogger(Catchup.class);
    static class CommandStoreListener extends SettableResult<Void> implements SyncPointListener
    {
        final DurableBefore durableBefore;
        Ranges waitingOn;

        CommandStoreListener(DurableBefore durableBefore)
        {
            this.durableBefore = durableBefore;
        }

        boolean register(SafeCommandStore safeStore)
        {
            waitingOn = safeStore.ranges().all().slice(durableBefore.ranges(Objects::nonNull), Minimal).mergeTouching();
            logger.debug("{}: Registering listener on {}, filtering by {}", safeStore.commandStore(), waitingOn, safeStore.redundantBefore().map(b -> b == null ? null : b.maxBound(LOCALLY_APPLIED), TxnId[]::new));
            updateWaitingOn(safeStore);

            if (!waitingOn.isEmpty())
            {
                logger.info("{}: catching-up {}", safeStore.commandStore(), durableBefore.foldl(waitingOn, (entry, sb, p1, p2) -> {
                    if (sb.length() > 0)
                        sb.append(", ");
                    if (entry == null)
                    {
                        sb.append("??");
                    }
                    else
                    {
                        TxnId txnId = entry.quorum.withoutNonIdentityFlags();
                        Range range = entry.toPlainRange();
                        markWaiting(safeStore, txnId, range);
                        sb.append(range).append(": ").append(entry.quorum);
                    }
                    return sb;
                }, new StringBuilder(), null, null));
                safeStore.register(this);
                return true;
            }
            else
            {
                done(safeStore);
                return false;
            }
        }

        private static void markWaiting(SafeCommandStore safeStore, TxnId txnId, Range range)
        {
            //noinspection DataFlowIssue
            safeStore = safeStore;
            ExecutionContext ctx = ExecutionContext.unsequenced(txnId, "Catchup");
            if (safeStore.canExecuteWith(ctx)) markWaiting(safeStore, safeStore.get(txnId), range);
            else safeStore.commandStore().execute(ctx, (Consumer<? super SafeCommandStore>) safeStore0 -> markWaiting(safeStore0, safeStore0.get(txnId), range), safeStore.agent());
        }

        private static void markWaiting(SafeCommandStore safeStore, SafeCommand safeCommand, Range range)
        {
            if (!safeCommand.current().hasBeen(Status.PreApplied))
                safeStore.progressLog().waiting(CanApply, safeStore, safeCommand, null, Ranges.of(range), null);
        }

        private void done(SafeCommandStore safeStore)
        {
            setSuccess(null);
            logger.info("{}: fully caught-up with quorums", safeStore.commandStore());
        }

        private void updateWaitingOn(SafeCommandStore safeStore)
        {
            RedundantBefore redundantBefore = safeStore.redundantBefore();
            Ranges newWaitingOn = redundantBefore.removeLostOrStale(waitingOn);
            if (newWaitingOn != waitingOn)
            {
                Ranges retiredOrStale = waitingOn.without(newWaitingOn);
                if (!retiredOrStale.isEmpty())
                    logger.info("{}: {} are retired (or stale)", safeStore.commandStore(), retiredOrStale);
            }

            waitingOn = removeRedundant(newWaitingOn, durableBefore, redundantBefore, (caughtUp, remaining) -> {
                logger.info("{}: caught-up with quorum for {}; {} remaining", safeStore.commandStore(), caughtUp, remaining);
            });
        }

        @Override
        public void update(SafeCommandStore safeStore, Command command)
        {
            if (command.saveStatus().compareTo(SaveStatus.Applied) < 0 || command.saveStatus().compareTo(SaveStatus.TruncatedUnapplied) >= 0)
                return;

            if (!command.participants().touches().intersects(waitingOn))
                return;

            updateWaitingOn(safeStore);
            if (waitingOn.isEmpty())
            {
                done(safeStore);
                safeStore.unregister(this);
            }
        }
    }

    static Ranges removeRedundant(Ranges waitingOn, DurableBefore durableBefore, RedundantBefore redundantBefore, BiConsumer<Ranges, Ranges> removedAndRemaining)
    {
        return durableBefore.foldl(waitingOn, (DurableBefore.Entry entry, Ranges ranges, Object p1, Object p2) -> {
            Ranges entryRanges = Ranges.of(entry);
            return redundantBefore.foldlWithBounds(entryRanges, (RedundantBefore.Bounds bounds, Ranges rs, RoutingKey boundStart, RoutingKey boundEnd) -> {
                TxnId locallyRedundant = bounds.maxBound(LOCALLY_REDUNDANT);
                if (locallyRedundant.compareTo(entry.quorum) >= 0)
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
        }, waitingOn, null, null);
    }

    public static AsyncChain<Void> catchup(Node node)
    {
        return catchup(node, Arrays.asList(node.commandStores().all()));
    }

    public static AsyncChain<Void> catchup(Node node, List<CommandStore> commandStores)
    {
        return FetchDurableBefore.catchup(node).flatMap(durableBefore -> {
            List<AsyncChain<CommandStoreListener>> chains = new ArrayList<>();
            for (CommandStore commandStore : commandStores)
            {
                chains.add(commandStore.chain((ExecutionContext.Empty)() -> "Catchup", safeStore -> {
                    CommandStoreListener listener = new CommandStoreListener(durableBefore);
                    if (listener.register(safeStore))
                        return listener;
                    return null;
                }));
            }
            return AsyncChains.allOf(chains).flatMap(listeners -> {
                List<AsyncResult<Void>> registered = new ArrayList<>(listeners.size());
                for (CommandStoreListener listener : listeners)
                {
                    if (listener != null)
                        registered.add(listener);
                }
                if (registered.isEmpty())
                    return AsyncChains.success(null);
                return AsyncResults.reduce(registered, Reduce.toNull()).chain();
            });
        });
    }
}
