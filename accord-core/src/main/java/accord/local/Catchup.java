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
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
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
import static accord.local.ExecutionContext.*;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Functions.alwaysFalse;

public class Catchup
{
    private static final Logger logger = LoggerFactory.getLogger(Catchup.class);
    static class CommandStoreListener extends SettableResult<Unsuccessful> implements SyncPointListener, Timeouts.Timeout
    {
        final CommandStore commandStore;
        final long deadline;
        final TimeUnit deadlineUnits;
        final DurableBefore durableBefore;
        RegisteredTimeout timeout;
        Ranges waitingOn;

        CommandStoreListener(SafeCommandStore safeStore, long deadline, TimeUnit deadlineUnits, DurableBefore durableBefore)
        {
            this.commandStore = safeStore.commandStore();
            this.deadline = deadline;
            this.deadlineUnits = deadlineUnits;
            this.durableBefore = durableBefore;
        }

        synchronized boolean register(SafeCommandStore safeStore)
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
                timeout = safeStore.node().timeouts().registerAt(this, deadline, deadlineUnits);
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
            ExecutionContext ctx = unsequenced(txnId, "Catchup");
            if (safeStore.canExecuteWith(ctx)) markWaiting(safeStore, safeStore.unsafeGet(txnId), range);
            else safeStore.commandStore().execute(ctx, (Consumer<? super SafeCommandStore>) safeStore0 -> markWaiting(safeStore0, safeStore0.unsafeGet(txnId), range), safeStore.agent());
        }

        private static void markWaiting(SafeCommandStore safeStore, SafeCommand safeCommand, Range range)
        {
            if (!safeCommand.current().hasBeen(Status.PreApplied))
                safeStore.progressLog().waiting(CanApply, safeStore, safeCommand, null, Ranges.of(range), null);
        }

        private void done(SafeCommandStore safeStore)
        {
            trySuccess(null);
            logger.info("{}: fully caught-up with quorums", safeStore.commandStore());
            if (timeout != null)
            {
                timeout.cancel();
                timeout = null;
            }
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

            synchronized (this)
            {
                updateWaitingOn(safeStore);
                if (!waitingOn.isEmpty())
                    return;
            }
            done(safeStore);
            safeStore.unregister(this);
        }

        @Override
        public void timeout()
        {
            Unsuccessful unsuccessful = null;
            synchronized (this)
            {
                if (!waitingOn.isEmpty())
                    unsuccessful = new Unsuccessful(waitingOn);
            }
            commandStore.chain((Empty)() -> "Timeout Catchup", safeStore -> { safeStore.unregister(this); })
                        .begin(commandStore.agent);
            trySuccess(unsuccessful);
        }

        @Override
        public int stripe()
        {
            return commandStore.id;
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

    public static AsyncResult<Unsuccessful> catchup(Node node, long deadline, TimeUnit units, List<CommandStore> commandStores)
    {
        return FetchDurableBefore.catchup(node).flatMap(durableBefore -> {
            List<AsyncChain<CommandStoreListener>> chains = new ArrayList<>();
            for (CommandStore commandStore : commandStores)
            {
                chains.add(commandStore.chain((Empty)() -> "Catchup", safeStore -> {
                    CommandStoreListener listener = new CommandStoreListener(safeStore, deadline, units, durableBefore);
                    if (listener.register(safeStore))
                        return listener;
                    return null;
                }));
            }
            return AsyncChains.allOf(chains).flatMap(listeners -> {
                List<AsyncResult<Unsuccessful>> registered = new ArrayList<>(listeners.size());
                for (CommandStoreListener listener : listeners)
                {
                    if (listener != null)
                        registered.add(listener);
                }
                if (registered.isEmpty())
                    return AsyncChains.success((Unsuccessful)null);
                return AsyncResults.reduce(registered, Unsuccessful::merge).chain();
            });
        }).beginAsResult();
    }

    private static AsyncResult<?> rebootstrapIfBehind(Node node, SafeCommandStore safeStore, DurableBefore durableBefore)
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
            logger.info("No ranges to rebootstrap");
            return AsyncResults.success(null);
        }

        logger.info("Rebootstrapping {} with quorums", catchUp);
        return safeStore.commandStore().rebootstrap(node, catchUp, BootstrapReason.CATCHUP);
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
                }).flatMapResult(i -> i));
            }
            return AsyncChains.reduce(chains, Reduce.toNull());
        });
    }
}
