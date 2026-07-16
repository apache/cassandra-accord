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

package accord.topology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ProtocolModifiers;
import accord.api.VisibleForImplementation;
import accord.local.Node;
import accord.primitives.EpochSupplier;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.TopologyCollector.HasChangedReplication;
import accord.utils.Invariants;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Owned;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Unsynced;

public final class ActiveEpochs implements Iterable<ActiveEpoch>
{
    private static final Logger logger = LoggerFactory.getLogger(ActiveEpochs.class);

    final TopologyManager manager;
    final long currentEpoch;
    // TODO (desired): move this to TopologyManager
    final long firstNonEmptyEpoch;
    // Epochs are sorted in _descending_ order
    final ActiveEpoch[] epochs;

    private ActiveEpochs(TopologyManager manager, ActiveEpoch[] epochs, long firstNonEmptyEpoch)
    {
        this.manager = manager;
        this.currentEpoch = epochs.length > 0 ? epochs[0].epoch() : 0;
        this.firstNonEmptyEpoch = firstNonEmptyEpoch;
        this.epochs = epochs;
        for (int i = 1; i < epochs.length; i++)
            Invariants.requireArgument(epochs[i].epoch() == epochs[i - 1].epoch() - 1);
    }

    static ActiveEpochs empty(TopologyManager manager)
    {
        return new ActiveEpochs(manager, new ActiveEpoch[0], -1);
    }

    ActiveEpochs withNewEpochs(ActiveEpoch[] epochs)
    {
        long firstNonEmptyEpoch = this.firstNonEmptyEpoch;
        if (firstNonEmptyEpoch == -1)
        {
            for (int i = epochs.length - 1; firstNonEmptyEpoch == -1 && i >= 0 ; --i)
            {
                if (!epochs[i].all().isEmpty())
                    firstNonEmptyEpoch = epochs[i].epoch();
            }
        }
        return new ActiveEpochs(manager, epochs, firstNonEmptyEpoch);
    }

    ActiveEpochs maybeTruncate()
    {
        int truncateFrom = -1;
        // > 0 because we do not want to be left without epochs in case they're all empty
        for (int i = epochs.length - 1; i > 0; i--)
        {
            ActiveEpoch e = epochs[i];
            if (!e.allRetired())
                break;
            truncateFrom = i;
        }

        if (truncateFrom == -1)
            return this;

        ActiveEpoch[] newEpochs = Arrays.copyOf(epochs, truncateFrom);
        for (int i = truncateFrom; i < epochs.length; i++)
        {
            ActiveEpoch e = epochs[i];
            Invariants.require(epochs[i].isQuorumReady());
            logger.info("Retired epoch {} with added/removed ranges {}/{}. Topology: {}. Closed: {}", e.epoch(), e.addedRanges, e.removedRanges, e.all.ranges, e.closed());
        }
        if (logger.isTraceEnabled())
        {
            for (int i = 0; i < truncateFrom; i++)
            {
                ActiveEpoch e = epochs[i];
                Invariants.require(e.isQuorumReady());
                logger.trace("Leaving epoch {} with added/removed ranges {}/{}", e.epoch(), e.addedRanges, e.removedRanges);
            }
        }

        return withNewEpochs(newEpochs);
    }

    public boolean isEmpty()
    {
        return epochs.length == 0;
    }

    public int size()
    {
        return epochs.length;
    }

    public long nextEpoch()
    {
        return current().epoch + 1;
    }

    public long minEpoch()
    {
        if (currentEpoch == 0)
            return 0;
        return currentEpoch - epochs.length + 1;
    }

    public long epoch()
    {
        return currentEpoch;
    }

    public long maxEpoch(long minEpoch, Function<ActiveEpoch, Topology> topology, Routables<?> keys)
    {
        long epoch = Math.max(epoch(), minEpoch);
        while (!topology.apply(getKnown(epoch)).ranges().containsAll(keys))
        {
            if (--epoch < minEpoch())
                throw new IllegalArgumentException(keys + " not found in any active epoch");
        }
        return epoch;
    }

    public Topology current()
    {
        return epochs.length > 0 ? epochs[0].all() : Topology.EMPTY;
    }

    public Topology currentLocal()
    {
        return epochs.length > 0 ? epochs[0].local() : Topology.EMPTY;
    }

    boolean isQuorumReady(long epoch)
    {
        ActiveEpoch e = ifExists(epoch);
        return e != null && e.isQuorumReady();
    }

    @VisibleForTesting
    public EpochReady epochReady(long epoch)
    {
        if (epoch < minEpoch())
            return EpochReady.done(epoch);

        if (epoch > currentEpoch)
            throw new IllegalArgumentException(String.format("Epoch %d is larger than current epoch %d", epoch, currentEpoch));

        return getKnown(epoch).epochReady();
    }

    /**
     * Mark sync complete for the given node/epoch, and if this epoch
     * is now synced, update the prevSynced flag on superseding epochs
     */
    @GuardedBy("TopologyManager")
    void onReadyToCoordinate(Node.Id id, long epoch)
    {
        Invariants.requireArgument(epoch >= 0);

        int i = indexIfExists(epoch);
        if (i < 0)
        {
            if (epoch > currentEpoch) i = 0;
            else return;
        }

        while (i < epochs.length && epochs[i].onReadyToCoordinate(id))
            ++i;
    }

    private Ranges closedOrRetired(Ranges ranges, long epoch, BiFunction<ActiveEpoch, Ranges, Ranges> f)
    {
        Invariants.requireArgument(epoch >= 0);

        int i = indexIfExists(epoch);
        if (i < 0)
        {
            if (epoch > currentEpoch)
            {
                if (epochs.length == 0)
                    return ranges;
                i = 0;
            }
            else
            {
                return Ranges.EMPTY; // notification came for an already truncated epoch
            }
        }

        Ranges cur = f.apply(epochs[i++], ranges);
        Ranges report = epoch <= currentEpoch ? cur : ranges;
        while (!cur.isEmpty() && i < epochs.length)
            cur = f.apply(epochs[i++], cur);
        return report;
    }

    /**
     * Mark the epoch as "closed" for the provided ranges; this means that no new transactions
     * that intersect with this range may be proposed in the epoch (they will be rejected).
     */
    @GuardedBy("TopologyManager")
    Ranges closed(Ranges ranges, long epoch)
    {
        return closedOrRetired(ranges, epoch, ActiveEpoch::recordClosed);
    }

    /**
     * Mark the epoch as "retired" for the provided ranges; this means that all transactions that can be
     * proposed for this epoch have now been executed globally.
     */
    @GuardedBy("TopologyManager")
    Ranges retired(Ranges ranges, long epoch)
    {
        return closedOrRetired(ranges, epoch, ActiveEpoch::recordRetired);
    }

    public ActiveEpoch get(long epoch) throws TopologyException
    {
        int index = indexOf(epoch);
        return epochs[index];
    }

    public ActiveEpoch atIndex(int index)
    {
        return epochs[index];
    }

    // caller is expected to have verified that epoch is known to this ActiveEpochs
    public ActiveEpoch getKnown(long epoch)
    {
        return epochs[knownIndexOf(epoch)];
    }

    public @Nullable ActiveEpoch ifExists(long epoch)
    {
        int index = indexIfExists(epoch);
        if (index < 0)
            return null;

        return epochs[index];
    }

    private int indexOf(long epoch) throws TopologyException
    {
        if (epoch < minEpoch()) throw new TopologyRetiredException(epoch, minEpoch());
        else if (epoch > currentEpoch) throw new TopologyNotReadyException(epoch, currentEpoch);

        return (int) (currentEpoch - epoch);
    }

    private int knownIndexOf(long epoch)
    {
        if (epoch < minEpoch()) throw new IllegalArgumentException(TopologyRetiredException.message(epoch, minEpoch()));
        else if (epoch > currentEpoch) throw new IllegalArgumentException(TopologyNotReadyException.message(epoch, currentEpoch));

        return (int) (currentEpoch - epoch);
    }

    private int indexIfExists(long epoch)
    {
        if (epoch > currentEpoch || epoch <= currentEpoch - epochs.length)
            return -1;

        return (int) (currentEpoch - epoch);
    }


    /**
     * Fetch topologies between {@param minEpoch} (inclusive), and {@param maxEpoch} (inclusive).
     */
    public TopologyRange between(long minEpoch, long maxEpoch)
    {
        // No epochs known to Accord
        if (firstNonEmptyEpoch == -1 || minEpoch > currentEpoch)
            return new TopologyRange(minEpoch(), currentEpoch, firstNonEmptyEpoch, Collections.emptyList());

        minEpoch = Math.max(minEpoch, minEpoch());
        int diff =  Math.toIntExact(currentEpoch - minEpoch + 1);
        List<Topology> topologies = new ArrayList<>(diff);
        for (int i = 0; minEpoch + i <= maxEpoch && i < diff; i++)
            topologies.add(getKnown(minEpoch + i).all);

        return new TopologyRange(minEpoch, currentEpoch, firstNonEmptyEpoch, topologies);
    }

    // TODO (testing): test all of these methods when asking for epochs that have been cleaned up (and other code paths)

    /**
     * Returns topologies containing epochs where specified ranges haven't completed synchronization between min/max epochs.
     * Can be used during coordination operations to ensure they contact all relevant nodes across topology changes,
     * particularly when some ranges are still syncing after cluster membership changes.
     */
    public Topologies withUnsyncedEpochs(Unseekables<?> select, TxnId txnId, Timestamp max) throws TopologyException
    {
        return withUnsyncedEpochs(select, txnId, max, txnId.selectsShards());
    }

    public Topologies withUnsyncedEpochs(Unseekables<?> select, Timestamp min, Timestamp max, SelectShards selectShards) throws TopologyException
    {
        return withUnsyncedEpochs(select, min.epoch(), max.epoch(), selectShards);
    }

    public Topologies select(Unseekables<?> select, Timestamp min, Timestamp max, SelectShards selectShards, ProtocolModifiers.QuorumEpochIntersections.Include include) throws TopologyException
    {
        return select(select, min.epoch(), max.epoch(), selectShards, include);
    }

    public Topologies select(Unseekables<?> select, long minEpoch, long maxEpoch, SelectShards selectShards, ProtocolModifiers.QuorumEpochIntersections.Include include) throws TopologyException
    {
        switch (include)
        {
            default: throw new AssertionError("Unhandled Include: " + include);
            case Unsynced: return withUnsyncedEpochs(select, minEpoch, maxEpoch, selectShards);
            case Owned: return preciseEpochs(select, minEpoch, maxEpoch, selectShards);
        }
    }

    public Topologies reselect(@Nullable Topologies prev, @Nullable ProtocolModifiers.QuorumEpochIntersections.Include prevIncluded, Unseekables<?> select, Timestamp min, Timestamp max, SelectShards selectNodeOwnership, ProtocolModifiers.QuorumEpochIntersections.Include include) throws TopologyException
    {
        return reselect(prev, prevIncluded, select, min.epoch(), max.epoch(), selectNodeOwnership, include);
    }

    // prevIncluded may be null even when prev is not null, in cases where we do not know what prev was produced with
    public Topologies reselect(@Nullable Topologies prev, @Nullable ProtocolModifiers.QuorumEpochIntersections.Include prevIncluded, Unseekables<?> select, long minEpoch, long maxEpoch, SelectShards selectShards, ProtocolModifiers.QuorumEpochIntersections.Include include) throws TopologyException
    {
        if (include == Owned)
        {
            if (prev != null && prev.currentEpoch() >= maxEpoch && prev.oldestEpoch() <= minEpoch)
                return prev.forEpochs(minEpoch, maxEpoch);
            else
                return preciseEpochs(select, minEpoch, maxEpoch, selectShards);
        }
        else
        {
            if (prevIncluded == Unsynced && prev != null && prev.currentEpoch() == maxEpoch && prev.oldestEpoch() <= minEpoch)
                return prev;
            else // TODO (desired): can we avoid recalculating when only minEpoch advances?
                return withUnsyncedEpochs(select, minEpoch, maxEpoch, selectShards);
        }

    }

    public Topologies withUnsyncedEpochs(Unseekables<?> select, long minEpoch, long maxEpoch, SelectShards selectShards) throws TopologyException
    {
        Invariants.requireArgument(minEpoch <= maxEpoch, "min epoch %d > max %d", minEpoch, maxEpoch);
        return withSufficientEpochsAtLeast(select, minEpoch, maxEpoch, selectShards, ActiveEpoch::quorumReady);
    }

    public TxnId.FastPath selectFastPath(Routables<?> select, long epoch)
    {
        return atLeast(select, epoch, epoch, ActiveEpoch::quorumReady, manager.bestFastPath);
    }

    public boolean supportsPrivilegedFastPath(Routables<?> select, long epoch)
    {
        return atLeast(select, epoch, epoch, ActiveEpoch::quorumReady, manager.supportsPrivilegedFastPath);
    }

    public Topologies withOpenEpochs(Routables<?> select, @Nullable EpochSupplier min, @Nullable EpochSupplier max, SelectShards selectShards) throws TopologyException
    {
        return withSufficientEpochsAtMost(select,
                                          min == null ? Long.MIN_VALUE : min.epoch(),
                                          max == null ? Long.MAX_VALUE : max.epoch(),
                                          selectShards,
                                          ActiveEpoch::closed);
    }

    public Topologies withUncompletedEpochs(Unseekables<?> select, @Nullable EpochSupplier min, EpochSupplier max, SelectShards selectShards) throws TopologyException
    {
        return withSufficientEpochsAtLeast(select,
                                           min == null ? Long.MIN_VALUE : min.epoch(),
                                           max == null ? Long.MAX_VALUE : max.epoch(),
                                           selectShards,
                                           ActiveEpoch::retired);
    }

    private Topologies withSufficientEpochsAtLeast(Unseekables<?> select, long minEpoch, long maxEpoch, SelectShards selectShards, Function<ActiveEpoch, Ranges> isSufficientFor) throws TopologyException
    {
        return atLeast(select, minEpoch, maxEpoch, isSufficientFor,
                       selectShards == SelectShards.LIVE ? manager.liveCollector : manager.allCollector);
    }

    private <C, K extends Routables<?>, T, E extends Exception>
    T atLeast(K select, long minEpoch, long maxEpoch,
              Function<ActiveEpoch, Ranges> isSufficientFor,
              TopologyCollector<C, K, T, E> collectors) throws E
    {
        Invariants.requireArgument(minEpoch <= maxEpoch);
        if (maxEpoch < minEpoch())
            return collectors.retired(maxEpoch, minEpoch());

        if (maxEpoch == Long.MAX_VALUE) maxEpoch = currentEpoch;
        else Invariants.require(currentEpoch >= maxEpoch, "current epoch %d < max %d", currentEpoch, maxEpoch);

        ActiveEpoch max = getKnown(maxEpoch);
        if (minEpoch == maxEpoch && isSufficientFor.apply(max).containsAll(select))
            return collectors.one(max, select);

        int i = (int)(currentEpoch - maxEpoch);
        int maxi = (int)(Math.min(1 + currentEpoch - minEpoch, epochs.length));
        C collector = collectors.allocate(maxi - i);

        // Previous logic would exclude synced ranges, but this was removed as that makes min epoch selection harder.
        // An issue was found where a range was removed from a replica and min selection picked the epoch before that,
        // which caused a node to get included in the txn that actually lost the range
        // See CASSANDRA-18804
        while (i < maxi)
        {
            ActiveEpoch e = epochs[i++];
            collector = collectors.update(collector, e, select);
            select = (K)select.without(e.addedRanges);
        }

        if (select.isEmpty())
            return collectors.multi(collector);

        if (i == epochs.length)
        {
            // now we GC epochs, we cannot rely on addedRanges to remove all ranges, so we also remove the ranges found in the earliest epoch we have
            select = (K)select.without(collectors.selects(epochs[epochs.length - 1]).ranges);
            if (!select.isEmpty())
                throw Invariants.illegalArgument("Ranges %s could not be found", select);
            return collectors.multi(collector);
        }

        // remaining is updated based off isSufficientFor, but select is not
        Routables<?> remaining = select;

        // include any additional epochs to reach sufficiency
        ActiveEpoch prev = epochs[maxi - 1];
        do
        {
            remaining = remaining.without(isSufficientFor.apply(prev));
            Routables<?> prevSelect = select;
            select = (K)select.without(prev.addedRanges);
            if (prevSelect != select) // perf optimization; if select wasn't changed (it does not intersect addedRanges), then remaining won't
                remaining = remaining.without(prev.addedRanges);
            if (remaining.isEmpty())
                return collectors.multi(collector);

            ActiveEpoch next = epochs[i++];
            collector = collectors.update(collector, next, select);
            prev = next;
        } while (i < epochs.length);
        // need to remove sufficient / added else remaining may not be empty when the final matches are the last epoch
        remaining = remaining.without(isSufficientFor.apply(prev));
        remaining = remaining.without(prev.addedRanges);
        // TODO (desired): propagate addedRanges to the earliest epoch we retain for consistency
        // now we GC epochs, we cannot rely on addedRanges to remove all ranges, so we also remove the ranges found in the earliest epoch we have
        remaining = remaining.without(collectors.selects(epochs[epochs.length - 1]).ranges);
        if (!remaining.isEmpty())
            Invariants.illegalArgument("Ranges %s could not be found", remaining);

        return collectors.multi(collector);
    }

    private Topologies withSufficientEpochsAtMost(Routables<?> select, long minEpoch, long maxEpoch, SelectShards selectShards, Function<ActiveEpoch, Ranges> isSufficientFor) throws TopologyException
    {
        return atMost(select, minEpoch, maxEpoch, isSufficientFor, selectShards == SelectShards.LIVE ? manager.liveCollector : manager.allCollector);
    }

    private <C, K extends Routables<?>, T, E extends Exception> T atMost(K select, long minEpoch, long maxEpoch, Function<ActiveEpoch, Ranges> isSufficientFor,
                                                    TopologyCollector<C, K, T, E> collectors) throws E
    {
        Invariants.requireArgument(minEpoch <= maxEpoch);

        minEpoch = Math.max(minEpoch(), minEpoch);
        if (maxEpoch == Long.MAX_VALUE) maxEpoch = currentEpoch;
        if (maxEpoch > currentEpoch) return collectors.notReady(maxEpoch, currentEpoch);
        else if (maxEpoch < minEpoch) return collectors.retired(maxEpoch, minEpoch);

        ActiveEpoch cur = getKnown(maxEpoch);
        if (minEpoch == maxEpoch)
             return collectors.one(cur, select);

        int i = (int)(currentEpoch - maxEpoch);
        int maxi = (int)(Math.min(1 + currentEpoch - minEpoch, epochs.length));
        C collector = collectors.allocate(maxi - i);

        while (!select.isEmpty())
        {
            collector = collectors.update(collector, cur, select);
            select = (K)select.without(cur.addedRanges)
                              .without(isSufficientFor.apply(cur));

            if (++i == maxi)
                break;

            cur = epochs[i];
        }

        return collectors.multi(collector);
    }

    public Topologies preciseEpochs(Unseekables<?> select, long minEpoch, long maxEpoch, SelectShards shards) throws TopologyException
    {
        return preciseEpochs(select, minEpoch, maxEpoch, shards, Topology::select);
    }

    public Topologies preciseEpochsIfExists(Unseekables<?> select, long minEpoch, long maxEpoch, SelectShards selectNodeOwnership) throws TopologyException
    {
        return preciseEpochs(select, minEpoch, maxEpoch, selectNodeOwnership, Topology::selectIfExists);
    }

    @Override
    public Iterator<ActiveEpoch> iterator()
    {
        return Stream.of(epochs).iterator();
    }

    public Stream<ActiveEpoch> stream()
    {
        return Stream.of(epochs);
    }

    public Topologies preciseEpochs(Unseekables<?> select, long minEpoch, long maxEpoch, SelectShards shards, SelectTopology selectTopology) throws TopologyException
    {
        // TODO (expected): we should disambiguate minEpoch we can bump (i.e. historical epochs) and those we cannot (i.e. txnId.epoch())
        minEpoch = Math.max(minEpoch(), minEpoch);
        if (maxEpoch == Long.MAX_VALUE) maxEpoch = currentEpoch;
        if (maxEpoch > currentEpoch) throw new TopologyNotReadyException(maxEpoch, currentEpoch);
        else if (maxEpoch < minEpoch) throw new TopologyRetiredException(maxEpoch, minEpoch);

        if (minEpoch == maxEpoch)
            return new Topologies.Single(manager.sorter, selectTopology.apply(get(minEpoch).get(shards), select));

        int count = (int)(1 + maxEpoch - minEpoch);
        Topologies.Builder topologies = new Topologies.Builder(count);
        for (int i = count - 1 ; i >= 0 ; --i)
        {
            ActiveEpoch e = get(minEpoch + i);
            topologies.add(selectTopology.apply(e.get(shards), select));
            select = select.without(e.addedRanges);
        }
        Invariants.require(!topologies.isEmpty(), "Unable to find an epoch that contained %s", select);

        return topologies.build(manager.sorter);
    }

    public Topologies forEpoch(Unseekables<?> select, long epoch, SelectShards shards) throws TopologyException
    {
        return new Topologies.Single(manager.sorter, get(epoch).get(shards).select(select));
    }

    public boolean hasReplicationMaybeChanged(Unseekables<?> select, long sinceEpoch)
    {
        return atLeast(select, sinceEpoch, Long.MAX_VALUE, ignore -> Ranges.EMPTY, HasChangedReplication.INSTANCE);
    }

    public Topologies forEpochAtLeast(Unseekables<?> select, long epoch, SelectShards selectShards) throws TopologyMismatch
    {
        ActiveEpoch e = ifExists(epoch);
        if (e == null)
        {
            Invariants.require(currentEpoch >= epoch, "current epoch %d < provided max %d", currentEpoch, epoch);
            e = getKnown(minEpoch());
        }
        return new Topologies.Single(manager.sorter, e.get(selectShards).select(select));
    }

    @VisibleForImplementation
    public Shard forEpochIfKnown(RoutableKey key, long epoch)
    {
        ActiveEpoch e = ifExists(epoch);
        if (e == null)
            return null;
        return e.all().forKeyIfKnown(key);
    }

    public boolean hasEpoch(long epoch)
    {
        return ifExists(epoch) != null;
    }

    public boolean hasAtLeastEpoch(long epoch)
    {
        return currentEpoch >= epoch;
    }

    public Topology globalForEpoch(long epoch) throws TopologyException
    {
        ActiveEpoch e = get(epoch);
        return e.all();
    }

    public List<Topology> topologySnapshot()
    {
        // Write to this volatile variable is done via synchronized, so this is single-writer multi-consumer; safe to read without locks
        ImmutableList.Builder<Topology> builder = ImmutableList.builderWithExpectedSize(epochs.length);
        for (int i = 0; i < epochs.length; i++)
        {
            // This class's state is mutable with regard to: ready, synced, closed, retired
            ActiveEpoch e = epochs[i];
            builder.add(e.all);
        }
        return builder.build();
    }

    @VisibleForTesting
    public static ActiveEpochs unsafeNew(TopologyManager tm, ActiveEpoch[] active, long prevFirstNonEmptyEpoch)
    {
        return new ActiveEpochs(tm, active, prevFirstNonEmptyEpoch);
    }
}
