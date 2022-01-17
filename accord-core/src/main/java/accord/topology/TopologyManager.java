package accord.topology;

import accord.api.ConfigurationService;
import accord.api.Key;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.messages.EpochRequest;
import accord.messages.Request;
import accord.topology.Topologies.Single;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import java.util.*;
import java.util.function.LongConsumer;

/**
 * Manages topology state changes and update bookkeeping
 *
 * Each time the topology changes we need to:
 * * confirm previous owners of ranges we replicate are aware of the new config
 * * learn of any outstanding operations for ranges we replicate
 * * clean up obsolete data
 *
 * Assumes a topology service that won't report epoch n without having n-1 etc also available
 *
 * TODO: make TopologyManager a Topologies and copy-on-write update to it, so we can always just take a reference for
 *       transactions instead of copying every time (and index into it by the txnId.epoch)
 */
public class TopologyManager implements ConfigurationService.Listener
{
    private static final Future<Void> SUCCESS = ImmediateFuture.success(null);
    static class EpochState
    {
        private final Topology global;
        private final Topology local;
        private final QuorumTracker syncTracker;
        private boolean syncComplete = false;
        private boolean prevSynced;

        EpochState(Node.Id node, Topology global, boolean prevSynced)
        {
            this.global = global;
            this.local = global.forNode(node);
            Preconditions.checkArgument(!global().isSubset());
            this.syncTracker = new QuorumTracker(new Single(global(), false));
            this.prevSynced = prevSynced;
        }

        void markPrevSynced()
        {
            prevSynced = true;
        }

        public void recordSyncComplete(Node.Id node)
        {
            syncComplete = syncTracker.success(node);
        }

        Topology global()
        {
            return global;
        }

        Topology local()
        {
            return local;
        }

        long epoch()
        {
            return global().epoch;
        }

        boolean syncComplete()
        {
            return prevSynced && syncComplete;
        }

        /**
         * determine if sync has completed for all shards intersecting with the given keys
         */
        boolean syncCompleteFor(Keys keys)
        {
            if (!prevSynced)
                return false;
            if (syncComplete)
                return true;
            Boolean result = global().foldl(keys, (i, shard, acc) -> {
                if (acc == Boolean.FALSE)
                    return acc;
                return Boolean.valueOf(syncTracker.unsafeGet(i).hasReachedQuorum());
            }, Boolean.TRUE);
            return result == Boolean.TRUE;
        }

        boolean shardIsUnsynced(int idx, Shard shard)
        {
            return !prevSynced || !syncTracker.unsafeGet(idx).hasReachedQuorum();
        }
    }

    private class Epochs
    {
        private final long currentEpoch;
        private final EpochState[] epochs;
        // nodes we've received sync complete notifications from, for epochs we do not yet have topologies for.
        // Pending sync notifications are indexed by epoch, with the current epoch as index[0], and future epochs
        // as index[epoch - currentEpoch]. Sync complete notifications for the current epoch are marked pending
        // until the superseding epoch has been applied
        private final List<Set<Node.Id>> pendingSyncComplete;

        // list of promises to be completed as newer epochs become active. This is to support processes that
        // are waiting on future epochs to begin (ie: txn requests from futures epochs). Index 0 is for
        // currentEpoch + 1
        private final List<AsyncPromise<Void>> futureEpochFutures;

        private Epochs(EpochState[] epochs, List<Set<Node.Id>> pendingSyncComplete, List<AsyncPromise<Void>> futureEpochFutures)
        {
            this.currentEpoch = epochs.length > 0 ? epochs[0].epoch() : 0;
            this.pendingSyncComplete = pendingSyncComplete;
            this.futureEpochFutures = futureEpochFutures;
            for (int i=1; i<epochs.length; i++)
                Preconditions.checkArgument(epochs[i].epoch() == epochs[i-1].epoch() - 1);
            this.epochs = epochs;
        }

        private Epochs(EpochState[] epochs)
        {
            this(epochs, new ArrayList<>(), new ArrayList<>());
        }

        public Future<Void> awaitEpoch(long epoch)
        {
            if (epoch <= currentEpoch)
                return SUCCESS;

            int diff = (int) (epoch - currentEpoch);
            while (futureEpochFutures.size() < diff)
                futureEpochFutures.add(new AsyncPromise<>());

            return futureEpochFutures.get(diff - 1);
        }

        public long nextEpoch()
        {
            return current().epoch + 1;
        }

        public Topology current()
        {
            return epochs.length > 0 ? epochs[0].global() : Topology.EMPTY;
        }

        /**
         * Mark sync complete for the given node/epoch, and if this epoch
         * is now synced, update the prevSynced flag on superseding epochs
         */
        public void syncComplete(Node.Id node, long epoch)
        {
            Preconditions.checkArgument(epoch > 0);
            if (epoch > currentEpoch - 1)
            {
                int idx = (int) (epoch - currentEpoch);
                for (int i=pendingSyncComplete.size(); i<=idx; i++)
                    pendingSyncComplete.add(new HashSet<>());

                pendingSyncComplete.get(idx).add(node);
            }
            else
            {
                EpochState state = get(epoch);
                state.recordSyncComplete(node);
                for (epoch++ ; state.syncComplete() && epoch <= currentEpoch; epoch++)
                {
                    state = get(epoch);
                    state.markPrevSynced();
                }
            }
        }

        private EpochState get(long epoch)
        {
            if (epoch > currentEpoch || epoch < currentEpoch - epochs.length)
                return null;

            return epochs[(int) (currentEpoch - epoch)];
        }

        boolean requiresHistoricalTopologiesFor(Keys keys, long epoch)
        {
            Preconditions.checkState(epoch <= currentEpoch);
            if (1 + currentEpoch - epoch >= epochs.length)
                return false;
            int i = (int)(1 + currentEpoch - epoch);
            return !epochs[i].syncCompleteFor(keys);
        }
    }

    private final Node.Id node;
    // TODO (now): this is unused!?
    private final LongConsumer epochReporter;
    private volatile Epochs epochs;

    public TopologyManager(Node.Id node, LongConsumer epochReporter)
    {
        this.node = node;
        this.epochReporter = epochReporter;
        this.epochs = new Epochs(new EpochState[0]);
    }

    @Override
    public synchronized void onTopologyUpdate(Topology topology)
    {
        Epochs current = epochs;

        Preconditions.checkArgument(topology.epoch == current.nextEpoch());
        EpochState[] nextEpochs = new EpochState[current.epochs.length + 1];
        List<Set<Node.Id>> pendingSync = new ArrayList<>(current.pendingSyncComplete);
        if (!pendingSync.isEmpty())
        {
            EpochState currentEpoch = current.epochs[0];
            if (current.epochs.length <= 1 || current.epochs[1].syncComplete())
                currentEpoch.markPrevSynced();
            pendingSync.remove(0).forEach(currentEpoch::recordSyncComplete);
        }
        System.arraycopy(current.epochs, 0, nextEpochs, 1, current.epochs.length);

        boolean prevSynced = current.epochs.length == 0 || current.epochs[0].syncComplete();
        nextEpochs[0] = new EpochState(node, topology, prevSynced);

        List<AsyncPromise<Void>> futureEpochFutures = new ArrayList<>(current.futureEpochFutures);
        AsyncPromise<Void> toComplete = !futureEpochFutures.isEmpty() ? futureEpochFutures.remove(0) : null;
        epochs = new Epochs(nextEpochs, pendingSync, futureEpochFutures);
        if (toComplete != null)
            toComplete.trySuccess(null);
    }

    public synchronized Future<Void> awaitEpoch(long epoch)
    {
        return epochs.awaitEpoch(epoch);
    }

    @Override
    public void onEpochSyncComplete(Node.Id node, long epoch)
    {
        epochs.syncComplete(node, epoch);
    }

    public Topology current()
    {
        return epochs.current();
    }

    public long epoch()
    {
        return current().epoch;
    }

    @VisibleForTesting
    EpochState getEpochStateUnsafe(long epoch)
    {
        return epochs.get(epoch);
    }

    public Topologies syncForKeys(Keys keys, long minEpoch, long maxEpoch)
    {
        Epochs snapshot = epochs;

        if (maxEpoch == Long.MAX_VALUE) maxEpoch = snapshot.currentEpoch;
        else Preconditions.checkState(snapshot.currentEpoch >= maxEpoch);

        EpochState epochState = snapshot.get(maxEpoch);
        if (minEpoch == maxEpoch && !snapshot.requiresHistoricalTopologiesFor(keys, maxEpoch))
            return new Single(epochState.global.forKeys(keys), true);

        int i = (int)(snapshot.currentEpoch - maxEpoch);
        int limit = (int)(Math.min(1 + snapshot.currentEpoch - minEpoch, snapshot.epochs.length));
        int count = limit - i;
        while (limit < snapshot.epochs.length && !snapshot.epochs[limit].syncCompleteFor(keys))
        {
            ++count;
            ++limit;
        }

        Topologies.Multi topologies = new Topologies.Multi(count);
        while (i < limit)
            topologies.add(snapshot.epochs[i++].global.forKeys(keys));

        return topologies;
    }

    public Topologies syncForKeys(Keys keys, long epoch)
    {
        return syncForKeys(keys, epoch, epoch);
    }

    public Topologies unsyncForKeys(Keys keys, long minEpoch, long maxEpoch)
    {
        Epochs snapshot = epochs;

        if (minEpoch == maxEpoch)
            return new Single(snapshot.get(minEpoch).global().forKeys(keys), true);

        int count = (int)(1 + maxEpoch - minEpoch);
        Topologies.Multi topologies = new Topologies.Multi(count);
        for (int i = 0 ; i < count ; ++i)
            topologies.add(snapshot.get(minEpoch + count).global.forKeys(keys));

        return topologies;
    }

    public Topologies unsyncForKeys(Keys keys, long epoch)
    {
        return unsyncForKeys(keys, epoch, epoch);
    }

    public Topologies currentForKeys(Keys keys)
    {
        Epochs snapshot = epochs;
        return new Single(snapshot.get(snapshot.currentEpoch).global().forKeys(keys), false);
    }

    public Topologies forEpoch(Keys keys, long epoch)
    {
        return new Single(epochs.get(epoch).global().forKeys(keys), true);
    }

    public Shard forEpochIfKnown(Key key, long epoch)
    {
        EpochState epochState = epochs.get(epoch);
        if (epochState == null)
            return null;
        return epochState.global().forKey(key);
    }

    public boolean hasEpoch(long epoch)
    {
        return epochs.get(epoch) != null;
    }

    public Topologies forTxn(Txn txn, long epoch)
    {
        return syncForKeys(txn.keys(), epoch, epoch);
    }

    public Topologies forTxn(Txn txn, long minEpoch, long maxEpoch)
    {
        return syncForKeys(txn.keys(), minEpoch, maxEpoch);
    }

    public Topologies forTxn(Txn txn, Timestamp min, Timestamp max)
    {
        return syncForKeys(txn.keys(), min.epoch, max.epoch);
    }

    public Topologies unsyncForTxn(Txn txn, long epoch)
    {
        return unsyncForKeys(txn.keys(), epoch, epoch);
    }

    public Topologies unsyncForTxn(Txn txn, long minEpoch, long maxEpoch)
    {
        return syncForKeys(txn.keys(), minEpoch, maxEpoch);
    }

    public Topology localForEpoch(long epoch)
    {
        return epochs.get(epoch).local();
    }

    public Topology globalForEpoch(long epoch)
    {
        return epochs.get(epoch).global();
    }

    public Topologies forEpoch(Txn txn, long epoch)
    {
        return forEpoch(txn.keys(), epoch);
    }

    public long maxUnknownEpoch(Request request)
    {
        if (!(request instanceof EpochRequest))
            return 0;

        long waitForEpoch = ((EpochRequest) request).waitForEpoch();
        if (epochs.currentEpoch < waitForEpoch)
            return waitForEpoch;

        return 0;
    }
}
