package accord.topology;

import accord.api.ConfigurationService;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.messages.Request;
import accord.messages.TxnRequest;
import accord.txn.Keys;
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
 */
public class TopologyManager implements ConfigurationService.Listener
{
    private static final Future<Void> SUCCESS = ImmediateFuture.success(null);
    class EpochState
    {
        private final Topology global;
        private final Topology local;
        private final QuorumTracker syncTracker;
        private boolean syncComplete = false;
        private boolean prevSynced;

        EpochState(Topology global, boolean prevSynced)
        {
            Preconditions.checkArgument(!global.isSubset());
            this.global = global;
            this.local = global.forNode(node);
            this.syncTracker = new QuorumTracker(new Topologies.Singleton(global, false));
            this.prevSynced = prevSynced;
        }

        void markPrevSynced()
        {
            prevSynced = true;
        }

        public void recordSyncComplete(Node.Id node)
        {
            syncTracker.recordSuccess(node);
            syncComplete = syncTracker.hasReachedQuorum();
        }

        long epoch()
        {
            return global.epoch;
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
            Boolean result = global.foldl(keys, (i, shard, acc) -> {
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
            return epochs.length > 0 ? epochs[0].global : Topology.EMPTY;
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

        long maxUnknownEpoch(TxnRequest.Scope scope)
        {
            if (currentEpoch < scope.minRequiredEpoch())
                return scope.minRequiredEpoch();
            return 0;
        }

        boolean requiresHistoricalTopologiesFor(Keys keys)
        {
            return epochs.length > 1 && !epochs[1].syncCompleteFor(keys);
        }
    }

    private final Node.Id node;
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
        nextEpochs[0] = new EpochState(topology, prevSynced);

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

    public Topologies forKeys(Keys keys, long minEpoch)
    {
        Epochs snapshot = epochs;

        EpochState epochState = snapshot.get(snapshot.currentEpoch);
        Topology currentTopology = epochState.global.forKeys(keys);

        if (minEpoch == Long.MAX_VALUE)
            minEpoch = currentTopology.epoch();
        else
            Preconditions.checkArgument(minEpoch <= currentTopology.epoch() && minEpoch >= 0);

        if (currentTopology.epoch() == minEpoch && !epochs.requiresHistoricalTopologiesFor(keys))
        {
            return new Topologies.Singleton(currentTopology, true);
        }
        else
        {
            Topologies.Multi topologies = new Topologies.Multi(2);
            topologies.add(currentTopology);
            for (int i=1; i<snapshot.epochs.length; i++)
            {
                epochState = snapshot.epochs[i];
                if (i > 1 && epochState.syncCompleteFor(keys) && epochState.epoch() < minEpoch)
                    break;

                if (epochState.epoch() < minEpoch)
                    topologies.add(epochState.global.forKeys(keys, epochState::shardIsUnsynced));
                else
                    topologies.add(epochState.global.forKeys(keys));
            }
            return topologies;
        }
    }

    public Topologies forKeys(Keys keys)
    {
        return forKeys(keys, Long.MAX_VALUE);
    }

    public Topologies forEpoch(Keys keys, long epoch)
    {
        return new Topologies.Singleton(epochs.get(epoch).global.forKeys(keys), true);
    }

    public Topologies forTxn(Txn txn, long minEpoch)
    {
        return forKeys(txn.keys(), minEpoch);
    }

    public Topologies forTxn(Txn txn)
    {
        return forKeys(txn.keys(), Long.MAX_VALUE);
    }

    public Topologies forEpoch(Txn txn, long epoch)
    {
        return forEpoch(txn.keys(), epoch);
    }

    public long maxUnknownEpoch(Request request)
    {
        if (!(request instanceof TxnRequest))
            return 0;

        return epochs.maxUnknownEpoch(((TxnRequest) request).scope());
    }
}
