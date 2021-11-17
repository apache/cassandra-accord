package accord.topology;

import accord.api.ConfigurationService;
import accord.coordinate.tracking.AbstractResponseTracker;
import accord.local.Node;
import accord.messages.Request;
import accord.messages.TxnRequest;
import accord.messages.TxnRequestScope;
import accord.txn.Keys;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

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
    static class ShardEpochTracker extends AbstractResponseTracker.ShardTracker
    {
        /*
         * Until a quorum of nodes has acknowledged a superseding configuration, they need to be included in the replica
         * set of operations
         */
        private int acknowledged = 0;
        private final Set<Node.Id> unacknowledged;

        private int syncComplete = 0;
        private final Set<Node.Id> pendingSync;

        public ShardEpochTracker(Shard shard)
        {
            super(shard);
            this.unacknowledged = new HashSet<>(shard.nodes);
            this.pendingSync = new HashSet<>(shard.nodes);
        }

        public boolean acknowledge(Node.Id id)
        {
            if (!unacknowledged.remove(id))
                return false;
            acknowledged++;
            return true;
        }

        public boolean quorumAcknowledged()
        {
            return acknowledged >= shard.slowPathQuorumSize;
        }

        public boolean syncComplete(Node.Id id)
        {
            if (!pendingSync.remove(id))
                return false;
            syncComplete++;
            return true;
        }

        public boolean quorumSyncComplete()
        {
            return syncComplete >= shard.slowPathQuorumSize;
        }
    }

    private static class EpochTracker extends AbstractResponseTracker<ShardEpochTracker>
    {
        public EpochTracker(Topologies topologies)
        {
            super(topologies, ShardEpochTracker[]::new, ShardEpochTracker::new);
        }

        public void recordAcknowledgement(Node.Id node)
        {
            forEachTrackerForNode(node, ShardEpochTracker::acknowledge);
        }

        public boolean epochAcknowledged()
        {
            return all(ShardEpochTracker::quorumAcknowledged);
        }

        public void recordSyncComplete(Node.Id node)
        {
            forEachTrackerForNode(node, ShardEpochTracker::syncComplete);
        }

        public boolean epochSynced()
        {
            return all(ShardEpochTracker::quorumSyncComplete);
        }
    }

    class EpochState
    {
        private final Topology topology;
        private final Topology local;
        private final Topology previous;
        private final EpochTracker tracker;
        private boolean acknowledged = false;
        private boolean syncComplete = false;

        private void updateState()
        {
            acknowledged = tracker.epochAcknowledged();
            syncComplete = tracker.epochSynced();
        }

        EpochState(Topology topology, Topology previous)
        {
            Preconditions.checkArgument(!topology.isSubset());
            Preconditions.checkArgument(!previous.isSubset());
            this.topology = topology;
            this.local = topology.forNode(node);
            this.previous = previous;
            this.tracker = new EpochTracker(new Topologies.Singleton(previous, false));
            updateState();
        }

        public void recordAcknowledgement(Node.Id node)
        {
            tracker.recordAcknowledgement(node);
            updateState();
        }

        public void recordSyncComplete(Node.Id node)
        {
            tracker.recordSyncComplete(node);
            updateState();
        }

        long epoch()
        {
            return topology.epoch;
        }

        boolean acknowledged()
        {
            return acknowledged;
        }

        /**
         * determine if all shards intersecting with the given keys have acknowledged the new epoch
         */
        boolean acknowledgedFor(Keys keys)
        {
            if (acknowledged)
                return true;
            Boolean result = previous.accumulateForKeys(keys, (i, shard, acc) -> {
                if (acc == Boolean.FALSE)
                    return acc;
                ShardEpochTracker shardTracker = tracker.unsafeGet(i);
                return Boolean.valueOf(shardTracker.quorumAcknowledged());
            }, Boolean.TRUE);
            return result == Boolean.TRUE;
        }

        /**
         * determine if the given nodes has acknowledged the new epoch
         */
        boolean acknowledgedBy(Node.Id node)
        {
            return previous.matchesOn(node, (i, shard) -> {
                ShardEpochTracker shardTracker = tracker.unsafeGet(i);
                return shardTracker.unacknowledged.contains(node);
            }) == 0;
        }

        boolean shardIsUnacknowledged(int idx, Shard shard)
        {
            return !tracker.unsafeGet(idx).quorumAcknowledged();
        }

        boolean syncComplete()
        {
            return syncComplete;
        }

        /**
         * determine if sync has completed for all shards intersecting with the given keys
         */
        boolean syncCompleteFor(Keys keys)
        {
            if (syncComplete)
                return true;
            Boolean result = previous.accumulateForKeys(keys, (i, shard, acc) -> {
                if (acc == Boolean.FALSE)
                    return acc;
                ShardEpochTracker shardTracker = tracker.unsafeGet(i);
                return Boolean.valueOf(shardTracker.quorumSyncComplete());
            }, Boolean.TRUE);
            return result == Boolean.TRUE;
        }
    }

    private class Epochs
    {

        private final long maxEpoch;
        private final long minEpoch;
        private final EpochState[] epochs;

        private Epochs(EpochState[] epochs)
        {
            this.maxEpoch = epochs.length > 0 ? epochs[0].epoch() : 0;
            for (int i=1; i<epochs.length; i++)
                Preconditions.checkArgument(epochs[i].epoch() == epochs[i-1].epoch() - 1);
            this.minEpoch = epochs.length > 0 ? epochs[epochs.length - 1].epoch() : 0;
            this.epochs = epochs;
        }

        public long nextEpoch()
        {
            return current().epoch + 1;
        }

        public Topology current()
        {
            return epochs.length > 0 ? epochs[0].topology : Topology.EMPTY;
        }

        public Epochs add(Topology topology)
        {
            Preconditions.checkArgument(topology.epoch == nextEpoch());
            EpochState[] nextEpochs = new EpochState[epochs.length + 1];
            System.arraycopy(epochs, 0, nextEpochs, 1, epochs.length);
            nextEpochs[0] = new EpochState(topology, current());
            return new Epochs(nextEpochs);
        }

        public void acknowledge(Node.Id node, long epoch)
        {
            for (int i=0; i<epochs.length && epochs[i].epoch() <= epoch; i++)
                epochs[i].recordAcknowledgement(node);
        }

        public void syncComplete(Node.Id node, long epoch)
        {
            for (int i=0; i<epochs.length && epochs[i].epoch() <= epoch; i++)
                epochs[i].recordSyncComplete(node);
        }

        private EpochState get(long epoch)
        {
            if (epoch > maxEpoch || epoch < maxEpoch - epochs.length)
                return null;

            return epochs[(int) (maxEpoch - epoch)];
        }

        long canProcess(TxnRequestScope scope)
        {
            EpochState lastState = null;
            long missingEpoch = 0;
            for (int i=0, mi=scope.size(); i<mi; i++)
            {
                TxnRequestScope.EpochRanges requestRanges = scope.get(i);
                EpochState epochState = get(requestRanges.epoch);

                if (epochState != null)
                {
                    lastState = epochState;
                }
                else if (lastState != null && requestRanges.ranges.difference(lastState.local.ranges()).isEmpty())
                {
                    // we don't have the most recent epoch, but still replicate the requested ranges
                    missingEpoch = requestRanges.epoch;
                    continue;
                }
                else
                {
                    // we don't have the most recent epoch, and we don't replicate the requested ranges
                    return scope.epoch();
                }

                // validate requested ranges
                KeyRanges localRanges = epochState.local.ranges();
                if (!requestRanges.ranges.difference(localRanges).isEmpty())
                    throw new RuntimeException("Received request for ranges not replicated by this node");
            }
            if (missingEpoch > 0)
                missingEpochNotify.accept(missingEpoch);

            return 0;
        }
    }

    private final Node.Id node;
    private final LongConsumer missingEpochNotify;
    private volatile Epochs epochs;

    public TopologyManager(Node.Id node, LongConsumer missingEpochNotify)
    {
        this.node = node;
        this.missingEpochNotify = missingEpochNotify;
        this.epochs = new Epochs(new EpochState[0]);
    }

    @Override
    public synchronized void onTopologyUpdate(Topology topology)
    {
        epochs = epochs.add(topology);
    }

    @Override
    public void onEpochAcknowledgement(Node.Id node, long epoch)
    {
        epochs.acknowledge(node, epoch);
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

    public Topologies forKeys(Keys keys)
    {
        Epochs current = epochs;
        long maxEpoch = current.maxEpoch;

        EpochState epochState = current.get(maxEpoch);
        Topology topology = epochState.topology.forKeys(keys);
        if (epochState.acknowledgedFor(keys))
        {
            return new Topologies.Singleton(topology, epochState.syncCompleteFor(keys));
        }
        else
        {
            Topologies.Multi topologies = new Topologies.Multi(2);
            topologies.add(topology);
            for (int i=1; i<current.epochs.length; i++)
            {
                // FIXME: again, this is confusing
                EpochState nextState = current.epochs[i-1];
                epochState = current.epochs[i];
                topologies.add(nextState.previous.forKeys(keys, nextState::shardIsUnacknowledged));
                if (epochState.acknowledgedFor(keys))
                    break;
            }
            return topologies;
        }
    }

    public boolean epochAcknowledgedBy(Node.Id node, long epoch)
    {
        EpochState epochState = epochs.get(epoch);
        return epochState.acknowledgedBy(node);
    }

    public Predicate<Node.Id> epochAcknowledgedPredicate(long epoch)
    {
        return id -> epochAcknowledgedBy(id, epoch);
    }

    public long canProcess(Request request)
    {
        if (!(request instanceof TxnRequest))
            return 0;

        return epochs.canProcess(((TxnRequest) request).scope());
    }
}
