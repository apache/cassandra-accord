package accord.topology;

import accord.api.ConfigurationService;
import accord.api.KeyRange;
import accord.coordinate.tracking.AbstractResponseTracker;
import accord.local.Node;
import accord.txn.Keys;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.*;

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
 * TODO: support creating after epoch 0
 * TODO: rename
 */
public class TopologyTracker implements ConfigurationService.Listener
{
    public enum EpochStatus
    {
        ANNOUNCED,
        ACKNOWLEDGED,
        REPAIRED,
        READY,
        SUPERSEDED
    }

    static class ShardEpochTracker extends AbstractResponseTracker.ShardTracker
    {
        /*
         * Until a quorum of nodes has acknowledged a superseding configuration, they need to be included in the replica
         * set of operations
         */
        private int acknowledged = 0;
        private final Set<Node.Id> unacknowledged;

        /*
         * Until all intersecting nodes (ie those present in both epoch n and n+1) have repaired the operations of
         * epoch n, other nodes may not remove accord metadata for the previous epoch
         */
        private final Set<Node.Id> needRepair;

        public ShardEpochTracker(Shard shard)
        {
            super(shard);
            this.unacknowledged = new HashSet<>(shard.nodes);
            this.needRepair = new HashSet<>(shard.nodes);
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
    }

    private static class EpochAcknowledgementTracker extends AbstractResponseTracker<ShardEpochTracker>
    {
        public EpochAcknowledgementTracker(Topologies topologies)
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
    }

    static class EpochState
    {
        private final Topology topology;
        private final Topology previous;
        private final EpochAcknowledgementTracker acknowledgements;
        private boolean acknowledged = false;

        private void updateState()
        {
            acknowledged = acknowledgements.epochAcknowledged();
        }

        EpochState(Topology topology, Topology previous)
        {
            this.topology = topology;
            this.previous = previous;
            // FIXME: may need a separate tracker class
            this.acknowledgements = new EpochAcknowledgementTracker(new Topologies.Singleton(previous));
            updateState();
        }

        public void recordAcknowledgement(Node.Id node)
        {
            acknowledgements.recordAcknowledgement(node);
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

        boolean acknowledgedFor(Keys keys)
        {
            // TODO: check individual shards
            return acknowledged;
        }
    }

    private static class Epochs
    {
        static final Epochs EMPTY = new Epochs(new EpochState[0]);

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

        private EpochState get(long epoch)
        {
            if (epoch > maxEpoch || epoch < maxEpoch - epochs.length)
                return null;

            return epochs[(int) (maxEpoch - epoch)];
        }
    }

    private volatile Epochs epochs = Epochs.EMPTY;

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

    public Topology current()
    {
        return epochs.current();
    }

    public long epoch()
    {
        return current().epoch;
    }

    @VisibleForTesting
    EpochState getEpochState(long epoch)
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
            return new Topologies.Singleton(topology);
        }
        else
        {
            // TODO: use number of unacknowledged epochs for initial capacity
            Topologies.Multi topologies = new Topologies.Multi(2);
            topologies.add(topology);
            for (int i=1; i<current.epochs.length; i++)
            {
                epochState = current.epochs[i];
                topologies.add(epochState.topology.forKeys(keys));
                if (epochState.acknowledgedFor(keys))
                    break;
            }
            return topologies;
        }
    }

    /**
     * Return the nodes from all active epochs needed to service the intersection of the given
     * ranges and keys
     */
    public Set<Node.Id> nodesFor(KeyRanges ranges, Keys keys)
    {
        Topologies topologies = forKeys(keys);
        Set<Node.Id> result = new HashSet<>();
        for (int i=0,mi=topologies.size(); i<mi; i++)
        {
            Topology topology = topologies.get(i);
            for (Shard shard : topology)
            {
                // TODO: efficiency
                for (KeyRange range : ranges)
                {
                    if (range.compareIntersecting(shard.range) == 0)
                    {
                        result.addAll(shard.nodes);
                        break;
                    }
                }
            }
        }
        return result;
    }
}
