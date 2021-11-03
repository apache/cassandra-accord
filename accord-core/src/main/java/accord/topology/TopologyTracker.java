package accord.topology;

import accord.api.ConfigurationService;
import accord.coordinate.tracking.AbstractResponseTracker;
import accord.local.Node;
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
        public EpochAcknowledgementTracker(Topology topology)
        {
            super(topology, ShardEpochTracker[]::new, ShardEpochTracker::new);
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
            this.acknowledgements = new EpochAcknowledgementTracker(previous);
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
    }

    private static class Epochs
    {
        static final Epochs EMPTY = new Epochs(0, new EpochState[0]);

        final long minEpoch;
        final EpochState[] epochs;

        private Epochs(long minEpoch, EpochState[] epochs)
        {
            this.minEpoch = minEpoch;
            this.epochs = epochs;
        }

        public long nextEpoch()
        {
            return minEpoch + Math.max(epochs.length, 1);
        }

        public Topology current()
        {
            return epochs.length > 0 ? epochs[epochs.length - 1].topology : Topology.EMPTY;
        }

        public Epochs add(Topology topology)
        {
            Preconditions.checkArgument(topology.epoch == nextEpoch());
            EpochState[] nextEpochs = new EpochState[epochs.length + 1];
            System.arraycopy(epochs, 0, nextEpochs, 0, epochs.length);
            nextEpochs[nextEpochs.length - 1] = new EpochState(topology, current());
            return new Epochs(nextEpochs[0].epoch(), nextEpochs);
        }

        public void acknowledge(Node.Id node, long epoch)
        {
            for (int i=0; i<epochs.length && epochs[i].epoch() <= epoch; i++)
                epochs[i].recordAcknowledgement(node);
        }

        private EpochState get(long epoch)
        {
            if (epoch < minEpoch || epoch > minEpoch + epochs.length)
                return null;

            return epochs[(int) (epoch - minEpoch)];
        }
    }

    private final Node.Id thisNode;
    private Topology current = Topology.EMPTY;
    private volatile Epochs epochs = Epochs.EMPTY;

    public TopologyTracker(Node.Id thisNode)
    {
        this.thisNode = thisNode;
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

    Topology current()
    {
        return current;
    }

    @VisibleForTesting
    EpochState getEpochState(long epoch)
    {
        return epochs.get(epoch);
    }
}
