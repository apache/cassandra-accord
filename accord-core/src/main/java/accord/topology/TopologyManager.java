package accord.topology;

import accord.api.ConfigurationService;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.messages.Request;
import accord.messages.TxnRequest;
import accord.messages.TxnRequestScope;
import accord.txn.Keys;
import accord.txn.Txn;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

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
    class EpochState
    {
        private final Topology topology;
        private final Topology local;
        private final QuorumTracker syncTracker;
        private boolean syncComplete = false;

        private void updateState()
        {
            syncComplete = syncTracker.hasReachedQuorum();
        }

        EpochState(Topology topology)
        {
            Preconditions.checkArgument(!topology.isSubset());
            this.topology = topology;
            this.local = topology.forNode(node);
            this.syncTracker = new QuorumTracker(new Topologies.Singleton(topology, false));
            updateState();
        }

        public void recordSyncComplete(Node.Id node)
        {
            syncTracker.recordSuccess(node);
            updateState();
        }

        long epoch()
        {
            return topology.epoch;
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
            Boolean result = topology.accumulateForKeys(keys, (i, shard, acc) -> {
                if (acc == Boolean.FALSE)
                    return acc;
                return Boolean.valueOf(syncTracker.unsafeGet(i).hasReachedQuorum());
            }, Boolean.TRUE);
            return result == Boolean.TRUE;
        }

        boolean shardIsUnsynced(int idx, Shard shard)
        {
            return !syncTracker.unsafeGet(idx).hasReachedQuorum();
        }
    }

    private class Epochs
    {
        private final long maxEpoch;
        private final long minEpoch;
        private final EpochState[] epochs;
        private final List<Set<Node.Id>> pendingSyncComplete;

        private Epochs(EpochState[] epochs, List<Set<Node.Id>> pendingSyncComplete)
        {
            this.maxEpoch = epochs.length > 0 ? epochs[0].epoch() : 0;
            this.pendingSyncComplete = pendingSyncComplete;
            for (int i=1; i<epochs.length; i++)
                Preconditions.checkArgument(epochs[i].epoch() == epochs[i-1].epoch() - 1);
            this.minEpoch = epochs.length > 0 ? epochs[epochs.length - 1].epoch() : 0;
            this.epochs = epochs;
        }

        private Epochs(EpochState[] epochs)
        {
            this(epochs, new ArrayList<>());
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
            List<Set<Node.Id>> pendingSync = pendingSyncComplete;
            if (!pendingSync.isEmpty())
            {
                EpochState currentEpoch = epochs[0];
                pendingSync.remove(0).forEach(currentEpoch::recordSyncComplete);
            }
            System.arraycopy(epochs, 0, nextEpochs, 1, epochs.length);

            EpochState nextEpochState = new EpochState(topology);
            nextEpochs[0] = nextEpochState;
            return new Epochs(nextEpochs, pendingSync);
        }

        public void syncComplete(Node.Id node, long epoch)
        {
            for (long e=minEpoch; e<=epoch; e++)
            {
                if (e > maxEpoch - 1)
                {
                    int idx = (int) (e - maxEpoch);
                    for (int i=pendingSyncComplete.size(); i<=idx; i++)
                        pendingSyncComplete.add(new HashSet<>());

                    pendingSyncComplete.get(idx).add(node);
                }
                else
                {
                    get(e).recordSyncComplete(node);
                }
            }
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
                    continue;
                }
                else
                {
                    // we don't have the most recent epoch, and we don't replicate the requested ranges
                    return scope.maxEpoch();
                }

                // validate requested ranges
                KeyRanges localRanges = epochState.local.ranges();
                if (!requestRanges.ranges.difference(localRanges).isEmpty())
                    throw new RuntimeException("Received request for ranges not replicated by this node");
            }
            if (scope.maxEpoch() > 0)
                missingEpochNotify.accept(scope.maxEpoch());

            return 0;
        }

        boolean requiresHistoricalTopologiesFor(Keys keys)
        {
            return epochs.length > 1 && !epochs[1].syncCompleteFor(keys);
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
        if (!epochs.requiresHistoricalTopologiesFor(keys))
        {
            return new Topologies.Singleton(topology, true);
        }
        else
        {
            Topologies.Multi topologies = new Topologies.Multi(2);
            topologies.add(topology);
            for (int i=1; i<current.epochs.length; i++)
            {
                epochState = current.epochs[i];
                if (i > 1 && epochState.syncCompleteFor(keys))
                    break;
                topologies.add(epochState.topology.forKeys(keys, epochState::shardIsUnsynced));
            }
            return topologies;
        }
    }

    public Topologies forTxn(Txn txn)
    {
        return forKeys(txn.keys());
    }

    public long canProcess(Request request)
    {
        if (!(request instanceof TxnRequest))
            return 0;

        return epochs.canProcess(((TxnRequest) request).scope());
    }
}
