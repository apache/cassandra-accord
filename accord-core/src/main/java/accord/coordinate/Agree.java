package accord.coordinate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import accord.coordinate.tracking.FastPathTracker;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.txn.Ballot;
import accord.messages.Callback;
import accord.local.Node;
import accord.txn.Dependencies;
import accord.txn.Keys;
import accord.local.Node.Id;
import accord.txn.Timestamp;
import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptOk;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.messages.PreAccept.PreAcceptReply;
import com.google.common.collect.Sets;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 */
class Agree extends AcceptPhase implements Callback<PreAcceptReply>
{
    static class ShardTracker extends FastPathTracker.FastPathShardTracker
    {
        public ShardTracker(Shard shard)
        {
            super(shard);
        }

        @Override
        public boolean includeInFastPath(Node.Id node, boolean withFastPathTimestamp)
        {
            return withFastPathTimestamp && shard.fastPathElectorate.contains(node);
        }

        @Override
        public boolean hasMetFastPathCriteria()
        {
            return fastPathAccepts >= shard.fastPathQuorumSize;
        }
    }

    static class PreacceptTracker extends FastPathTracker<ShardTracker>
    {
        volatile long supersedingEpoch = -1;
        private final boolean fastPathPermitted;
        private final Set<Id> successes = new HashSet<>();
        private final Set<Id> failures = new HashSet<>();

        public PreacceptTracker(Topologies topologies, boolean fastPathPermitted)
        {
            super(topologies, Agree.ShardTracker[]::new, Agree.ShardTracker::new);
            this.fastPathPermitted = fastPathPermitted;
        }

        public PreacceptTracker(Topologies topologies)
        {
            this(topologies, topologies.fastPathPermitted());
        }

        @Override
        public void recordFailure(Id node)
        {
            failures.add(node);
            super.recordFailure(node);
        }

        @Override
        public void recordSuccess(Id node, boolean withFastPathTimestamp)
        {
            successes.add(node);
            super.recordSuccess(node, withFastPathTimestamp);
        }

        public void recordSuccess(Id node)
        {
            recordSuccess(node, false);
        }

        public synchronized boolean recordSupersedingEpoch(long epoch)
        {
            if (epoch <= supersedingEpoch)
                return false;
            supersedingEpoch = epoch;
            return true;
        }

        public boolean hasSupersedingEpoch()
        {
            return supersedingEpoch > 0;
        }

        public PreacceptTracker withUpdatedTopologies(Topologies topologies)
        {
            PreacceptTracker tracker = new PreacceptTracker(topologies, false);
            successes.forEach(tracker::recordSuccess);
            failures.forEach(tracker::recordFailure);
            return tracker;
        }

        @Override
        public boolean hasMetFastPathCriteria()
        {
            return fastPathPermitted && super.hasMetFastPathCriteria();
        }
    }

    final Keys keys;

    public enum PreacceptOutcome { COMMIT, ACCEPT }

    private PreacceptTracker tracker;

    private PreacceptOutcome preacceptOutcome;
    private final List<PreAcceptOk> preAcceptOks = new ArrayList<>();

    // TODO: hybrid fast path? or at least short-circuit accept if we gain a fast-path quorum _and_ proposed one by accept
    boolean permitHybridFastPath;

    private Agree(Node node, TxnId txnId, Txn txn)
    {
        super(node, Ballot.ZERO, txnId, txn);
        this.keys = txn.keys();
        tracker = new PreacceptTracker(node.topology().forKeys(txn.keys(), txnId.epoch));
        // TODO: consider sending only to electorate of most recent topology (as only these PreAccept votes matter)
        // note that we must send to all replicas of old topology, as electorate may not be reachable
        node.send(tracker.nodes(), to -> new PreAccept(to, tracker.topologies(), txnId, txn), this);
    }

    @Override
    public void onSuccess(Id from, PreAcceptReply response)
    {
        onPreAccept(from, response);
    }

    @Override
    public synchronized void onFailure(Id from, Throwable throwable)
    {
        if (isDone() || isPreAccepted())
            return;

        tracker.recordFailure(from);
        if (tracker.hasFailed())
            tryFailure(new Timeout());

        // if no other responses are expected and the slow quorum has been satisfied, proceed
        if (shouldSlowPathAccept())
            onPreAccepted();
    }

    private synchronized void onEpochUpdate()
    {
        if (!tracker.hasSupersedingEpoch())
            return;
        Topologies newTopologies = node.topology().forKeys(txn.keys(), txnId.epoch);
        if (newTopologies.currentEpoch() < tracker.supersedingEpoch)
            return;
        Set<Id> previousNodes = tracker.nodes();
        tracker = tracker.withUpdatedTopologies(newTopologies);

        // send messages to new nodes
        Set<Id> needMessages = Sets.difference(tracker.nodes(), previousNodes);
        if (!needMessages.isEmpty())
            node.send(needMessages, to -> new PreAccept(to, newTopologies, txnId, txn), this);

        if (shouldSlowPathAccept())
            onPreAccepted();
    }

    private synchronized void onPreAccept(Id from, PreAcceptReply receive)
    {
        if (isDone() || isPreAccepted())
            return;

        if (!receive.isOK())
        {
            // we've been preempted by a recovery coordinator; defer to it, and wait to hear any result
            tryFailure(new Preempted());
            return;
        }

        PreAcceptOk ok = (PreAcceptOk) receive;
        preAcceptOks.add(ok);

        boolean fastPath = ok.witnessedAt.compareTo(txnId) == 0;
        tracker.recordSuccess(from, fastPath);

        if (!fastPath && ok.witnessedAt.epoch > txnId.epoch)
        {
            if (tracker.recordSupersedingEpoch(ok.witnessedAt.epoch))
                node.configService().fetchTopologyForEpoch(ok.witnessedAt.epoch).addListener(this::onEpochUpdate);
        }

        if (!tracker.hasSupersedingEpoch() && (tracker.hasMetFastPathCriteria() || shouldSlowPathAccept()))
            onPreAccepted();
    }

    private void onPreAccepted()
    {
        if (tracker.hasMetFastPathCriteria())
        {
            preacceptOutcome = PreacceptOutcome.COMMIT;
            Dependencies deps = new Dependencies();
            for (PreAcceptOk preAcceptOk : preAcceptOks)
            {
                if (preAcceptOk.witnessedAt.equals(txnId))
                    deps.addAll(preAcceptOk.deps);
            }
            agreed(txnId, deps, tracker.topologies());
        }
        else
        {
            preacceptOutcome = PreacceptOutcome.ACCEPT;
            Timestamp executeAt = Timestamp.NONE;
            Dependencies deps = new Dependencies();
            for (PreAcceptOk preAcceptOk : preAcceptOks)
            {
                deps.addAll(preAcceptOk.deps);
                executeAt = Timestamp.max(executeAt, preAcceptOk.witnessedAt);
            }

            // TODO: perhaps don't submit Accept immediately if we almost have enough for fast-path,
            //       but by sending accept we rule out hybrid fast-path
            permitHybridFastPath = executeAt.compareTo(txnId) == 0;

            startAccept(executeAt, deps, tracker.topologies());
        }
    }

    private boolean shouldSlowPathAccept()
    {
        return (!tracker.fastPathPermitted || !tracker.hasInFlight()) && tracker.hasReachedQuorum();
    }

    private boolean isPreAccepted()
    {
        return preacceptOutcome != null;
    }

    static Future<Agreed> agree(Node node, TxnId txnId, Txn txn)
    {
        return new Agree(node, txnId, txn);
    }
}
