package accord.coordinate;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.tracking.FastPathTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptOk;
import accord.messages.PreAccept.PreAcceptReply;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.txn.*;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 */
public class Initiate implements Callback<PreAcceptReply>
{
    static class PreAcceptShardTracker extends FastPathTracker.FastPathShardTracker
    {
        public PreAcceptShardTracker(Shard shard)
        {
            super(shard);
        }

        @Override
        public boolean includeInFastPath(Id node, boolean withFastPathTimestamp)
        {
            return withFastPathTimestamp && shard.fastPathElectorate.contains(node);
        }

        @Override
        public boolean hasMetFastPathCriteria()
        {
            return fastPathAccepts >= shard.fastPathQuorumSize;
        }
    }

    static class PreAcceptTracker extends FastPathTracker<PreAcceptShardTracker>
    {
        volatile long supersedingEpoch = -1;
        private final boolean fastPathPermitted;
        private final Set<Id> successes = new HashSet<>();
        private Set<Id> failures;

        public PreAcceptTracker(Topologies topologies, boolean fastPathPermitted)
        {
            super(topologies, PreAcceptShardTracker[]::new, PreAcceptShardTracker::new);
            this.fastPathPermitted = fastPathPermitted;
        }

        public PreAcceptTracker(Topologies topologies)
        {
            this(topologies, topologies.fastPathPermitted());
        }

        @Override
        public boolean failure(Id node)
        {
            if (failures == null)
                failures = new HashSet<>();
            failures.add(node);
            return super.failure(node);
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

        public PreAcceptTracker withUpdatedTopologies(Topologies topologies)
        {
            PreAcceptTracker tracker = new PreAcceptTracker(topologies, false);
            successes.forEach(tracker::recordSuccess);
            if (failures != null)
                failures.forEach(tracker::failure);
            return tracker;
        }

        @Override
        public boolean hasMetFastPathCriteria()
        {
            return fastPathPermitted && super.hasMetFastPathCriteria();
        }

        boolean shouldSlowPathAccept()
        {
            return (!fastPathPermitted || !hasInFlight()) && hasReachedQuorum();
        }
    }

    final Node node;
    final TxnId txnId;
    final Txn txn;
    final Key homeKey;
    final BiConsumer<Result, Throwable> callback;

    private PreAcceptTracker tracker;
    private final List<PreAcceptOk> preAcceptOks = new ArrayList<>();
    private boolean isDone;

    // TODO: hybrid fast path? or at least short-circuit accept if we gain a fast-path quorum _and_ proposed one by accept
    boolean permitHybridFastPath;

    Initiate(Node node, TxnId txnId, Txn txn, Key homeKey, BiConsumer<Result, Throwable> callback)
    {
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.callback = callback;
        Topologies topologies = node.topology().withUnsyncedEpochs(txn.keys(), txnId.epoch, txnId.epoch);
        this.tracker = new PreAcceptTracker(topologies);
    }

    void start()
    {
        // TODO: consider sending only to electorate of most recent topology (as only these PreAccept votes matter)
        // note that we must send to all replicas of old topology, as electorate may not be reachable
        node.send(tracker.nodes(), to -> new PreAccept(to, tracker.topologies(), txnId, txn, homeKey), this);
    }

    @Override
    public synchronized void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        if (tracker.failure(from))
        {
            isDone = true;
            callback.accept(null, new Timeout(txnId, homeKey));
        }

        // if no other responses are expected and the slow quorum has been satisfied, proceed
        if (tracker.shouldSlowPathAccept())
            onPreAccepted();
    }

    @Override
    public synchronized void onCallbackFailure(Throwable failure)
    {
        isDone = true;
        callback.accept(null, failure);
    }

    // TODO (soon): do we need to preaccept in later epochs? the sync logic should take care of it for us, since
    //              either we haven't synced between a majority and the earlier epochs are still involved for
    //              later preaccepts, or they have been sync'd and the earlier transactions are known to the later epochs
    private synchronized void onEpochUpdate()
    {
        if (!tracker.hasSupersedingEpoch())
            return;
        Topologies newTopologies = node.topology().withUnsyncedEpochs(txn.keys(), txnId.epoch, tracker.supersedingEpoch);
        if (newTopologies.currentEpoch() < tracker.supersedingEpoch)
            return;
        Set<Id> previousNodes = tracker.nodes();
        tracker = tracker.withUpdatedTopologies(newTopologies);

        // send messages to new nodes
        Set<Id> needMessages = Sets.difference(tracker.nodes(), previousNodes);
        if (!needMessages.isEmpty())
            node.send(needMessages, to -> new PreAccept(to, newTopologies, txnId, txn, homeKey), this);

        if (tracker.shouldSlowPathAccept())
            onPreAccepted();
    }

    public synchronized void onSuccess(Id from, PreAcceptReply receive)
    {
        if (isDone)
            return;

        if (!receive.isOK())
        {
            // we've been preempted by a recovery coordinator; defer to it, and wait to hear any result
            isDone = true;
            callback.accept(null, new Preempted(txnId, homeKey));
            return;
        }

        PreAcceptOk ok = (PreAcceptOk) receive;
        preAcceptOks.add(ok);

        boolean fastPath = ok.witnessedAt.compareTo(txnId) == 0;
        tracker.recordSuccess(from, fastPath);

        // TODO: we should only update epoch if we need to in order to reach quorum
        if (!fastPath && ok.witnessedAt.epoch > txnId.epoch && tracker.recordSupersedingEpoch(ok.witnessedAt.epoch))
        {
            node.configService().fetchTopologyForEpoch(ok.witnessedAt.epoch);
            node.topology().awaitEpoch(ok.witnessedAt.epoch).addListener(this::onEpochUpdate);
        }

        if (!tracker.hasSupersedingEpoch() && (tracker.hasMetFastPathCriteria() || tracker.shouldSlowPathAccept()))
            onPreAccepted(); // note, can already have invoked onPreAccepted in onEpochUpdate
    }

    private void onPreAccepted()
    {
        if (isDone)
            return;

        isDone = true;
        if (tracker.hasMetFastPathCriteria())
        {
            isDone = true;
            Dependencies deps = new Dependencies();
            for (PreAcceptOk preAcceptOk : preAcceptOks)
            {
                if (preAcceptOk.witnessedAt.equals(txnId))
                    deps.addAll(preAcceptOk.deps);
            }

            Execute.execute(node, txnId, txn, homeKey, txnId, deps, callback);
        }
        else
        {
            Dependencies deps = new Dependencies();
            Timestamp executeAt; {
                Timestamp tmp = Timestamp.NONE;
                for (PreAcceptOk preAcceptOk : preAcceptOks)
                {
                    deps.addAll(preAcceptOk.deps);
                    tmp = Timestamp.max(tmp, preAcceptOk.witnessedAt);
                }
                executeAt = tmp;
            }

            // TODO: perhaps don't submit Accept immediately if we almost have enough for fast-path,
            //       but by sending accept we rule out hybrid fast-path
            permitHybridFastPath = executeAt.compareTo(txnId) == 0;
            node.withEpoch(executeAt.epoch, () -> Propose.propose(node, tracker.topologies(), Ballot.ZERO, txnId, txn, homeKey, executeAt, deps, callback));
        }
    }
}
