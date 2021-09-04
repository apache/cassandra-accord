package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import accord.messages.Preempted;
import accord.messages.Timeout;
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

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 */
class Agree extends AcceptPhase implements Callback<PreAcceptReply>
{
    final Keys keys;

    public enum PreacceptOutcome { COMMIT, ACCEPT }

    // TODO: handle reconfigurations
    private int[] preAccepts;
    private int[] fastPathPreAccepts;
    private int[] failures;
    private int[] responsesOutstanding;

    private int preAccepted;
    private int fastPathAccepted;
    private int noOutstandingResponses;
    private PreacceptOutcome preacceptOutcome;
    private final List<PreAcceptOk> preAcceptOks = new ArrayList<>();

    // TODO: hybrid fast path? or at least short-circuit accept if we gain a fast-path quorum _and_ proposed one by accept
    boolean permitHybridFastPath;

    private Agree(Node node, TxnId txnId, Txn txn)
    {
        super(node, Ballot.ZERO, txnId, txn, node.cluster().forKeys(txn.keys()));
        this.keys = txn.keys();
        this.failures = new int[shards.size()];
        this.preAccepts = new int[shards.size()];
        this.fastPathPreAccepts = new int[shards.size()];
        this.responsesOutstanding = new int[shards.size()];
        shards.forEach((i, shard) -> {
            this.responsesOutstanding[i] = shard.nodes.size();
        });


        node.send(shards, new PreAccept(txnId, txn), this);
    }

    private void messageReceived(int shard)
    {
        if (--responsesOutstanding[shard] == 0)
            noOutstandingResponses++;
    }

    @Override
    public void onSuccess(Id from, PreAcceptReply response)
    {
        onPreAccept(from, response);
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        if (isDone() || isPreAccepted())
            return;

        shards.forEachOn(from, (i, shard) -> {
            messageReceived(i);
            if (++failures[i] >= shard.slowPathQuorumSize)
                completeExceptionally(new Timeout());
        });

        // if no other responses are expected and the slow quorum has been satisfied, proceed
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
            completeExceptionally(new Preempted());
            return;
        }

        PreAcceptOk ok = (PreAcceptOk) receive;
        preAcceptOks.add(ok);

        boolean fastPath = ok.witnessedAt.compareTo(txnId) == 0;
        shards.forEachOn(from, (i, shard) -> {
            messageReceived(i);
            if (fastPath && shard.fastPathElectorate.contains(from) && ++fastPathPreAccepts[i] == shard.fastPathQuorumSize)
                ++fastPathAccepted;

            if (++preAccepts[i] == shard.slowPathQuorumSize)
                ++preAccepted;
        });

        if (isFastPathAccepted() || shouldSlowPathAccept())
            onPreAccepted();
    }

    private void onPreAccepted()
    {
        if (isFastPathAccepted())
        {
            preacceptOutcome = PreacceptOutcome.COMMIT;
            Dependencies deps = new Dependencies();
            for (PreAcceptOk preAcceptOk : preAcceptOks)
            {
                if (preAcceptOk.witnessedAt.equals(txnId))
                    deps.addAll(preAcceptOk.deps);
            }
            agreed(txnId, deps);
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

            startAccept(executeAt, deps);
        }
    }

    private boolean isFastPathAccepted()
    {
        return fastPathAccepted == shards.size();
    }

    private boolean shouldSlowPathAccept()
    {
        return noOutstandingResponses == shards.size() && preAccepted == shards.size();
    }

    private boolean isPreAccepted()
    {
        return preacceptOutcome != null;
    }

    static CompletionStage<Agreed> agree(Node node, TxnId txnId, Txn txn)
    {
        return new Agree(node, txnId, txn);
    }
}
