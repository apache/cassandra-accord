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

package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.Invalidate.Outcome;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.TxnOperation;
import accord.messages.BeginInvalidate;
import accord.messages.BeginInvalidate.InvalidateNack;
import accord.messages.BeginInvalidate.InvalidateOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.topology.Shard;
import accord.primitives.Ballot;
import accord.primitives.Keys;
import accord.primitives.TxnId;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.messages.BeginRecovery.RecoverOk.maxAcceptedOrLater;

public class Invalidate extends AsyncFuture<Outcome> implements Callback<RecoverReply>, BiConsumer<Result, Throwable>
{
    public enum Outcome { PREEMPTED, EXECUTED, INVALIDATED }

    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final Keys someKeys;
    final Key someKey;

    final List<Id> invalidateOksFrom = new ArrayList<>();
    final List<InvalidateOk> invalidateOks = new ArrayList<>();
    final QuorumShardTracker preacceptTracker;

    private Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, Keys someKeys, Key someKey)
    {
        Preconditions.checkArgument(someKeys.contains(someKey));
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.someKeys = someKeys;
        this.someKey = someKey;
        this.preacceptTracker = new QuorumShardTracker(shard);
    }

    public static Invalidate invalidate(Node node, TxnId txnId, Keys someKeys, Key someKey)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        Shard shard = node.topology().forEpochIfKnown(someKey, txnId.epoch);
        Invalidate invalidate = new Invalidate(node, shard, ballot, txnId, someKeys, someKey);
        node.send(shard.nodes, to -> new BeginInvalidate(txnId, someKey, ballot), invalidate);
        return invalidate;
    }

    @Override
    public synchronized void onSuccess(Id from, RecoverReply response)
    {
        if (isDone() || preacceptTracker.hasReachedQuorum())
            return;

        if (!response.isOK())
        {
            InvalidateNack nack = (InvalidateNack) response;
            if (nack.homeKey != null && nack.txn != null)
            {
                Key progressKey = node.trySelectProgressKey(txnId.epoch, nack.txn.keys(), nack.homeKey);
                // TODO: consider limiting epoch upper bound we process this status for
                node.ifLocalSince(TxnOperation.scopeFor(txnId, nack.txn.keys()), someKey, txnId, instance -> {
                    instance.command(txnId).preaccept(nack.txn, nack.homeKey, progressKey);
                    return null;
                });
            }
            else if (nack.homeKey != null)
            {
                // TODO: consider limiting epoch upper bound we process this status for
                node.ifLocalSince(TxnOperation.scopeFor(txnId), someKey, txnId, instance -> {
                    instance.command(txnId).homeKey(nack.homeKey);
                    return null;
                });
            }
            trySuccess(Outcome.PREEMPTED);
            return;
        }

        InvalidateOk ok = (InvalidateOk) response;
        invalidateOks.add(ok);
        invalidateOksFrom.add(from);
        if (preacceptTracker.success(from))
            invalidate();
    }

    private void invalidate()
    {
        // first look to see if it has already been
        InvalidateOk acceptOrCommit = maxAcceptedOrLater(invalidateOks);
        if (acceptOrCommit != null)
        {
            switch (acceptOrCommit.status)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                    throw new IllegalStateException("Should only have Accepted or later statuses here");
                case Accepted:
                    // note: we do not propagate our responses to the Recover instance to avoid mistakes;
                    //       since invalidate contacts only one key, only responses from nodes that replicate
                    //       *only* that key for the transaction will be valid, as the shards on the replica
                    //       that own the other keys may not have responded. It would be possible to filter
                    //       replies now that we have the transaction, but safer to just start from scratch.
                    Recover.recover(node, ballot, txnId, acceptOrCommit.txn, acceptOrCommit.homeKey)
                           .addCallback(this);
                    return;
                case AcceptedInvalidate:
                    break; // latest accept also invalidating, so we're on the same page and should finish our invalidation
                case Committed:
                case ReadyToExecute:
                    node.withEpoch(acceptOrCommit.executeAt.epoch, () -> {
                        Execute.execute(node, txnId, acceptOrCommit.txn, acceptOrCommit.homeKey, acceptOrCommit.executeAt,
                                        acceptOrCommit.deps, this);
                    });
                    return;
                case Executed:
                case Applied:
                    node.withEpoch(acceptOrCommit.executeAt.epoch, () -> {
                        Persist.persistAndCommit(node, txnId, acceptOrCommit.homeKey, acceptOrCommit.txn, acceptOrCommit.executeAt,
                                        acceptOrCommit.deps, acceptOrCommit.writes, acceptOrCommit.result);
                        trySuccess(Outcome.EXECUTED);
                    });
                    return;
                case Invalidated:
                    trySuccess(Outcome.INVALIDATED);
                    return;
            }
        }

        // if we have witnessed the transaction, but are able to invalidate, do we want to proceed?
        // Probably simplest to do so, but perhaps better for user if we don't.
        proposeInvalidate(node, ballot, txnId, someKey).addCallback((success, fail) -> {
            if (fail != null)
            {
                tryFailure(fail);
                return;
            }

            try
            {
                Commit.commitInvalidate(node, txnId, someKeys, txnId);
                node.forEachLocalSince(TxnOperation.scopeFor(txnId, someKeys), someKeys, txnId, instance -> {
                    instance.command(txnId).commitInvalidate();
                });
            }
            finally
            {
                trySuccess(Outcome.INVALIDATED);
            }
        });
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone())
            return;

        if (preacceptTracker.failure(from))
            tryFailure(new Timeout(txnId, null));
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        tryFailure(failure);
    }

    @Override
    public void accept(Result result, Throwable fail)
    {
        if (fail != null)
        {
            if (fail instanceof Invalidated) trySuccess(Outcome.INVALIDATED);
            else tryFailure(fail);
        }
        else
        {
            node.agent().onRecover(node, result, fail);
            trySuccess(Outcome.EXECUTED);
        }
    }
}
