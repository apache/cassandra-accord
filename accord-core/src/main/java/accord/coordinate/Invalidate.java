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

import accord.coordinate.tracking.QuorumTracker;
import accord.local.SaveStatus;
import accord.primitives.*;
import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.BeginInvalidation;
import accord.messages.BeginInvalidation.InvalidateNack;
import accord.messages.BeginInvalidation.InvalidateOk;
import accord.messages.BeginInvalidation.InvalidateReply;
import accord.messages.Callback;
import accord.topology.Shard;

import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Fail;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Success;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.Status.*;
import static accord.local.Status.Known.Definition;
import static accord.messages.Commit.Invalidate.commitInvalidate;
import static accord.primitives.ProgressToken.INVALIDATED;

public class Invalidate implements Callback<InvalidateReply>
{
    private final Node node;
    private final Ballot ballot;
    private final TxnId txnId;
    private final RoutingKeys informKeys;
    private final RoutingKey invalidateWithKey;
    private final Status recoverIfAtLeast;
    private final BiConsumer<Outcome, Throwable> callback;

    private boolean isDone;
    private boolean isBallotPromised;
    private final List<InvalidateOk> invalidateOks = new ArrayList<>();
    private final QuorumTracker.QuorumShardTracker tracker;

    private Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, RoutingKeys informKeys, RoutingKey invalidateWithKey, Status recoverIfAtLeast, BiConsumer<Outcome, Throwable> callback)
    {
        this.callback = callback;
        Preconditions.checkArgument(informKeys.contains(invalidateWithKey));
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.informKeys = informKeys;
        this.invalidateWithKey = invalidateWithKey;
        this.recoverIfAtLeast = recoverIfAtLeast;
        this.tracker = new QuorumTracker.QuorumShardTracker(shard);
    }

    public static Invalidate invalidate(Node node, TxnId txnId, RoutingKeys informKeys, RoutingKey invalidateWithKey, BiConsumer<Outcome, Throwable> callback)
    {
        return invalidate(node, txnId, informKeys, invalidateWithKey, PreAccepted, callback);
    }

    // TODO (now): (file separately) this is a bug, as it is possible for a preaccept to still be in-flight.
    //             It's not even safe to do it after an initial Invalidate round unless we ensure that recovery never pre-accepts with txnId (which we should also enforce)
    public static Invalidate invalidateIfNotAccepted(Node node, TxnId txnId, RoutingKeys informKeys, RoutingKey invalidateWithKey, BiConsumer<Outcome, Throwable> callback)
    {
        return invalidate(node, txnId, informKeys, invalidateWithKey, Accepted, callback);
    }

    private static Invalidate invalidate(Node node, TxnId txnId, RoutingKeys informKeys, RoutingKey invalidateWithKey, Status recoverIfAtLeast, BiConsumer<Outcome, Throwable> callback)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        Shard shard = node.topology().forEpochIfKnown(invalidateWithKey, txnId.epoch);
        Invalidate invalidate = new Invalidate(node, shard, ballot, txnId, informKeys, invalidateWithKey, recoverIfAtLeast, callback);
        node.send(shard.nodes, to -> new BeginInvalidation(txnId, invalidateWithKey, ballot), invalidate);
        return invalidate;
    }

    @Override
    public synchronized void onSuccess(Id from, InvalidateReply reply)
    {
        if (isDone || isBallotPromised)
            return;

        if (!reply.isOk())
        {
            InvalidateNack nack = (InvalidateNack) reply;
            if (nack.homeKey != null)
            {
                node.ifLocalSince(contextFor(txnId), invalidateWithKey, txnId, safeStore -> {
                    safeStore.command(txnId).updateHomeKey(safeStore, nack.homeKey);
                }).addCallback(node.agent());
            }

            isDone = true;
            callback.accept(null, new Preempted(txnId, null));
            return;
        }

        InvalidateOk ok = (InvalidateOk) reply;
        invalidateOks.add(ok);
        if (tracker.onSuccess(from) == Success)
            invalidate();
    }

    private void invalidate()
    {
        Preconditions.checkState(!isBallotPromised);
        isBallotPromised = true;
        // first look to see if it has already been
        {
            Route route = InvalidateOk.findRoute(invalidateOks);
            RoutingKey homeKey = route != null ? route.homeKey : InvalidateOk.findHomeKey(invalidateOks);
            SaveStatus maxStatus = invalidateOks.stream().map(ok -> ok.status).max(Comparable::compareTo).orElseThrow(IllegalStateException::new);
            Known maxKnown = invalidateOks.stream().map(ok -> ok.status.known).max(Comparable::compareTo).orElseThrow(IllegalStateException::new);

            switch (maxStatus.status)
            {
                default: throw new IllegalStateException();
                case AcceptedInvalidate:
                    // latest accept also invalidating, so we're on the same page and should finish our invalidation
                case NotWitnessed:
                    break;

                case PreAccepted:
                case Accepted:
                    // note: we do not attempt to calculate PreAccept outcome here, we rely on the caller to tell us
                    // what is safe to do. If the caller knows no decision was reached with PreAccept, we can safely
                    // invalidate if we see PreAccept, and only need to recover if we see Accept
                    // TODO: if we see Accept, go straight to propose to save some unnecessary work
                    if (recoverIfAtLeast.compareTo(maxStatus.status) > 0)
                        break;

                case Committed:
                case ReadyToExecute:
                case PreApplied:
                case Applied:
                    // TODO: if we see Committed or above, go straight to Execute if we have assembled enough information
                    if (route != null)
                    {
                        RecoverWithRoute.recover(node, ballot, txnId, route, callback);
                    }
                    else if (homeKey != null)
                    {
                        if (homeKey.equals(invalidateWithKey) && maxKnown.compareTo(Definition) >= 0)
                            throw new IllegalStateException("Received a reply from a node that must have known the route, but that did not include it");

                        RecoverWithHomeKey.recover(node, txnId, homeKey, callback);
                    }
                    else
                    {
                        throw new IllegalStateException("Received a reply from a node that must have known the homeKey, but that did not include it");
                    }
                    return;

                case Invalidated:
                    isDone = true;
                    node.forEachLocalSince(contextFor(txnId), informKeys, txnId, safeStore -> {
                        safeStore.command(txnId).commitInvalidate(safeStore);
                    }).addCallback((success, fail) -> {
                        callback.accept(INVALIDATED, null);
                    });
                    return;
            }
        }

        // if we have witnessed the transaction, but are able to invalidate, do we want to proceed?
        // Probably simplest to do so, but perhaps better for user if we don't.
        proposeInvalidate(node, ballot, txnId, invalidateWithKey, (success, fail) -> {
            /**
             * We're now inside our *exactly once* callback we registered with proposeInvalidate, and we need to
             * make sure we honour our own exactly once semantics with {@code callback}.
             * So we are responsible for all exception handling.
             */
            isDone = true;
            if (fail != null)
            {
                callback.accept(null, fail);
                return;
            }

            try
            {
                AbstractRoute route = InvalidateOk.mergeRoutes(invalidateOks);
                // TODO: commitInvalidate (and others) should skip the network for local applications,
                //  so we do not need to explicitly do so here before notifying the waiter
                commitInvalidate(node, txnId, route != null ? route : informKeys, txnId);
                // TODO: pick a reasonable upper bound, so we don't invalidate into an epoch/commandStore that no longer cares about this command
                node.forEachLocalSince(contextFor(txnId), informKeys, txnId, safeStore -> {
                    safeStore.command(txnId).commitInvalidate(safeStore);
                }).addCallback((s, f) -> {
                    callback.accept(INVALIDATED, null);
                    if (f != null) // TODO: consider exception handling more carefully: should we catch these prior to passing to callbacks?
                        node.agent().onUncaughtException(f);
                });
            }
            catch (Throwable t)
            {
                callback.accept(null, t);
            }
        });
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone || isBallotPromised)
            return;

        if (tracker.onFailure(from) == Fail)
        {
            isDone = true;
            callback.accept(null, new Timeout(txnId, null));
        }
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        isDone = true;
        callback.accept(null, failure);
    }
}
