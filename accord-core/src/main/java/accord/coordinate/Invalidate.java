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

import java.util.function.BiConsumer;

import accord.coordinate.tracking.InvalidationTracker;
import accord.coordinate.tracking.InvalidationTracker.InvalidationShardTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node.Id;
import accord.messages.Commit;
import accord.local.*;
import accord.primitives.*;
import accord.topology.Shard;
import accord.topology.Topologies;

import accord.api.RoutingKey;
import accord.messages.BeginInvalidation;
import accord.messages.BeginInvalidation.InvalidateReply;
import accord.messages.Callback;
import accord.topology.Topology;
import accord.utils.Invariants;

import javax.annotation.Nullable;

import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.Status.Accepted;
import static accord.local.Status.PreAccepted;
import static accord.primitives.ProgressToken.INVALIDATED;
import static accord.primitives.ProgressToken.TRUNCATED_DURABLE_OR_INVALIDATED;
import static accord.utils.Invariants.illegalState;

public class Invalidate implements Callback<InvalidateReply>
{
    private final Node node;
    private final Ballot ballot;
    private final TxnId txnId;
    private final Unseekables<?> invalidateWith;
    private final BiConsumer<Outcome, Throwable> callback;

    private boolean isDone;
    private boolean isPrepareDone;
    private final boolean transitivelyInvokedByPriorInvalidation;
    private final InvalidateReply[] replies;
    private final Topology topology;
    private final InvalidationTracker tracker;
    private Throwable failure;

    private Invalidate(Node node, Ballot ballot, TxnId txnId, Unseekables<?> invalidateWith, boolean transitivelyInvokedByPriorInvalidation, BiConsumer<Outcome, Throwable> callback)
    {
        this.callback = callback;
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.transitivelyInvokedByPriorInvalidation = transitivelyInvokedByPriorInvalidation;
        this.invalidateWith = invalidateWith;
        Topologies topologies = node.topology().forEpoch(invalidateWith, txnId.epoch());
        Invariants.checkState(topologies.size() == 1);
        this.tracker = new InvalidationTracker(topologies);
        this.topology = topologies.current();
        this.replies = new InvalidateReply[topology.nodes().size()];
    }

    public static Invalidate invalidate(Node node, TxnId txnId, Unseekables<?> invalidateWith, BiConsumer<Outcome, Throwable> callback)
    {
        return invalidate(node, txnId, invalidateWith, false, callback);
    }

    public static Invalidate invalidate(Node node, TxnId txnId, Unseekables<?> invalidateWith, boolean transitivelyInvokedByPriorInvalidation, BiConsumer<Outcome, Throwable> callback)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        Invalidate invalidate = new Invalidate(node, ballot, txnId, invalidateWith, transitivelyInvokedByPriorInvalidation, callback);
        invalidate.start();
        return invalidate;
    }

    private void start()
    {
        node.send(tracker.nodes(), to -> new BeginInvalidation(to, tracker.topologies(), txnId, invalidateWith, ballot), this);
    }

    @Override
    public synchronized void onSuccess(Id from, InvalidateReply reply)
    {
        if (isDone || isPrepareDone)
            return;

        replies[topology.nodes().find(from)] = reply;
        handle(tracker.recordSuccess(from, reply.isPromised(), reply.hasDecision(), reply.acceptedFastPath));
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone || isPrepareDone)
            return;

        if (this.failure == null) this.failure = failure;
        else this.failure.addSuppressed(failure);

        handle(tracker.recordFailure(from));
    }

    private void handle(RequestStatus status)
    {
        switch (status)
        {
            default: throw new AssertionError();
            case Success:
                // EITHER we have a shard that has promised AND a shard that decisively rejects the fast path
                // OR we have promises from EVERY shard so that we can decisively move forwards by guaranteeing
                // either that recovery will succeed or that if it fails to find sufficient information then in
                // combination with this invalidation we have ruled out the possibility of the transaction completing
                // successfully
                invalidate();
                break;

            case Failed:
                // We reach here if we failed to obtain promises from every shard.
                // If we had any actual failures reported we propagate these
                isDone = isPrepareDone = true;
                callback.accept(null, failure != null ? failure : new Preempted(txnId, null));
                break;

            case NoChange:
        }
    }

    private void invalidate()
    {
        Invariants.checkState(!isPrepareDone);
        isPrepareDone = true;

        FullRoute<?> fullRoute = InvalidateReply.findRoute(replies);
        Route<?> someRoute = InvalidateReply.mergeRoutes(replies);

        // first look to see if it has already been decided/invalidated
        // check each shard independently - if we find any that can be invalidated, do so
        InvalidateReply max = InvalidateReply.max(replies);
        InvalidateReply maxNotTruncated = max.maxKnowledgeStatus.is(Status.Truncated) ? max : InvalidateReply.maxNotTruncated(replies);

        if (maxNotTruncated != null)
        {
            switch (maxNotTruncated.maxKnowledgeStatus.status)
            {
                default: throw new AssertionError("Unhandled status: " + maxNotTruncated.maxKnowledgeStatus.status);
                case Truncated: throw illegalState();

                case AcceptedInvalidate:
                    // latest accept also invalidating, so we're on the same page and should finish our invalidation
                case NotDefined:
                    break;

                case PreAccepted:
                    if (tracker.isSafeToInvalidate() || transitivelyInvokedByPriorInvalidation)
                        break;

                case Applied:
                case PreApplied:
                case Stable:
                case Committed:
                case PreCommitted:
                    Invariants.checkState(maxNotTruncated.maxKnowledgeStatus.status == PreAccepted || !invalidateWith.contains(someRoute.homeKey()) || fullRoute != null);

                case Accepted:
                    // TODO (desired, efficiency): if we see Committed or above, go straight to Execute if we have assembled enough information
                    Invariants.checkState(fullRoute != null, "Received a reply from a node that must have known some route, but that did not include it"); // we now require the FullRoute on all replicas to preaccept, commit or apply
                    // The data we see might have made it only to a minority in the event of PreAccept ONLY.
                    // We want to protect against infinite loops, so we inform the recovery of the state we have
                    // witnessed during our initial invalidation.

                    // However, if the state is not guaranteed to be recoverable (i.e. PreAccept/NotWitnessed),
                    // we do not relay this information unless we can guarantee that any shard recovery may contact
                    // has been prevented from reaching a _later_ fast-path decision by our promises.
                    // Which means checking we contacted every shard, since we only reach that point if we have promises
                    // from every shard we contacted.

                    // Note that there's lots of scope for variations in behaviour here, but lots of care is needed.

                    Status witnessedByInvalidation = maxNotTruncated.maxKnowledgeStatus.status;
                    if (!witnessedByInvalidation.hasBeen(Accepted))
                    {
                        Invariants.checkState(tracker.all(InvalidationShardTracker::isPromised));
                        if (!invalidateWith.containsAll(fullRoute))
                            witnessedByInvalidation = null;
                    }
                    RecoverWithRoute.recover(node, ballot, txnId, fullRoute, witnessedByInvalidation, callback);
                    return;

                case Invalidated:
                    // TODO (desired, API consistency): standardise semantics of whether local application of state prior is async or sync to callback
                    isDone = true;
                    commitInvalidate();
                    return;
            }
        }

        if (max != maxNotTruncated)
        {
            boolean allShardsTruncated = true;
            for (Shard shard : topology.shards())
            {
                InvalidateReply maxReply = InvalidateReply.max(replies, shard, topology.nodes());
                allShardsTruncated &= maxReply.maxStatus.is(Status.Truncated);
            }
            if (allShardsTruncated)
            {
                isDone = true;
                callback.accept(TRUNCATED_DURABLE_OR_INVALIDATED, null);
                return;
            }
        }

        // if we have witnessed the transaction, but are able to invalidate, do we want to proceed?
        // Probably simplest to do so, but perhaps better for user if we don't.
        Ranges ranges = Ranges.of(tracker.promisedShard().range);
        // we look up by TxnId at the target node, so it's fine to pick a RoutingKey even if it's a range transaction
        RoutingKey someKey = invalidateWith.slice(ranges).get(0).someIntersectingRoutingKey(ranges);
        proposeInvalidate(node, ballot, txnId, someKey, (success, fail) -> {
            /*
              We're now inside our *exactly once* callback we registered with proposeInvalidate, and we need to
              make sure we honour our own exactly once semantics with {@code callback}.
              So we are responsible for all exception handling.
             */
            isDone = true;
            if (fail != null)
            {
                callback.accept(null, fail);
            }
            else
            {
                try
                {
                    commitInvalidate();
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                }
            }
        });
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void commitInvalidate()
    {
        @Nullable Route<?> route = InvalidateReply.mergeRoutes(replies);
        if (route == null && Route.isRoute(invalidateWith)) route = Route.castToRoute(invalidateWith);
        if (route != null) route = route.withHomeKey();

        // TODO (desired, efficiency): commitInvalidate (and others) should skip the network for local applications,
        //  so we do not need to explicitly do so here before notifying the waiter
        Unseekables<?> commitTo = Unseekables.merge(route, (Unseekables) invalidateWith);
        Commit.Invalidate.commitInvalidate(node, txnId, commitTo, txnId);
        commitInvalidateLocal(commitTo);
    }

    private void commitInvalidateLocal(Unseekables<?> commitTo)
    {
        // TODO (desired): merge with FetchData.InvalidateOnDone
        // TODO (desired): when sending to network, register a callback for when local application of commitInvalidate message ahs been performed, so no need to special-case
        node.forEachLocal(contextFor(txnId), commitTo, txnId.epoch(), txnId.epoch(), safeStore -> {
            Commands.commitInvalidate(safeStore, safeStore.get(txnId, txnId, commitTo), commitTo);
        }).begin((s, f) -> {
            callback.accept(INVALIDATED, null);
            if (f != null) // TODO (required): consider exception handling more carefully: should we catch these prior to passing to callbacks?
                node.agent().onUncaughtException(f);
        });
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
