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

package accord.api;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import accord.api.ProgressLog.BlockedUntil;
import accord.coordinate.Coordination.CoordinationKind;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.messages.ReplyContext;
import accord.primitives.Participants;
import accord.primitives.Routable.Domain;
import accord.primitives.Status.Phase;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncChain;

/**
 * Facility for augmenting node behaviour at specific points
 */
public interface Agent extends UncaughtExceptionListener
{
    default @Nullable Tracing trace(@Nullable TxnId txnId, @Nullable Participants<?> participants, CoordinationKind eventType) { return null; }

    OwnershipEventListener ownershipEvents();

    default CoordinatorEventListener coordinatorEvents()
    {
        return CoordinatorEventListener.NOOP;
    }

    default ReplicaEventListener replicaEvents()
    {
        return ReplicaEventListener.NOOP;
    }

    @Override
    void onUncaughtException(Throwable t);
    void onCaughtException(Throwable t, String context);

    /**
     * Controls pruning of CommandsForKey
     *
     * The timestamp delta between the prune point and any pruned TxnId. This works primarily to minimise the
     * chance of encountering a TxnId that precedes prunedBefore.
     */
    long cfkHlcPruneDelta();

    /**
     * Controls pruning of CommandsForKey.
     *
     * The number of entries before the candidate prune point that we require before we try to prune.
     * This only works to reduce the time wasted pruning when there is limited benefit.
     */
    int cfkPruneInterval();

    /**
     * Controls pruning of MaxConflicts
     *
     * The timestamp delta between a timestamp being added to MaxConflicts and the minimum timestamp we
     * want to maintain granular max conflict data for. A smaller value minimizes the amount of memory taken
     * for granular maxConflicts data. A larger value minimizes the number of unneccesary fast path rejections,
     * within the bounds of inter-node clock drift and messaging latencies.
     */
    long maxConflictsHlcPruneDelta();

    /**
     * Controls pruning of MaxConflicts
     *
     * Every n updates, max conflicts is pruned to the delta, where n is the value returned by this method
     */
    long maxConflictsPruneInterval();

    /**
     * Create an empty transaction that Accord can use for its own internal transactions.
     */
    Txn emptySystemTxn(Kind kind, Domain domain);

    /**
     * For each shard, select a small number of replicas that should be preferred for listening to progress updates
     * from {@code from}. This should be 1-2 nodes that will be contacted preferentially for progress to minimise
     * the number of messages we exchange. These nodes should be picked in a fashion so that there is a chain
     * connecting all replicas of a shard together, e.g. in a ring picking the replicas directly behind you in the ring.
     */
    default Topologies selectPreferred(Node.Id from, Topologies to) { return to; }

    /**
     * @return true if the txnId is too old, and should be rejected
     */
    boolean rejectPreAccept(TimeService time, TxnId txnId);

    /**
     *  This method permits implementations to configure the time at which a local home shard will attempt
     *  to coordinate a transaction to completion.
     *
     *  This should aim to prevent two home replicas from attempting to initiate coordination at the same time.
     */
    long slowCoordinatorDelay(Node node, SafeCommandStore safeStore, TxnId txnId, TimeUnit units, int attempt);

    /**
     *  This method permits implementations to configure the time at which a local home shard will consider
     *  ITS OWN coordination slow for purposes of permitting other coordinators to attempt to take over recovery
     */
    boolean isSlowCoordinator(long elapsed, TimeUnit units, TxnId txnId, int attempt);

    /**
     *  This method permits implementations to configure a delay for waiting to attempt to progress the local
     *  state machine for a transaction by querying its remote peers.
     *
     *  This method should only attempt to minimise wasted work that would anyway be achieved by the transaction's
     *  coordinator, while ensuring prompt when the coordinator considers the transaction to be durable.
     */
    long slowReplicaDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int attempt, BlockedUntil blockedUntil, TimeUnit units);

    /**
     * When a peer is queries for a local state, asynchronous callbacks may be registered.
     * Any asynchronous reply is not guaranteed to be delivered, and only one attempt is made.
     * This method configures a retry timeout on the node querying its peer to renew any callback registrations
     * and re-query the local state.
     */
    long slowAwaitDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int attempt, @Nullable BlockedUntil retrying, TimeUnit units);
    long retrySyncPointDelay(Node node, int attempt, TimeUnit units);
    long retryDurabilityDelay(Node node, int attempt, TimeUnit units);
    long expireEpochWait(TimeUnit units);

    long expiresAt(ReplyContext replyContext, TimeUnit unit);
    long selfSlowAt(TxnId txnId, Phase phase, TimeUnit unit);
    long selfExpiresAt(TxnId txnId, Phase phase, TimeUnit unit);

    // make sure the staleId is sufficiently stale
    AsyncChain<TxnId> awaitStaleId(Node node, TxnId staleId, boolean requested);
    long minStaleHlc(Node node, boolean requested);
}
