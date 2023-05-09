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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;

import accord.api.RoutingKey;
import accord.coordinate.tracking.ReadTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.primitives.DataConsistencyLevel;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.Invariants;
import javax.annotation.Nullable;

import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.debug;
import static accord.utils.Invariants.nonNull;
import static com.google.common.collect.Maps.newHashMapWithExpectedSize;

abstract class ReadCoordinator<Reply extends accord.messages.Reply> extends ReadTracker implements Callback<Reply>
{
    protected enum Action
    {
        /**
         * Immediately fail the coordination
         */
        Abort,

        /**
         * This response is unsuitable for the purposes of this coordination, whether individually or as a quorum.
         */
        Reject,

        /**
         * An intermediate response has been received that suggests a full response may be delayed; another replica
         * should be contacted for its response. This is currently used when a read is lacking necessary information
         * (such as a commit) in order to serve the response, and so additional information is sent by the coordinator.
         */
        TryAlternative,

        /**
         * This response is unsuitable by itself, but if a quorum of such responses is received for the shard
         * we will Success.Quorum
         */
        ApproveIfQuorum,

        /**
         * This response is suitable by itself; if we receive such a response from each shard we will complete
         * successfully with Success.Success
         */
        Approve
    }

    protected enum Success { Quorum, Success }

    final Node node;
    final TxnId txnId;
    private boolean isDone;
    private Throwable failure;
    final Map<Id, Object> debug = debug() ? new HashMap<>() : null;

    ReadCoordinator(Node node, Topologies topologies, TxnId txnId, DataConsistencyLevel cl)
    {
        super(topologies, cl);
        this.node = node;
        this.txnId = txnId;
    }

    protected abstract Action process(Id from, Reply reply);
    protected abstract void onDone(Success success, Throwable failure);

    protected abstract void contact(Id to);

    @Override
    public void onSuccess(Id from, Reply reply)
    {
        if (debug != null)
            debug.put(from, reply);

        if (isDone)
            return;

        switch (process(from, reply))
        {
            default: throw new IllegalStateException();
            case Abort:
                isDone = true;
                break;

            case TryAlternative:
                Invariants.checkState(!reply.isFinal());
                onSlowResponse(from);
                break;

            case Reject:
                handle(recordReadFailure(from));
                break;

            case ApproveIfQuorum:
                handle(recordQuorumReadSuccess(from));
                break;

            case Approve:
                handle(recordReadSuccess(from));
        }
    }

    @Override
    public void onSlowResponse(Id from)
    {
        handle(recordSlowResponse(from));
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (debug != null)
            debug.put(from, null);

        if (isDone)
            return;

        if (this.failure == null) this.failure = failure;
        else this.failure.addSuppressed(failure);

        handle(recordReadFailure(from));
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        if (isDone)
        {
            node.agent().onUncaughtException(failure);
            return;
        }

        if (this.failure != null)
            failure.addSuppressed(this.failure);
        this.failure = failure;
        finishOnFailure();
    }

    protected void finishOnFailure()
    {
        Invariants.checkState(!isDone);
        isDone = true;
        if (failure == null)
            failure = new Exhausted(txnId, null);
        onDone(null, failure);
    }

    private void handle(RequestStatus result)
    {
        switch (result)
        {
            default: throw new AssertionError();
            case NoChange:
                break;
            case Success:
                Invariants.checkState(!isDone);
                isDone = true;
                onDone(waitingOnData == 0 ? Success.Success : Success.Quorum, null);
                break;
            case Failed:
                finishOnFailure();
        }
    }

    /**
     * Send the reads of Accord metadata to the specified nodes.
     *
     * The override in Execute is used to send the initial reads for data (not Accord metadata).
     *
     * The value Keys is only for data reads and specifies which keys read from the node should be data reads.
     * An empty Keys indicates all reads should be digest reads.
     */
    protected void sendInitialReads(Map<Id, RoutingKeys> to)
    {
        to.forEach((id, dataReadKeys) -> {
            checkArgument(dataReadKeys.isEmpty(), "Don't support data read keys here");
            contact(id);
        });
    }

    public void start()
    {
        start(null);
    }

    /**
     * Start the read process by calculating which nodes to contact.
     * readDataKeys is only valid when reading actual data where digest vs data reads
     * matter and the ReadCoordinator needs to make sure at least one data read is sent for each
     * key.
     */
    public void start(@Nullable Unseekables<?, ?> dataReadKeys)
    {
        Map<Id, RoutingKeys> contact = newHashMapWithExpectedSize(maxShardsPerEpoch());
        if (trySendMore(Map::put, contact, getShardToKeys(dataReadKeys)) != RequestStatus.NoChange)
            throw new IllegalStateException();
        sendInitialReads(contact);
    }

    /*
     * Which nodes to send to reads to is based on contacting shards, but we also need to calculate
     * for each node which reads will be digest reads so map from shard to keys so
     * a data read is only sent once for each key.
     */
    private ListMultimap<Shard, RoutingKey> getShardToKeys(@Nullable Unseekables<?,?> dataReadKeys)
    {
        if (dataCL.requiresDigestReads)
        {
            nonNull(dataReadKeys, "dataReadKeys should not be null digest reads are required");
            checkArgument(!dataReadKeys.isEmpty(), "dataReadKeys shouldn't be empty if digest reads are required");
            ListMultimap<Shard, RoutingKey> shardToKeys = ArrayListMultimap.create();
            // Reads of user data only ever have one topology
            Topology topology = topologies().get(0);
            for (RoutingKey k : (RoutingKeys)dataReadKeys)
                shardToKeys.put(topology.forKey(k), k);
            return shardToKeys;
        }
        return ImmutableListMultimap.of();
    }

    @Override
    protected RequestStatus trySendMore()
    {
        // TODO (low priority): due to potential re-entrancy into this method, if the node we are contacting is unavailable
        //                      so onFailure is invoked immediately, for the moment we copy nodes to an intermediate list.
        //                      would be better to prevent reentrancy either by detecting this inside trySendMore or else
        //                      queueing callbacks externally, so two may not be in-flight at once
        List<Id> contacts = new ArrayList<>(1);
        RequestStatus status = trySendMore((list, id, dataReadKeys) -> list.add(id), contacts);
        contacts.forEach(this::contact);
        return status;
    }
}
