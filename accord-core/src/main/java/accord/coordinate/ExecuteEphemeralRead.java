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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.Result;
import accord.api.Timeouts;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SequentialAsyncExecutor;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.messages.MessageType;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadOkWithFutureEpoch;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadEphemeralTxnData;
import accord.messages.SafeCallback;
import accord.messages.StableThenRead;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.WrappableException;

import static accord.api.ProtocolModifiers.Toggles.permitLocalExecution;
import static accord.coordinate.ReadCoordinator.Action.Aborted;
import static accord.coordinate.ReadCoordinator.Action.Approve;
import static accord.coordinate.ReadCoordinator.Action.ApprovePartial;
import static accord.messages.ReadData.CommitOrReadNack.Waiting;
import static accord.primitives.Status.Phase.Execute;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;
import static accord.utils.Invariants.illegalState;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class ExecuteEphemeralRead extends ReadCoordinator<ReadReply>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ExecuteEphemeralRead.class);

    final Txn txn;
    final FullRoute<?> route;
    final Deps deps;
    final Topologies allTopologies;
    final CoordinationFlags flags;
    final BiConsumer<? super Result, Throwable> callback;
    private Data data;

    ExecuteEphemeralRead(Node node, SequentialAsyncExecutor executor, Topologies topologies, FullRoute<?> route, TxnId txnId, Txn txn, Deps deps, CoordinationFlags flags, BiConsumer<? super Result, Throwable> callback)
    {
        // we need to send Stable to the origin epoch as well as the execution epoch
        // TODO (desired): permit slicing Topologies by key (though unnecessary if we eliminate the concept of non-participating home keys)
        super(node, executor, topologies, txnId);
        Invariants.requireArgument(txnId.kind() == EphemeralRead);
        Invariants.require(topologies.currentEpoch() == txnId.epoch());
        this.txn = txn;
        this.route = route;
        this.allTopologies = topologies;
        this.deps = deps;
        this.flags = flags;
        this.callback = callback;
    }

    @Override
    protected void startOnceInitialised()
    {
        if (permitLocalExecution() && tryIfUniversal(node.id()))
        {
            new LocalExecute(txnId, node.id()).process(node, node.agent().selfExpiresAt(txnId, Execute, MICROSECONDS));
        }
        else
        {
            super.startOnceInitialised();
        }
    }


    @Override
    protected void start(Iterable<Id> to)
    {
        to.forEach(this::contact);
    }

    @Override
    public void contact(Id to)
    {
        node.send(to, new ReadEphemeralTxnData(to, allTopologies, txnId, route, txn, deps, route, flags.get(to)), executor, this);
    }

    @Override
    protected Ranges unavailable(ReadReply reply)
    {
        return ((ReadOk)reply).unavailable;
    }

    @Override
    protected Action process(Id from, ReadReply reply)
    {
        if (reply.isOk())
        {
            ReadOkWithFutureEpoch ok = ((ReadOkWithFutureEpoch) reply);
            if (ok.futureEpoch > allTopologies.currentEpoch())
            {
                // TODO (expected): only submit new requests for the keys that execute in a later epoch
                node.withEpochExact(ok.futureEpoch, executor, callback, t -> WrappableException.wrap(t), () -> {
                    new ExecuteEphemeralRead(node, executor, node.topology().preciseEpochs(route, ok.futureEpoch, ok.futureEpoch, SHARE), route, txnId.withEpoch(ok.futureEpoch), txn, deps, CoordinationFlags.none(), callback).start();
                });
                return Aborted;
            }

            Data next = ok.data;
            if (next != null)
                data = data == null ? next : data.merge(next);

            return ok.unavailable == null ? Approve : ApprovePartial;
        }

        CommitOrReadNack nack = (CommitOrReadNack) reply;
        switch (nack)
        {
            default: throw UnhandledEnum.unknown(nack);
            case Waiting:
                return Action.None;

            case Redundant:
            case Rejected:
                // TODO (expected): shouldn't be preemptible (can be made redundant, but should be a special case)
                callback.accept(null, new Preempted(txnId, route.homeKey()));
                return Action.Aborted;
            case Insufficient:
                // the replica may be missing the original commit, or the additional commit, so send everything
                // also try sending a read command to another replica, in case they're ready to serve a response
                callback.accept(null, illegalState("Received Insufficient response to ephemeral read request"));
                return Action.Aborted;
        }
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure == null)
        {
            callback.accept(txn.result(txnId, txnId.withEpochAtLeast(allTopologies.currentEpoch()), data), null);
        }
        else
        {
            callback.accept(null, failure);
        }
    }

    class LocalExecute extends ReadEphemeralTxnData
    {
        private final SafeCallback<ReadReply> callback;
        private Timeouts.RegisteredTimeout slowTimeout;

        public LocalExecute(TxnId txnId, Node.Id self)
        {
            super(txnId, route, txn.intersecting(route, true), deps.intersecting(route), route, ExecuteEphemeralRead.this.flags.get(self));
            this.callback = new SafeCallback<>(executor, ExecuteEphemeralRead.this);
        }

        @Override
        public CommitOrReadNack apply(SafeCommandStore safeStore)
        {
            StoreParticipants participants = StoreParticipants.execute(safeStore, route, txnId, minEpoch(), executeAtEpoch);
            SafeCommand safeCommand = safeStore.get(txnId, participants);
            return apply(safeStore, safeCommand, participants);
        }

        @Override
        public void accept(CommitOrReadNack reply, Throwable failure)
        {
            if (failure == null && reply == null)
            {
                reply = Waiting;
                // TODO (expected): share implementation with ExecuteTxn
                long slowAt = node.agent().selfSlowAt(txnId, Execute, MICROSECONDS);
                slowTimeout = node.timeouts().registerAt(new Timeouts.Timeout()
                {
                    @Override public void timeout() { executor.maybeExecuteImmediately(() -> {
                        onSlowResponse(node.id());
                        slowTimeout = null;
                    }); }
                    @Override public int stripe() { return txnId.hashCode(); }
                }, slowAt, MICROSECONDS);
            }
            super.accept(reply, failure);
        }

        @Override
        protected ExecuteOn executeOn()
        {
            return StableThenRead.EXECUTE_ON;
        }

        @Override
        public void timeout()
        {
            super.timeout();

            // TODO (desired): if we fail to commit locally we can submit a slow/medium path request
            callback.failure(node.id(), new Timeout(txnId, route.homeKey(), "Could not promptly read from local coordinator"));
        }

        @Override
        protected void reply(ReadReply reply, Throwable fail)
        {
            if (slowTimeout != null)
            {
                slowTimeout.cancel();
                slowTimeout = null;
            }
            // TODO (expected): execute immediately if already on CommandStore
            if (fail == null) callback.success(node.id(), reply);
            else callback.failure(node.id(), fail);
        }

        @Override
        public ReadType kind() { throw new UnsupportedOperationException(); }

        @Override
        public MessageType type() { throw new UnsupportedOperationException(); }
    }
}
