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
import accord.api.ExclusiveAsyncExecutor;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadOkWithFutureEpoch;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadEphemeralTxnData;
import accord.messages.StableThenRead;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static accord.api.ProtocolModifiers.permitCoordinatorLocalExecution;
import static accord.coordinate.Coordination.CoordinationKind.Execute;
import static accord.coordinate.ExecutePath.EPHEMERAL;
import static accord.coordinate.ReadCoordinator.Action.Approve;
import static accord.coordinate.ReadCoordinator.Action.ApprovePartial;
import static accord.messages.MessageType.StandardMessage.READ_EPHEMERAL_REQ;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.topology.SelectShards.LIVE;
import static accord.utils.Invariants.illegalState;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class ExecuteEphemeralRead extends ReadCoordinator<Result, ReadReply>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ExecuteEphemeralRead.class);

    final Txn txn;
    final FullRoute<?> route;
    final Deps deps;
    final Topologies allTopologies;
    final CoordinationFlags flags;
    private Data data;

    ExecuteEphemeralRead(Node node, ExclusiveAsyncExecutor executor, Topologies topologies, FullRoute<?> route, TxnId txnId, Txn txn, Deps deps, CoordinationFlags flags, BiConsumer<? super Result, Throwable> callback)
    {
        // we need to send Stable to the origin epoch as well as the execution epoch
        // TODO (desired): permit slicing Topologies by key (though unnecessary if we eliminate the concept of non-participating home keys)
        super(node, executor, topologies, txnId, route, callback);
        Invariants.requireArgument(txnId.kind() == EphemeralRead);
        Invariants.require(topologies.currentEpoch() == txnId.epoch());
        this.txn = txn;
        this.route = route;
        this.allTopologies = topologies;
        this.deps = deps;
        this.flags = flags;
    }

    @Override
    protected void startOnceInitialised()
    {
        node.agent().coordinatorEvents().onExecuting(txnId, Ballot.ZERO, deps, EPHEMERAL);
        if (permitCoordinatorLocalExecution() && tryIfUniversal(node.id()))
        {
            new LocalExecute(txnId, node.id()).process(node, node.agent().selfExpiresAt(txnId, READ_EPHEMERAL_REQ, MICROSECONDS));
        }
        else
        {
            super.startOnceInitialised();
        }
    }

    @Override
    public void contact(Id to)
    {
        node.send(to, new ReadEphemeralTxnData(to, allTopologies, txnId, route, txn, deps, route, flags.get(to)), executor, this, tracing);
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
                return retryWithEpochExact(ok.futureEpoch, () -> {
                    BiConsumer<? super Result, Throwable> callback = takeCallback();
                    ExecuteEphemeralRead execute;
                    try
                    {
                        Topologies topologies = node.topology().active().preciseEpochs(route, ok.futureEpoch, ok.futureEpoch, LIVE);
                        execute = new ExecuteEphemeralRead(node, executor, topologies, route, txnId.withEpoch(ok.futureEpoch), txn, deps, CoordinationFlags.none(), callback);
                    }
                    catch (Throwable t)
                    {
                        callback.accept(null, t);
                        return;
                    }
                    execute.start();
                });
            }

            Data next = ok.data;
            if (next != null)
                data = data == null ? next : data.merge(next);

            return ok.unavailable == null ? Approve : ApprovePartial;
        }

        CommitOrReadNack nack = (CommitOrReadNack) reply;
        switch (nack.kind)
        {
            default: throw UnhandledEnum.unknown(nack.kind);
            case Waiting:
                return Action.None;

            case Redundant:
            case Rejected:
                // TODO (expected): shouldn't be preemptible (can be made redundant, but should be a special case)
                invokeCallback(null, Preempted.preempted(node.agent(), txnId, route.homeKey()));
                return Action.Aborted;
            case InsufficientAndWaiting:
            case InsufficientEpochs:
                // the replica may be missing the original commit, or the additional commit, so send everything
                // also try sending a read command to another replica, in case they're ready to serve a response
                invokeCallback(null, illegalState("Received Insufficient response to ephemeral read request"));
                return Action.Aborted;
        }
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure == null)
        {
            node.agent().coordinatorEvents().onExecuted(txnId, Ballot.ZERO);
            invokeCallback(txn.result(txnId, txnId.withEpochAtLeast(allTopologies.currentEpoch()), data), null);
        }
        else
        {
            invokeCallback(null, failure);
        }
    }

    class LocalExecute extends ReadEphemeralTxnData
    {
        private Timeouts.RegisteredTimeout slowTimeout;

        public LocalExecute(TxnId txnId, Node.Id self)
        {
            super(txnId, route, txn.intersecting(route, true), deps.intersecting(route), route, ExecuteEphemeralRead.this.flags.get(self));
        }

        @Override
        public CommitOrReadNack applyInternal(SafeCommandStore safeStore)
        {
            StoreParticipants participants = StoreParticipants.execute(safeStore, route, txnId, minEpoch(), executeAtEpoch);
            SafeCommand safeCommand = safeStore.get(txnId, participants);
            return applyInternal(safeStore, safeCommand, participants);
        }

        @Override
        public void accept(CommitOrReadNack reply, Throwable failure)
        {
            if (reply == CommitOrReadNack.Waiting)
            {
                // TODO (expected): share implementation with ExecuteTxn
                long slowAt = node.agent().selfSlowAt(txnId, READ_EPHEMERAL_REQ, MICROSECONDS);
                slowTimeout = node.timeouts().registerAt(new Timeouts.Timeout()
                {
                    @Override public void timeout() { executor.executeMaybeImmediately(() -> {
                        onSlow(node.id());
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
        protected boolean timeoutInternal()
        {
            if (!super.timeoutInternal())
                return false;

            // TODO (desired): if we fail to commit locally we can submit a slow/medium path request
            ExecuteEphemeralRead.this.onFailure(node.id(), new Timeout(txnId, route.homeKey(), "Could not promptly read from local coordinator"));
            return true;
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
            if (fail == null) ExecuteEphemeralRead.this.onSuccess(node.id(), reply);
            else ExecuteEphemeralRead.this.onFailure(node.id(), fail);
        }

        @Override
        public ReadType kind() { throw new UnsupportedOperationException(); }

        public ExecutionKind executionKind()
        {
            return ExecutionKind.STABLE;
        }
    }

    @Override
    public CoordinationKind kind()
    {
        return Execute;
    }

    @Override
    public Participants<?> scope()
    {
        return route;
    }

    @Override
    public String describe()
    {
        // TODO (desired): summarise what data replies we have
        return "flags=" + flags;
    }
}
