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

package accord.impl.list;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.CheckShards;
import accord.coordinate.CoordinationFailed;
import accord.coordinate.Exhausted;
import accord.coordinate.Invalidated;
import accord.coordinate.Truncated;
import accord.coordinate.Timeout;
import accord.impl.MessageListener;
import accord.impl.basic.Packet;
import accord.impl.basic.SimulatedFault;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.messages.MessageType;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.RoutingKeys;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.TopologyException;
import accord.topology.TopologyRetiredException;
import accord.utils.async.Cancellable;

import javax.annotation.Nullable;

import static accord.coordinate.Infer.InvalidIf.NotKnownToBeInvalid;
import static accord.impl.list.ListResult.Status.RecoveryApplied;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Status.Applied;
import static accord.primitives.Status.Phase.Cleanup;

import static accord.primitives.Status.PreAccepted;
import static accord.primitives.Status.PreApplied;
import static accord.primitives.Status.PreCommitted;
import static accord.utils.Invariants.illegalState;

public class ListRequest implements Request
{
    static class Outcome
    {
        enum Kind { Applied, Invalidated, Lost, Truncated, Other }

        static final Outcome Invalidated = new Outcome(Kind.Invalidated, null);
        static final Outcome Lost = new Outcome(Kind.Lost, null);
        static final Outcome Truncated = new Outcome(Kind.Truncated, null);
        static final Outcome Other = new Outcome(Kind.Other, null);

        final Kind kind;
        final ListResult result;
        Outcome(Kind kind, ListResult result)
        {
            this.kind = kind;
            this.result = result;
        }
    }

    static class CheckOnResult extends CheckShards<Outcome, Participants<?>>
    {
        int count = 0;
        protected CheckOnResult(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Outcome, Throwable> callback) throws TopologyException
        {
            super(node, node.someExclusiveExecutor(), txnId, txnId.is(Key) ? RoutingKeys.of(homeKey) : Ranges.of(homeKey.asRange()), IncludeInfo.All, null, NotKnownToBeInvalid, callback);
        }

        static void checkOnResult(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Outcome, Throwable> callback)
        {
            try
            {
                CheckOnResult result = new CheckOnResult(node, txnId, homeKey, callback);
                result.start();
            }
            catch (Throwable t)
            {
                callback.accept(null, t);
            }
        }

        @Override
        protected Action checkSufficient(Id from, CheckStatusOk ok)
        {
            ++count;
            // this method is called for each reply, so if we see a reply where the status is not known, it may be known on others;
            // once all status are merged, then onDone will apply additional logic to make sure things are safe.
            if (ok.maxKnowledgeSaveStatus == SaveStatus.Uninitialised)
                return Action.ApproveIfQuorum;
            return ok.maxKnowledgeSaveStatus.hasBeen(PreApplied) ? Action.Approve : Action.Reject;
        }

        @Override
        protected void onDone(CheckShards.Success done, Throwable failure)
        {
            BiConsumer<? super Outcome, Throwable> callback = takeCallback();
            if (failure instanceof Exhausted) callback.accept(Outcome.Lost, null);
            else if (failure != null) callback.accept(null, failure);
            else if (merged.maxKnowledgeSaveStatus.is(Status.Invalidated)) callback.accept(Outcome.Invalidated, null);
            else if (merged.maxKnowledgeSaveStatus.is(Status.Truncated)) callback.accept(Outcome.Truncated, null);
            else if (merged.maxKnowledgeSaveStatus.hasBeen(Applied)) callback.accept(new Outcome(Outcome.Kind.Applied, (ListResult) ((CheckStatus.CheckStatusOkFull) merged).result), null);
            else if (!merged.maxKnowledgeSaveStatus.hasBeen(PreCommitted) && merged.maxSaveStatus.phase == Cleanup) callback.accept(Outcome.Truncated, null);
            else if (!merged.maxSaveStatus.hasBeen(PreAccepted) && count >= (1 + nodes().size())/2) callback.accept(Outcome.Lost, null);
            else callback.accept(Outcome.Other, null);
        }

        @Override
        protected boolean isSufficient(CheckStatusOk ok)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CoordinationKind kind()
        {
            return CoordinationKind.Other;
        }
    }

    static class ResultCallback implements BiConsumer<Result, Throwable>
    {
        final Node node;
        final Id client;
        final ReplyContext replyContext;
        final MessageListener listener;
        final TxnId id;
        final Txn txn;

        ResultCallback(Node node, Id client, ReplyContext replyContext, MessageListener listener, TxnId id, Txn txn)
        {
            this.node = node;
            this.client = client;
            this.replyContext = replyContext;
            this.listener = listener;
            this.id = id;
            this.txn = txn;
        }

        @Override
        public void accept(Result success, Throwable fail)
        {
            if (fail != null)
            {
                listener.onClientAction(MessageListener.ClientAction.FAILURE, node.id(), id, fail);
                if (id.kind() == Txn.Kind.EphemeralRead)
                {
                    node.reply(client, replyContext, ListResult.lost(client, ((Packet)replyContext).requestId, id), null, null);
                }
                else if (fail instanceof CoordinationFailed)
                {
                    RoutingKey homeKey = ((CoordinationFailed) fail).homeKey();
                    if (fail instanceof Invalidated)
                    {
                        node.reply(client, replyContext, ListResult.invalidated(client, ((Packet)replyContext).requestId, id), null, null);
                        return;
                    }

                    node.reply(client, replyContext, ListResult.heartBeat(client, ((Packet)replyContext).requestId, id), null, null);
                    node.scheduler().once(() -> checkOnResult(homeKey, id, 0, null), 1L, TimeUnit.MINUTES);
                }
                else if (fail instanceof SimulatedFault)
                {
                    node.reply(client, replyContext, ListResult.heartBeat(client, ((Packet)replyContext).requestId, id), null, null);
                    node.scheduler().once(() -> checkOnResult(null, id, 0, null), 1L, TimeUnit.MINUTES);
                }
                else if (fail instanceof CancellationException)
                {
                    node.reply(client, replyContext, ListResult.heartBeat(client, ((Packet)replyContext).requestId, id), null, null);
                    node.scheduler().once(() -> checkOnResult(null, id, 0, null), 1L, TimeUnit.MINUTES);
                }
                else
                {
                    node.reply(client, replyContext, ListResult.lost(client, ((Packet)replyContext).requestId, id), null, null);
                }
            }
            else if (success != null)
            {
                listener.onClientAction(MessageListener.ClientAction.SUCCESS, node.id(), id, success);
                node.reply(client, replyContext, (ListResult) success, null, null);
            }
            else
            {
                listener.onClientAction(MessageListener.ClientAction.UNKNOWN, node.id(), id, null);
                node.agent().onException(new NullPointerException("Success and Failure were both null"));
            }
        }

        private void checkOnResult(@Nullable RoutingKey homeKey, TxnId txnId, int attempt, Throwable t) {
            if (node.epoch() < txnId.epoch())
            {
                RoutingKey hk = homeKey;
                node.withEpochAtLeast(txnId.epoch(), null, (success, fail) -> checkOnResult(hk, txnId, attempt + 1, t));
                return;
            }

            if (txnId.epoch() < node.topology().minEpoch())
            {
                node.reply(client, replyContext, ListResult.failure(client, ((Packet)replyContext).requestId, txnId), null, null);
                node.agent().onException(new TopologyRetiredException(txnId.epoch(), node.topology().minEpoch()));
                return;
            }

            if (homeKey == null)
            {
                try
                {
                    homeKey = node.computeRoute(txnId, txn.keys()).homeKey();
                }
                catch (Throwable t2)
                {
                    throw new RuntimeException(t2);
                }
            }
            RoutingKey finalHomeKey = homeKey;
            node.someExecutor().execute(() -> CheckOnResult.checkOnResult(node, txnId, finalHomeKey, (s, f) -> {
                if (f != null)
                {
                    if (f instanceof Truncated)
                    {
                        node.reply(client, replyContext, ListResult.truncated(client, ((Packet)replyContext).requestId, txnId), null, null);
                        return;
                    }
                    // some arbitrarily large limit to attempts
                    if (attempt < 1000 && (f instanceof Timeout || f instanceof SimulatedFault || f instanceof Exhausted))
                    {
                        if (attempt < 100) checkOnResult(finalHomeKey, txnId, attempt + 1, f);
                        else node.scheduler().once(() -> checkOnResult(finalHomeKey, txnId, attempt + 1, null), retryDelay(attempt), TimeUnit.MINUTES);
                    }
                    else
                    {
                        node.reply(client, replyContext, ListResult.failure(client, ((Packet)replyContext).requestId, txnId), null, null);
                        node.agent().onException(f);
                    }
                    return;
                }
                switch (s.kind)
                {
                    case Applied:
                        node.reply(client, replyContext, new ListResult(RecoveryApplied, client, ((Packet)replyContext).requestId, txnId, s.result.readKeys, s.result.responseKeys, s.result.read, s.result.update), null, null);
                        break;
                    case Truncated:
                        node.reply(client, replyContext, ListResult.truncated(client, ((Packet)replyContext).requestId, txnId), null, null);
                        break;
                    case Invalidated:
                        node.reply(client, replyContext, ListResult.invalidated(client, ((Packet)replyContext).requestId, txnId), null, null);
                        break;
                    case Lost:
                        node.reply(client, replyContext, ListResult.lost(client, ((Packet)replyContext).requestId, txnId), null, null);
                        break;
                    case Other:
                        if (attempt < 100)
                        {
                            node.reply(client, replyContext, ListResult.heartBeat(client, ((Packet)replyContext).requestId, txnId), null, null);
                            node.scheduler().selfRecurring(() -> checkOnResult(finalHomeKey, txnId, attempt + 1, null), retryDelay(attempt), TimeUnit.MINUTES);
                        }
                        else
                        {
                            node.reply(client, replyContext, ListResult.other(client, ((Packet)replyContext).requestId, txnId), null, null);
                        }
                        break;
                    default:
                        node.agent().onException(new AssertionError("Unknown outcome: " + s));
                }
            }));
        }
    }

    private static long retryDelay(int attempts)
    {
        return attempts <= 1 ? 1 : Math.min(5, attempts);
    }

    private final String description;
    private final Function<Node, Txn> gen;
    private final BiFunction<Node, Txn, TxnId> txnIdGen;
    private final MessageListener listener;
    private transient Txn txn;
    private transient TxnId id;

    public ListRequest(String description, Function<Node, Txn> gen, BiFunction<Node, Txn, TxnId> txnIdGen, MessageListener listener)
    {
        this.description = description;
        this.gen = gen;
        this.txnIdGen = txnIdGen;
        this.listener = listener;
    }

    @Override
    public Cancellable process(Node node, Id client, ReplyContext replyContext)
    {
        if (id != null)
            throw illegalState("Called process multiple times");
        txn = gen.apply(node);
        id = txnIdGen.apply(node, txn);
        listener.onClientAction(MessageListener.ClientAction.SUBMIT, node.id(), id, txn);
        node.coordinate(id, txn).begin(new ResultCallback(node, client, replyContext, listener, id, txn));
        return null;
    }

    @Override
    public MessageType type()
    {
        return null;
    }

    @Override
    public String toString()
    {
        return "ListRequest{" +
               "description='" + description + '\'' +
               ", id=" + id +
               ", txn=" + txn +
               '}';
    }
}
