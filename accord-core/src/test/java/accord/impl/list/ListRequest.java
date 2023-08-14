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

import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.CheckShards;
import accord.coordinate.CoordinationFailed;
import accord.coordinate.Invalidated;
import accord.coordinate.Truncated;
import accord.coordinate.Timeout;
import accord.impl.basic.Cluster;
import accord.impl.basic.NodeSink;
import accord.impl.basic.Packet;
import accord.impl.basic.SimulatedFault;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.messages.MessageType;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.RoutingKeys;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import javax.annotation.Nullable;

import static accord.local.Status.Phase.Cleanup;
import java.util.function.BiConsumer;

import static accord.local.Status.PreApplied;
import static accord.local.Status.PreCommitted;

public class ListRequest implements Request
{
    enum Outcome { Invalidated, Lost, Truncated, Other }

    static class CheckOnResult extends CheckShards
    {
        final BiConsumer<Outcome, Throwable> callback;
        int count = 0;
        protected CheckOnResult(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Outcome, Throwable> callback)
        {
            super(node, txnId, RoutingKeys.of(homeKey), IncludeInfo.All);
            this.callback = callback;
        }

        static void checkOnResult(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Outcome, Throwable> callback)
        {
            CheckOnResult result = new CheckOnResult(node, txnId, homeKey, callback);
            result.start();
        }

        @Override
        protected Action checkSufficient(Id from, CheckStatusOk ok)
        {
            ++count;
            // this method is called for each reply, so if we see a reply where the status is not known, it may be known on others;
            // once all status are merged, then onDone will apply aditional logic to make sure things are safe.
            if (ok.saveStatus == SaveStatus.Uninitialised)
                return Action.ApproveIfQuorum;
            return ok.saveStatus.hasBeen(PreApplied) ? Action.Approve : Action.Reject;
        }

        @Override
        protected void onDone(CheckShards.Success done, Throwable failure)
        {
            if (failure != null) callback.accept(null, failure);
            else if (merged.saveStatus.is(Status.Invalidated)) callback.accept(Outcome.Invalidated, null);
            else if (merged.saveStatus.is(Status.Truncated)) callback.accept(Outcome.Truncated, null);
            else if (!merged.saveStatus.hasBeen(PreCommitted) && merged.maxSaveStatus.phase == Cleanup) callback.accept(Outcome.Truncated, null);
            else if (count == nodes().size()) callback.accept(Outcome.Lost, null);
            else callback.accept(Outcome.Other, null);
        }

        @Override
        protected boolean isSufficient(CheckStatusOk ok)
        {
            throw new UnsupportedOperationException();
        }
    }

    static class ResultCallback implements BiConsumer<Result, Throwable>
    {
        final Node node;
        final Id client;
        final ReplyContext replyContext;
        final TxnId id;
        final Txn txn;

        ResultCallback(Node node, Id client, ReplyContext replyContext, TxnId id, Txn txn)
        {
            this.node = node;
            this.client = client;
            this.replyContext = replyContext;
            this.id = id;
            this.txn = txn;
        }

        @Override
        public void accept(Result success, Throwable fail)
        {
            if (fail != null)
            {
                ((NodeSink) node.messageSink()).debugClient(id, fail, NodeSink.ClientAction.FAILURE);
                if (fail instanceof CoordinationFailed)
                {
                    RoutingKey homeKey = ((CoordinationFailed) fail).homeKey();
                    TxnId txnId = ((CoordinationFailed) fail).txnId();
                    if (fail instanceof Invalidated)
                    {
                        node.reply(client, replyContext, ListResult.invalidated(client, ((Packet)replyContext).requestId, txnId), null);
                        return;
                    }

                    node.reply(client, replyContext, ListResult.heartBeat(client, ((Packet)replyContext).requestId, txnId), null);
                    ((Cluster) node.scheduler()).onDone(() -> checkOnResult(homeKey, txnId, 0, null));
                }
                else if (fail instanceof SimulatedFault)
                {
                    node.reply(client, replyContext, ListResult.heartBeat(client, ((Packet)replyContext).requestId, id), null);
                    ((Cluster) node.scheduler()).onDone(() -> checkOnResult(null, id, 0, null));
                }
                else
                {
                    node.agent().onUncaughtException(fail);
                }
            }
            else if (success != null)
            {
                ((NodeSink) node.messageSink()).debugClient(id, success, NodeSink.ClientAction.SUCCESS);
                node.reply(client, replyContext, (ListResult) success, null);
            }
            else
            {
                ((NodeSink) node.messageSink()).debugClient(id, null, NodeSink.ClientAction.UNKNOWN);
                node.agent().onUncaughtException(new NullPointerException("Success and Failure were both null"));
            }
        }

        private void checkOnResult(@Nullable RoutingKey homeKey, TxnId txnId, int attempt, Throwable t) {
            if (attempt == 42)
            {
                node.agent().onUncaughtException(t);
                return;
            }
            if (homeKey == null)
                homeKey = node.selectRandomHomeKey(txnId);
            RoutingKey finalHomeKey = homeKey;
            node.commandStores().select(homeKey).execute(() -> CheckOnResult.checkOnResult(node, txnId, finalHomeKey, (s, f) -> {
                if (f != null)
                {
                    if (f instanceof Truncated)
                    {
                        node.reply(client, replyContext, ListResult.truncated(client, ((Packet)replyContext).requestId, txnId), null);
                        return;
                    }
                    if (f instanceof Timeout || f instanceof SimulatedFault) checkOnResult(finalHomeKey, txnId, attempt + 1, f);
                    else
                    {
                        node.reply(client, replyContext, ListResult.failure(client, ((Packet)replyContext).requestId, txnId), null);
                        node.agent().onUncaughtException(f);
                    }
                    return;
                }
                switch (s)
                {
                    case Truncated:
                        node.reply(client, replyContext, ListResult.truncated(client, ((Packet)replyContext).requestId, txnId), null);
                        break;
                    case Invalidated:
                        node.reply(client, replyContext, ListResult.invalidated(client, ((Packet)replyContext).requestId, txnId), null);
                        break;
                    case Lost:
                        node.reply(client, replyContext, ListResult.lost(client, ((Packet)replyContext).requestId, txnId), null);
                        break;
                    case Other:
                        node.reply(client, replyContext, ListResult.other(client, ((Packet)replyContext).requestId, txnId), null);
                        break;
                    default:
                        node.agent().onUncaughtException(new AssertionError("Unknown outcome: " + s));
                }
            }));
        }
    }

    public final Txn txn;
    private TxnId id;

    public ListRequest(Txn txn)
    {
        this.txn = txn;
    }

    @Override
    public void process(Node node, Id client, ReplyContext replyContext)
    {
        if (id != null)
            throw new IllegalStateException("Called process multiple times");
        id = node.nextTxnId(txn.kind(), txn.keys().domain());
        ((NodeSink) node.messageSink()).debugClient(id, txn, NodeSink.ClientAction.SUBMIT);
        node.coordinate(id, txn).addCallback(new ResultCallback(node, client, replyContext, id, txn));
    }

    @Override
    public MessageType type()
    {
        return null;
    }

    @Override
    public String toString()
    {
        return id == null ? txn.toString() : id + " -> " + txn;
    }

}
