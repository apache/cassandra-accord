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

import java.util.function.BiConsumer;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.CheckShards;
import accord.coordinate.CoordinationFailed;
import accord.coordinate.Invalidated;
import accord.coordinate.Truncated;
import accord.impl.basic.Cluster;
import accord.impl.basic.Packet;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.messages.MessageType;
import accord.messages.ReplyContext;
import accord.primitives.RoutingKeys;
import accord.primitives.Txn;
import accord.messages.Request;
import accord.primitives.TxnId;

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
            return ok.saveStatus.hasBeen(PreApplied) ? Action.Approve : Action.Reject;
        }

        @Override
        protected void onDone(CheckShards.Success done, Throwable failure)
        {
            if (failure != null) callback.accept(null, failure);
            else if (merged.saveStatus.is(Status.Invalidated)) callback.accept(Outcome.Invalidated, null);
            else if (merged.saveStatus.is(Status.Truncated)) callback.accept(Outcome.Truncated, null);
            else if (!merged.saveStatus.hasBeen(PreCommitted) && merged.truncated) callback.accept(Outcome.Truncated, null);
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
        final Txn txn;

        ResultCallback(Node node, Id client, ReplyContext replyContext, Txn txn)
        {
            this.node = node;
            this.client = client;
            this.replyContext = replyContext;
            this.txn = txn;
        }

        @Override
        public void accept(Result success, Throwable fail)
        {
            // TODO (desired, testing): error handling
            if (success != null)
            {
                node.reply(client, replyContext, (ListResult) success);
            }
            else if (fail instanceof CoordinationFailed)
            {
                RoutingKey homeKey = ((CoordinationFailed) fail).homeKey();
                TxnId txnId = ((CoordinationFailed) fail).txnId();
                if (fail instanceof Invalidated)
                {
                    node.reply(client, replyContext, new ListResult(client, ((Packet) replyContext).requestId, txnId, null, null, null, null));
                    return;
                }
                node.reply(client, replyContext, new ListResult(client, ((Packet)replyContext).requestId, txnId, null, null, new int[0][], null));
                ((Cluster)node.scheduler()).onDone(() -> {
                    node.commandStores()
                        .select(homeKey)
                        .execute(() -> CheckOnResult.checkOnResult(node, txnId, homeKey, (s, f) -> {
                            if (f != null)
                            {
                                node.reply(client, replyContext, new ListResult(client, ((Packet) replyContext).requestId, txnId, null, null, f instanceof Truncated ? new int[2][] : new int[3][], null));
                                return;
                            }
                            switch (s)
                            {
                                case Truncated:
                                    node.reply(client, replyContext, new ListResult(client, ((Packet) replyContext).requestId, txnId, null, null, new int[2][], null));
                                    break;
                                case Invalidated:
                                    node.reply(client, replyContext, new ListResult(client, ((Packet) replyContext).requestId, txnId, null, null, null, null));
                                    break;
                                case Lost:
                                    node.reply(client, replyContext, new ListResult(client, ((Packet) replyContext).requestId, txnId, null, null, new int[1][], null));
                                    break;
                                case Other:
                                    // currently caught elsewhere in response tracking, but might help to throw an exception here
                            }
                        }));
                });
            }
        }
    }

    public final Txn txn;

    public ListRequest(Txn txn)
    {
        this.txn = txn;
    }

    @Override
    public void process(Node node, Id client, ReplyContext replyContext)
    {
        node.coordinate(txn).addCallback(new ResultCallback(node, client, replyContext, txn));
    }

    @Override
    public MessageType type()
    {
        return null;
    }

    @Override
    public String toString()
    {
        return txn.toString();
    }

}
