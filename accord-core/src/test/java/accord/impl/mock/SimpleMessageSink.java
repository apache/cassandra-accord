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

package accord.impl.mock;

import accord.api.MessageSink;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Reply.FailureReply;
import accord.messages.ReplyContext;
import accord.messages.Request;

public class SimpleMessageSink implements MessageSink
{
    public final Node.Id node;
    public final Network network;

    public SimpleMessageSink(Node.Id node, Network network)
    {
        this.node = node;
        this.network = network;
    }

    @Override
    public void send(Node.Id to, Request request)
    {
        network.send(node, to, request, null, null);
    }

    @Override
    public void send(Node.Id to, Request request, AgentExecutor executor, Callback callback)
    {
        network.send(node, to, request, executor, callback);
    }

    @Override
    public void reply(Node.Id replyingToNode, ReplyContext replyContext, Reply reply)
    {
        network.reply(node, replyingToNode, Network.getMessageId(replyContext), reply);
    }

    @Override
    public void replyWithFailure(Id replyingToNode, ReplyContext replyContext, Throwable failure)
    {
        network.reply(node, replyingToNode, Network.getMessageId(replyContext), new FailureReply(failure));
    }
}
