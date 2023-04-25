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

import accord.local.AgentExecutor;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;

import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RecordingMessageSink extends SimpleMessageSink
{
    public static class Envelope<T>
    {
        public final Node.Id to;
        public final T payload;
        public final Callback callback;

        public Envelope(Node.Id to, T payload, Callback callback)
        {
            this.to = to;
            this.payload = payload;
            this.callback = callback;
        }
    }

    public final List<Envelope<Request>> requests = Collections.synchronizedList(new ArrayList<>());
    public final List<Envelope<Reply>> responses = Collections.synchronizedList(new ArrayList<>());

    public RecordingMessageSink(Node.Id node, Network network)
    {
        super(node, network);
    }

    @Override
    public void send(Node.Id to, Request request)
    {
        requests.add(new Envelope<>(to, request, null));
        super.send(to, request);
    }

    @Override
    public void send(Node.Id to, Request request, AgentExecutor executor, Callback callback)
    {
        requests.add(new Envelope<>(to, request, callback));
        super.send(to, request, executor, callback);
    }

    @Override
    public void reply(Node.Id replyingToNode, ReplyContext replyContext, Reply reply)
    {
        responses.add(new Envelope<>(replyingToNode, reply, null));
        super.reply(replyingToNode, replyContext, reply);
    }

    public void assertHistorySizes(int requests, int responses)
    {
        Assertions.assertEquals(requests, this.requests.size());
        Assertions.assertEquals(responses, this.responses.size());
    }

    public void clearHistory()
    {
        requests.clear();
        responses.clear();
    }
}
