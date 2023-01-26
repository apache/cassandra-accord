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

package accord.impl.basic;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import accord.utils.RandomSource;
import accord.coordinate.Timeout;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.MessageSink;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;

import static accord.impl.basic.Packet.SENTINEL_MESSAGE_ID;

public class NodeSink implements MessageSink
{
    final Id self;
    final Function<Id, Node> lookup;
    final Cluster parent;
    final RandomSource random;

    int nextMessageId = 0;
    Map<Long, Callback> callbacks = new LinkedHashMap<>();

    public NodeSink(Id self, Function<Id, Node> lookup, Cluster parent, RandomSource random)
    {
        this.self = self;
        this.lookup = lookup;
        this.parent = parent;
        this.random = random;
    }

    @Override
    public synchronized void send(Id to, Request send)
    {
        parent.add(self, to, SENTINEL_MESSAGE_ID, send);
    }

    @Override
    public void send(Id to, Request send, Callback callback)
    {
        long messageId = nextMessageId++;
        callbacks.put(messageId, callback);
        parent.add(self, to, messageId, send);
        parent.pending.add((PendingRunnable) () -> {
            if (callback == callbacks.get(messageId))
            {
                try
                {
                    callback.onSlowResponse(to);
                }
                catch (Throwable t)
                {
                    callback.onCallbackFailure(to, t);
                    lookup.apply(self).agent().onUncaughtException(t);
                }

            }
        }, 100 + random.nextInt(200), TimeUnit.MILLISECONDS);
        parent.pending.add((PendingRunnable) () -> {
            if (callback == callbacks.remove(messageId))
            {
                try
                {
                    callback.onFailure(to, new Timeout(null, null));
                }
                catch (Throwable t)
                {
                    callback.onCallbackFailure(to, t);
                    lookup.apply(self).agent().onUncaughtException(t);
                }
            }
        }, 1000 + random.nextInt(10000), TimeUnit.MILLISECONDS);
    }

    @Override
    public void reply(Id replyToNode, ReplyContext replyContext, Reply reply)
    {
        parent.add(self, replyToNode, Packet.getMessageId(replyContext), reply);
    }
}
