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
import java.util.function.Function;

import accord.api.MessageSink;
import accord.impl.basic.Cluster.Link;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.Message;
import accord.messages.Reply;
import accord.messages.Reply.FailureReply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.messages.SafeCallback;
import accord.utils.RandomSource;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class NodeSink implements MessageSink
{
    private static final boolean DEBUG = false;
    public enum Action { DELIVER, DROP, DELIVER_WITH_FAILURE, FAILURE }

    final Id self;
    final Function<Id, Node> lookup;
    final Cluster parent;
    final RandomSource random;

    int nextMessageId = 0;
    Map<Long, SafeCallback> callbacks = new LinkedHashMap<>();

    public NodeSink(Id self, Function<Id, Node> lookup, Cluster parent, RandomSource random)
    {
        this.self = self;
        this.lookup = lookup;
        this.parent = parent;
        this.random = random;
    }

    @Override
    public void send(Id to, Request send)
    {
        maybeEnqueue(to, nextMessageId++, send, null);
    }

    @Override
    public void send(Id to, Request send, AgentExecutor executor, Callback callback)
    {
        long messageId = nextMessageId++;
        SafeCallback sc = new SafeCallback(executor, callback);
        callbacks.put(messageId, sc);
        if (maybeEnqueue(to, messageId, send, sc))
        {
            parent.pending.add((PendingRunnable) () -> {
                if (sc == callbacks.get(messageId))
                    sc.slowResponse(to);
            }, 100 + random.nextInt(200), MILLISECONDS);
            parent.pending.add((PendingRunnable) () -> {
                if (sc == callbacks.remove(messageId))
                    sc.timeout(to);
            }, 1000 + random.nextInt(1000), MILLISECONDS);
        }
    }

    @Override
    public void reply(Id replyToNode, ReplyContext replyContext, Reply reply)
    {
        maybeEnqueue(replyToNode, Packet.getMessageId(replyContext), reply, null);
    }

    private boolean maybeEnqueue(Node.Id to, long id, Message message, SafeCallback callback)
    {
        Link link = parent.links.apply(self, to);
        if (to.equals(self) || lookup.apply(to) == null /* client */)
        {
            parent.messageListener.onMessage(Action.DELIVER, self, to, id, message);
            deliver(to, id, message, link);
            return true;
        }

        Action action = link.action.get();
        parent.messageListener.onMessage(action, self, to, id, message);
        switch (action)
        {
            case DELIVER:
                deliver(to, id, message, link);
                return true;
            case DELIVER_WITH_FAILURE:
                deliver(to, id, message, link);
            case FAILURE:
                if (action == Action.FAILURE)
                    parent.notifyDropped(self, to, id, message);
                if (callback != null)
                {
                    parent.pending.add((PendingRunnable) () -> {
                        if (callback == callbacks.remove(id))
                        {
                            try
                            {
                                // TODO (now): we MUST drop any ACTUAL reply now
                                callback.failure(to, new SimulatedFault("Simulation Failure; src=" + self + ", to=" + to + ", id=" + id + ", message=" + message));
                            }
                            catch (Throwable t)
                            {
                                callback.onCallbackFailure(to, t);
                                lookup.apply(self).agent().onUncaughtException(t);
                            }
                        }
                    }, 1000 + random.nextInt(1000), MILLISECONDS);
                }
                return false;
            case DROP:
                // TODO (consistency): parent.notifyDropped is a trace logger that is very similar in spirit to MessageListener; can we unify?
                parent.notifyDropped(self, to, id, message);
                return true;
            default:
                throw new AssertionError("Unexpected action: " + action);
        }
    }

    private void deliver(Node.Id to, long id, Message message, Link link)
    {
        Packet packet;
        if (message instanceof Reply) packet = new Packet(self, to, id, (Reply) message);
        else packet = new Packet(self, to, id, (Request) message);
        parent.add(packet, link.latencyMicros.getAsLong(), MICROSECONDS);
    }

    @Override
    public void replyWithUnknownFailure(Id replyingToNode, ReplyContext replyContext, Throwable failure)
    {
        reply(replyingToNode, replyContext, new FailureReply(failure));
    }
}
