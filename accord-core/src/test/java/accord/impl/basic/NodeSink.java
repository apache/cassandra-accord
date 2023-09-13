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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import accord.api.MessageSink;
import accord.local.AgentExecutor;
import accord.burn.random.FrequentLargeRange;
import accord.messages.SafeCallback;
import accord.messages.Message;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Reply.FailureReply;
import accord.messages.ReplyContext;
import accord.messages.Request;

import static accord.impl.basic.Packet.SENTINEL_MESSAGE_ID;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NodeSink implements MessageSink
{
    public enum Action {DELIVER, DROP, DROP_PARTITIONED, DELIVER_WITH_FAILURE, FAILURE}

    private final Map<Id, Supplier<Action>> nodeActions = new HashMap<>();
    private final Map<Id, LongSupplier> networkJitter = new HashMap<>();
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
        maybeEnqueue(to, SENTINEL_MESSAGE_ID, send, null);
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
            }, 100 + random.nextInt(200), TimeUnit.MILLISECONDS);
            parent.pending.add((PendingRunnable) () -> {
                if (sc == callbacks.remove(messageId))
                    sc.timeout(to);
            }, 1000 + random.nextInt(1000), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void reply(Id replyToNode, ReplyContext replyContext, Reply reply)
    {
        maybeEnqueue(replyToNode, Packet.getMessageId(replyContext), reply, null);
    }

    private boolean maybeEnqueue(Node.Id to, long id, Message message, SafeCallback callback)
    {
        Runnable task = () -> {
            Packet packet;
            if (message instanceof Reply) packet = new Packet(self, to, id, (Reply) message);
            else                          packet = new Packet(self, to, id, (Request) message);
            parent.add(packet, networkJitterNanos(to), TimeUnit.NANOSECONDS);
        };
        if (to.equals(self) || lookup.apply(to) == null /* client */)
        {
            parent.messageListener.onMessage(Action.DELIVER, self, to, id, message);
            task.run();
            return true;
        }

        Action action = partitioned(to) ? Action.DROP_PARTITIONED
                                        // call actions() per node so each one has different "runs" state
                                        : nodeActions.computeIfAbsent(to, ignore -> actions()).get();
        parent.messageListener.onMessage(action, self, to, id, message);
        switch (action)
        {
            case DELIVER:
                task.run();
                return true;
            case DELIVER_WITH_FAILURE:
                task.run();
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
                                callback.failure(to, new SimulatedFault("Simulation Failure; src=" + self + ", to=" + to + ", id=" + id + ", message=" + message));
                            }
                            catch (Throwable t)
                            {
                                callback.onCallbackFailure(to, t);
                                lookup.apply(self).agent().onUncaughtException(t);
                            }
                        }
                    }, 1000 + random.nextInt(1000), TimeUnit.MILLISECONDS);
                }
                return false;
            case DROP_PARTITIONED:
            case DROP:
                // TODO (consistency): parent.notifyDropped is a trace logger that is very similar in spirit to MessageListener; can we unify?
                parent.notifyDropped(self, to, id, message);
                return true;
            default:
                throw new AssertionError("Unexpected action: " + action);
        }
    }

    private long networkJitterNanos(Node.Id dst)
    {
        return networkJitter.computeIfAbsent(dst, ignore -> defaultJitter())
                            .getAsLong();
    }

    private LongSupplier defaultJitter()
    {
        return FrequentLargeRange.builder(random)
                                 .ratio(1, 5)
                                 .small(500, TimeUnit.MICROSECONDS, 5, TimeUnit.MILLISECONDS)
                                 .large(50, TimeUnit.MILLISECONDS, 5, SECONDS)
                                 .build()
                                 .asLongSupplier(random);
    }

    private boolean partitioned(Id to)
    {
        return parent.partitionSet.contains(self) != parent.partitionSet.contains(to);
    }

    private Supplier<Action> actions()
    {
        Gen<Boolean> drops = Gens.bools().biasedRepeatingRuns(0.01);
        Gen<Boolean> failures = Gens.bools().biasedRepeatingRuns(0.01);
        Gen<Action> actionGen = rs -> {
            if (drops.next(rs))
                return Action.DROP;
            return failures.next(rs) ?
                   rs.nextBoolean() ? Action.FAILURE : Action.DELIVER_WITH_FAILURE
                   : Action.DELIVER;
        };
        return actionGen.asSupplier(random);
    }

    @Override
    public void replyWithUnknownFailure(Id replyingToNode, ReplyContext replyContext, Throwable failure)
    {
        reply(replyingToNode, replyContext, new FailureReply(failure));
    }
}
