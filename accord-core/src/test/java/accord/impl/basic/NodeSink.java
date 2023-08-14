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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import accord.api.MessageSink;
import accord.local.AgentExecutor;
import accord.burn.random.FrequentLargeRange;
import accord.local.PreLoadContext;
import accord.messages.SafeCallback;
import accord.messages.Message;
import accord.messages.TxnRequest;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static accord.impl.basic.Packet.SENTINEL_MESSAGE_ID;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NodeSink implements MessageSink
{
    private static final boolean DEBUG = false;
    private enum Action {DELIVER, DROP, DROP_PARTITIONED, DELIVER_WITH_FAILURE, FAILURE}
    public enum ClientAction {SUBMIT, SUCCESS, FAILURE, UNKNOWN}

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
            debug(to, id, message, Action.DELIVER);
            Packet packet;
            if (message instanceof Reply) packet = new Packet(self, to, id, (Reply) message);
            else                          packet = new Packet(self, to, id, (Request) message);
            parent.add(packet, networkJitterNanos(to), TimeUnit.NANOSECONDS);
        };
        if (to.equals(self) || lookup.apply(to) == null /* client */)
        {
            task.run();
            return true;
        }

        Action action = partitioned(to) ? Action.DROP_PARTITIONED
                                        // call actions() per node so each one has different "runs" state
                                        : nodeActions.computeIfAbsent(to, ignore -> actions()).get();
        switch (action)
        {
            case DELIVER:
                task.run();
                return true;
            case DELIVER_WITH_FAILURE:
                task.run();
            case FAILURE:
                debug(to, id, message, action);
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
                debug(to, id, message, action);
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
                                 .raitio(1, 5)
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
            return failures.next(rs) ? Action.FAILURE : Action.DELIVER;
        };
        return actionGen.asSupplier(random);
    }

    private void debug(Id to, long id, Message message, Action action)
    {
        if (!DEBUG)
            return;
        if (Debug.txnIdFilter.isEmpty() || Debug.containsTxnId(self, to, id, message))
            Debug.logger.debug("Message {}: From {}, To {}, id {}, Message {}", Debug.normalize(action), Debug.normalize(self), Debug.normalize(to), Debug.normalizeMessageId(id), message);
    }

    public void debugClient(TxnId id, Object message, ClientAction action)
    {
        if (!DEBUG)
            return;
        if (Debug.txnIdFilter.isEmpty() || Debug.txnIdFilter.contains(id))
        {
            String log = message instanceof Throwable ? "Client  {}: From {}, To {}, id {}" : "Client  {}: From {}, To {}, id {}, Message {}";
            Debug.logger.debug(log, Debug.normalize(action), Debug.normalize(self), Debug.normalize(self), Debug.normalize(id), Debug.normalizeClientMessage(message));
        }
    }

    private static class Debug
    {
        private static final Logger logger = LoggerFactory.getLogger(Debug.class);
        // to limit logging to specific TxnId, list in the set below
        private static final Set<TxnId> txnIdFilter = ImmutableSet.of();
        private static final Set<TxnReplyId> txnReplies = new HashSet<>();

        private static int ACTION_SIZE = Stream.of(Action.values()).map(Enum::name).mapToInt(String::length).max().getAsInt();
        private static int CLIENT_ACTION_SIZE = Stream.of(ClientAction.values()).map(Enum::name).mapToInt(String::length).max().getAsInt();
        private static int ALL_ACTION_SIZE = Math.max(ACTION_SIZE, CLIENT_ACTION_SIZE);

        private static Object normalizeClientMessage(Object o)
        {
            if (o instanceof Throwable)
                trimStackTrace((Throwable) o);
            return o;
        }

        private static void trimStackTrace(Throwable input)
        {
            for (Throwable current = input; current != null; current = current.getCause())
            {
                StackTraceElement[] stack = current.getStackTrace();
                // remove junit as its super dense and not helpful
                OptionalInt first = IntStream.range(0, stack.length).filter(i -> stack[i].getClassName().startsWith("org.junit")).findFirst();
                if (first.isPresent())
                    current.setStackTrace(Arrays.copyOfRange(stack, 0, first.getAsInt()));
                for (Throwable sup : current.getSuppressed())
                    trimStackTrace(sup);
            }
        }

        private static String normalize(Action action)
        {
            return Strings.padStart(action.name(), ALL_ACTION_SIZE, ' ');
        }

        private static String normalize(ClientAction action)
        {
            return Strings.padStart(action.name(), ALL_ACTION_SIZE, ' ');
        }

        private static String normalize(Id id)
        {
            return Strings.padStart(id.toString(), 4, ' ');
        }

        private static String normalizeMessageId(long id)
        {
            return Strings.padStart(Long.toString(id), 14, ' ');
        }

        private static String normalize(Timestamp ts)
        {
            return Strings.padStart(ts.toString(), 14, ' ');
        }

        public static boolean containsTxnId(Node.Id from, Node.Id to, long id, Message message)
        {
            if (message instanceof Request)
            {
                if (containsAny((Request) message))
                {
                    txnReplies.add(new TxnReplyId(from, to, id));
                    return true;
                }
                return false;
            }
            else
                return txnReplies.contains(new TxnReplyId(to, from, id));
        }

        private static boolean containsAny(Request message)
        {
            if (message instanceof TxnRequest<?>)
                return txnIdFilter.contains(((TxnRequest<?>) message).txnId);
            // this includes txn that depend on the txn, should this limit for the first txnId?
            if (message instanceof PreLoadContext)
                return ((PreLoadContext) message).txnIds().stream().anyMatch(txnIdFilter::contains);
            return false;
        }

        private static class TxnReplyId
        {
            final Id from;
            final Id to;
            final long id;

            private TxnReplyId(Id from, Id to, long id)
            {
                this.from = from;
                this.to = to;
                this.id = id;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                TxnReplyId that = (TxnReplyId) o;
                return id == that.id && Objects.equals(from, that.from) && Objects.equals(to, that.to);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(from, to, id);
            }

            @Override
            public String toString()
            {
                return "TxnReplyId{" +
                        "from=" + from +
                        ", to=" + to +
                        ", id=" + id +
                        '}';
            }
        }
    }

    @Override
    public void replyWithUnknownFailure(Id replyingToNode, ReplyContext replyContext, Throwable failure)
    {
        reply(replyingToNode, replyContext, new FailureReply(failure));
    }
}
