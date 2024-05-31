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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import accord.impl.MessageListener;
import accord.local.Bootstrap;
import accord.local.Node;
import accord.local.SerializerSupport;
import accord.messages.AbstractEpochRequest;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.ApplyThenWaitUntilApplied;
import accord.messages.BeginRecovery;
import accord.messages.Commit;
import accord.messages.LocalRequest;
import accord.messages.Message;
import accord.messages.MessageType;
import accord.messages.PreAccept;
import accord.messages.Propagate;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.messages.TxnRequest;
import accord.primitives.Ballot;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;

import static accord.messages.MessageType.ACCEPT_INVALIDATE_REQ;
import static accord.messages.MessageType.ACCEPT_REQ;
import static accord.messages.MessageType.APPLY_MAXIMAL_REQ;
import static accord.messages.MessageType.APPLY_MINIMAL_REQ;
import static accord.messages.MessageType.APPLY_THEN_WAIT_UNTIL_APPLIED_REQ;
import static accord.messages.MessageType.BEGIN_INVALIDATE_REQ;
import static accord.messages.MessageType.BEGIN_RECOVER_REQ;
import static accord.messages.MessageType.BOOTSTRAP_ATTEMPT_COMPLETE_MARKER;
import static accord.messages.MessageType.BOOTSTRAP_ATTEMPT_MARK_BOOTSTRAP_COMPLETE;
import static accord.messages.MessageType.COMMIT_INVALIDATE_REQ;
import static accord.messages.MessageType.COMMIT_MAXIMAL_REQ;
import static accord.messages.MessageType.COMMIT_SLOW_PATH_REQ;
import static accord.messages.MessageType.INFORM_DURABLE_REQ;
import static accord.messages.MessageType.INFORM_OF_TXN_REQ;
import static accord.messages.MessageType.PRE_ACCEPT_REQ;
import static accord.messages.MessageType.PROPAGATE_APPLY_MSG;
import static accord.messages.MessageType.PROPAGATE_OTHER_MSG;
import static accord.messages.MessageType.PROPAGATE_PRE_ACCEPT_MSG;
import static accord.messages.MessageType.PROPAGATE_STABLE_MSG;
import static accord.messages.MessageType.SET_GLOBALLY_DURABLE_REQ;
import static accord.messages.MessageType.SET_SHARD_DURABLE_REQ;
import static accord.messages.MessageType.STABLE_FAST_PATH_REQ;
import static accord.messages.MessageType.STABLE_MAXIMAL_REQ;
import static accord.messages.MessageType.STABLE_SLOW_PATH_REQ;

public class Journal implements LocalRequest.Handler, Runnable
{
    private static final TxnIdProvider EPOCH = msg -> ((AbstractEpochRequest<?>) msg).txnId;
    private static final TxnIdProvider TXN   = msg -> ((TxnRequest<?>) msg).txnId;
    private static final TxnIdProvider LOCAL = msg -> ((LocalRequest<?>) msg).primaryTxnId();
    private static final TxnIdProvider INVL  = msg -> ((Commit.Invalidate) msg).primaryTxnId();
    private static final Map<MessageType, TxnIdProvider> typeToProvider = ImmutableMap.<MessageType, TxnIdProvider>builder()
                                                                                      .put(PRE_ACCEPT_REQ, TXN)
                                                                                      .put(ACCEPT_REQ, TXN)
                                                                                      .put(ACCEPT_INVALIDATE_REQ, EPOCH)
                                                                                      .put(COMMIT_SLOW_PATH_REQ, TXN)
                                                                                      .put(COMMIT_MAXIMAL_REQ, TXN)
                                                                                      .put(STABLE_FAST_PATH_REQ, TXN)
                                                                                      .put(STABLE_SLOW_PATH_REQ, TXN)
                                                                                      .put(STABLE_MAXIMAL_REQ, TXN)
                                                                                      .put(COMMIT_INVALIDATE_REQ, INVL)
                                                                                      .put(APPLY_MINIMAL_REQ, TXN)
                                                                                      .put(APPLY_MAXIMAL_REQ, TXN)
                                                                                      .put(APPLY_THEN_WAIT_UNTIL_APPLIED_REQ, EPOCH)
                                                                                      .put(BEGIN_RECOVER_REQ, TXN)
                                                                                      .put(BEGIN_INVALIDATE_REQ, EPOCH)
                                                                                      .put(INFORM_OF_TXN_REQ, EPOCH)
                                                                                      .put(INFORM_DURABLE_REQ, TXN)
                                                                                      .put(SET_SHARD_DURABLE_REQ, EPOCH)
                                                                                      .put(SET_GLOBALLY_DURABLE_REQ, EPOCH)
                                                                                      .put(PROPAGATE_PRE_ACCEPT_MSG, LOCAL)
                                                                                      .put(PROPAGATE_STABLE_MSG, LOCAL)
                                                                                      .put(PROPAGATE_APPLY_MSG, LOCAL)
                                                                                      .put(PROPAGATE_OTHER_MSG, LOCAL)
                                                                                      .put(BOOTSTRAP_ATTEMPT_COMPLETE_MARKER, LOCAL)
                                                                                      .put(BOOTSTRAP_ATTEMPT_MARK_BOOTSTRAP_COMPLETE, LOCAL)
                                                                                      .build();

    private final Queue<RequestContext> unframedRequests = new ArrayDeque<>();
    private final LongArrayList waitForEpochs = new LongArrayList();
    private final Long2ObjectHashMap<ArrayList<RequestContext>> delayedRequests = new Long2ObjectHashMap<>();
    private final Map<TxnId, Map<MessageType, Message>> writes = new HashMap<>();
    private final MessageListener messageListener;
    private Node node;

    public Journal(MessageListener messageListener)
    {
        this.messageListener = messageListener;
    }

    public void start(Node node)
    {
        this.node = node;
        node.scheduler().recurring(this, 1, TimeUnit.MILLISECONDS);
    }

    public void shutdown()
    {
        this.node = null;
    }

    @Override
    public <R> void handle(LocalRequest<R> message, BiConsumer<? super R, Throwable> callback, Node node)
    {
        messageListener.onMessage(NodeSink.Action.DELIVER, node.id(), node.id(), -1, message);
        if (message.type().hasSideEffects())
        {
            // enqueue
            unframedRequests.add(new RequestContext(message, message.waitForEpoch(), () -> node.scheduler().now(() -> message.process(node, callback))));
            return;
        }
        message.process(node, callback);
    }

    public void handle(Request request, Node.Id from, ReplyContext replyContext)
    {
        if (request.type() != null && request.type().hasSideEffects())
        {
            // enqueue
            unframedRequests.add(new RequestContext(request, request.waitForEpoch(), () -> node.receive(request, from, replyContext)));
            return;
        }
        node.receive(request, from, replyContext);
    }

    private void save(Message request)
    {
        MessageType type = request.type();
        TxnIdProvider provider = typeToProvider.get(type);
        Invariants.nonNull(provider, "Unknown type %s: %s", type, request);
        TxnId txnId = provider.txnId(request);
        writes.computeIfAbsent(txnId, ignore -> new Testing()).put(type, request);
    }

    public SerializerSupport.MessageProvider makeMessageProvider(TxnId txnId)
    {
        return new MessageProvider(txnId, writes.getOrDefault(txnId, Map.of()));
    }

    private static class Testing extends LinkedHashMap<MessageType, Message>
    {
        public Map<MessageType, List<Message>> history()
        {
            LinkedHashMap<MessageType, List<Message>> history = new LinkedHashMap<>();
            for (MessageType k : keySet())
            {
                Object current = super.get(k);
                history.put(k, current instanceof List ? (List<Message>) current : Collections.singletonList((Message) current));
            }
            return history;
        }

        @Override
        public Message get(Object key)
        {
            Object current = super.get(key);
            if (current == null || current instanceof Message)
                return (Message) current;
            List<Message> messages = (List<Message>) current;
            return messages.get(messages.size() - 1);
        }

        @Override
        public Message put(MessageType key, Message value)
        {
            Object current = super.get(key);
            if (current == null)
                return super.put(key, value);
            else if (current instanceof List)
            {
                List<Message> list = (List<Message>) current;
                list.add(value);
                return list.get(list.size() - 2);
            }
            else
            {
                List<Message> messages = new ArrayList<>();
                messages.add((Message) current);
                messages.add(value);
                super.put(key, value);
                return (Message) current;
            }
        }
    }

    @Override
    public void run()
    {
        if (this.node == null)
            return;
        try
        {
            doRun();
        }
        catch (Throwable t)
        {
            node.agent().onUncaughtException(t);
        }
    }

    private void doRun()
    {
        ArrayList<RequestContext> requests = null;
        // check to see if any pending epochs are in
        waitForEpochs.sort(null);
        for (int i = 0; i < waitForEpochs.size(); i++)
        {
            long waitForEpoch = waitForEpochs.getLong(i);
            if (!node.topology().hasEpoch(waitForEpoch))
                break;
            List<RequestContext> delayed = delayedRequests.remove(waitForEpoch);
            if (null == requests) requests = new ArrayList<>(delayed.size());
            requests.addAll(delayed);
        }
        waitForEpochs.removeIfLong(epoch -> !delayedRequests.containsKey(epoch));
        
        // for anything queued, put into the pending epochs or schedule
        RequestContext request;
        while (null != (request = unframedRequests.poll()))
        {
            long waitForEpoch = request.waitForEpoch;
            if (waitForEpoch != 0 && !node.topology().hasEpoch(waitForEpoch))
            {
                delayedRequests.computeIfAbsent(waitForEpoch, ignore -> new ArrayList<>()).add(request);
                if (!waitForEpochs.containsLong(waitForEpoch))
                    waitForEpochs.addLong(waitForEpoch);
            }
            else
            {
                if (null == requests) requests = new ArrayList<>();
                requests.add(request);
            }
        }
        
        // schedule
        if (requests != null)
        {
            requests.forEach(r -> save(r.message)); // save in batches to simulate journal more...
            requests.forEach(Runnable::run);
        }
    }

    @FunctionalInterface
    interface TxnIdProvider
    {
        TxnId txnId(Message message);
    }

    private static class RequestContext implements Runnable
    {
        final long waitForEpoch;
        final Message message;
        final Runnable fn;

        protected RequestContext(Message request, long waitForEpoch, Runnable fn)
        {
            this.waitForEpoch = waitForEpoch;
            this.message = request;
            this.fn = fn;
        }

        @Override
        public void run()
        {
            fn.run();
        }
    }

    public static class MessageProvider implements SerializerSupport.MessageProvider
    {
        public final TxnId txnId;
        private final Map<MessageType, Message> writes;

        public MessageProvider(TxnId txnId, Map<MessageType, Message> writes)
        {
            this.txnId = txnId;
            this.writes = writes;
        }

        @Override
        public TxnId txnId()
        {
            return txnId;
        }

        @Override
        public Set<MessageType> test(Set<MessageType> messages)
        {
            return Sets.intersection(writes.keySet(), messages);
        }

        @Override
        public Set<MessageType> all()
        {
            return writes.keySet();
        }

        public Map<MessageType, Message> allMessages()
        {
            var all = all();
            Map<MessageType, Message> map = Maps.newHashMapWithExpectedSize(all.size());
            for (MessageType messageType : all)
                map.put(messageType, get(messageType));
            return map;
        }

        public  <T extends Message> T get(MessageType type)
        {
            return (T) writes.get(type);
        }

        @Override
        public PreAccept preAccept()
        {
            return get(PRE_ACCEPT_REQ);
        }

        @Override
        public BeginRecovery beginRecover()
        {
            return get(BEGIN_RECOVER_REQ);
        }

        @Override
        public Propagate propagatePreAccept()
        {
            return get(PROPAGATE_PRE_ACCEPT_MSG);
        }

        @Override
        public Accept accept(Ballot ballot)
        {
            return get(ACCEPT_REQ);
        }

        @Override
        public Commit commitSlowPath()
        {
            return get(COMMIT_SLOW_PATH_REQ);
        }

        @Override
        public Commit commitMaximal()
        {
            return get(COMMIT_MAXIMAL_REQ);
        }

        @Override
        public Commit stableFastPath()
        {
            return get(STABLE_FAST_PATH_REQ);
        }

        @Override
        public Commit stableSlowPath()
        {
            return get(STABLE_SLOW_PATH_REQ);
        }

        @Override
        public Commit stableMaximal()
        {
            return get(STABLE_MAXIMAL_REQ);
        }

        @Override
        public Propagate propagateStable()
        {
            return get(PROPAGATE_STABLE_MSG);
        }

        @Override
        public Apply applyMinimal()
        {
            return get(APPLY_MINIMAL_REQ);
        }

        @Override
        public Apply applyMaximal()
        {
            return get(APPLY_MAXIMAL_REQ);
        }

        @Override
        public Propagate propagateApply()
        {
            return get(PROPAGATE_APPLY_MSG);
        }

        @Override
        public Propagate propagateOther()
        {
            return get(PROPAGATE_OTHER_MSG);
        }

        @Override
        public ApplyThenWaitUntilApplied applyThenWaitUntilApplied()
        {
            return get(APPLY_THEN_WAIT_UNTIL_APPLIED_REQ);
        }

        @Override
        public Bootstrap.CreateBootstrapCompleteMarkerTransaction bootstrapAttemptCompleteMarker()
        {
            return get(BOOTSTRAP_ATTEMPT_COMPLETE_MARKER);
        }

        @Override
        public Bootstrap.MarkBootstrapComplete bootstrapAttemptMarkBootstrapComplete()
        {
            return get(BOOTSTRAP_ATTEMPT_MARK_BOOTSTRAP_COMPLETE);
        }
    }
}
