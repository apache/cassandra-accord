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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import accord.local.Node;
import accord.local.SerializerSupport;
import accord.messages.AbstractEpochRequest;
import accord.messages.Accept;
import accord.messages.Apply;
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
import org.agrona.collections.ObjectHashSet;
import org.apache.cassandra.concurrent.ManyToOneConcurrentLinkedQueue;

import static accord.messages.MessageType.ACCEPT_INVALIDATE_REQ;
import static accord.messages.MessageType.ACCEPT_REQ;
import static accord.messages.MessageType.APPLY_MAXIMAL_REQ;
import static accord.messages.MessageType.APPLY_MINIMAL_REQ;
import static accord.messages.MessageType.APPLY_THEN_WAIT_UNTIL_APPLIED_REQ;
import static accord.messages.MessageType.BEGIN_INVALIDATE_REQ;
import static accord.messages.MessageType.BEGIN_RECOVER_REQ;
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
                                                                                      .build();

    private final ManyToOneConcurrentLinkedQueue<RequestContext> unframedRequests = new ManyToOneConcurrentLinkedQueue<>();
    private final LongArrayList waitForEpochs = new LongArrayList();
    private final Long2ObjectHashMap<ArrayList<RequestContext>> delayedRequests = new Long2ObjectHashMap<>();
    private final Map<Key, Message> writes = new HashMap<>();
    private Node node;
    
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
    public void handle(LocalRequest<?> message, Node node)
    {
        if (message.type().hasSideEffects())
        {
            // enqueue
            unframedRequests.add(new RequestContext(message, () -> node.scheduler().now(() -> message.process(node))));
            return;
        }
        message.process(node);
    }

    public void handle(Request request, Node.Id from, ReplyContext replyContext)
    {
        if (request.type() != null && request.type().hasSideEffects())
        {
            // enqueue
            unframedRequests.add(new RequestContext(request, () -> node.receive(request, from, replyContext)));
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
        Key key = new Key(txnId, type);
        writes.put(key, request);
    }

    public SerializerSupport.MessageProvider makeMessageProvider(TxnId txnId)
    {
        return new SerializerSupport.MessageProvider()
        {
            @Override
            public Set<MessageType> test(Set<MessageType> messages)
            {
                Set<Key> keys = new ObjectHashSet<>(messages.size() + 1, 0.9f);
                for (MessageType msg : messages)
                    keys.add(new Key(txnId, msg));
                Set<Key> presentKeys = Sets.intersection(writes.keySet(), keys);
                Set<MessageType> presentMessages = new ObjectHashSet<>(presentKeys.size() + 1, 0.9f);
                for (Key key : presentKeys)
                    presentMessages.add(key.type);
                return presentMessages;
            }

            @Override
            public Set<MessageType> all()
            {
                Set<Key> presentKeys = writes.keySet().stream().filter(k -> k.txnId.equals(txnId)).collect(Collectors.toSet());
                Set<MessageType> presentMessages = new ObjectHashSet<>(presentKeys.size() + 1, 0.9f);
                for (Key key : presentKeys)
                    presentMessages.add(key.type);
                return presentMessages;
            }

            private <T extends Message> T get(Key key)
            {
                return (T) writes.get(key);
            }

            private <T extends Message> T get(MessageType type)
            {
                return get(new Key(txnId, type));
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
        };
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

        protected RequestContext(Request request, Runnable fn)
        {
            this.waitForEpoch = request.waitForEpoch();
            this.message = request;
            this.fn = fn;
        }

        @Override
        public void run()
        {
            fn.run();
        }
    }

    private static class Key
    {
        final TxnId txnId;
        final MessageType type;

        private Key(TxnId txnId, MessageType type)
        {
            this.txnId = txnId;
            this.type = type;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return Objects.equals(txnId, key.txnId) && Objects.equals(type, key.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(txnId, type);
        }

        @Override
        public String toString()
        {
            return "{" +
                   "txnId=" + txnId +
                   ", type=" + type +
                   '}';
        }
    }
}