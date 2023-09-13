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

package accord.impl;

import accord.impl.basic.NodeSink;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.messages.Message;
import accord.messages.Request;
import accord.messages.TxnRequest;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface MessageListener
{
    enum ClientAction {SUBMIT, SUCCESS, FAILURE, UNKNOWN}

    void onMessage(NodeSink.Action action, Node.Id src, Node.Id to, long id, Message message);

    void onClientAction(ClientAction action, Node.Id from, TxnId id, Object message);

    static MessageListener get()
    {
        if (!DebugListener.DEBUG) return Noop.INSTANCE;
        return new DebugListener();
    }

    enum Noop implements MessageListener
    {
        INSTANCE;

        @Override
        public void onMessage(NodeSink.Action action, Node.Id src, Node.Id to, long id, Message message)
        {

        }

        @Override
        public void onClientAction(ClientAction action, Node.Id from, TxnId id, Object message)
        {

        }
    }

    class DebugListener implements MessageListener
    {
        private static final Logger logger = LoggerFactory.getLogger(DebugListener.class);
        /**
         * When running tests, if this is enabled log events will happen for each matching message type
         */
        private static final boolean DEBUG = false;
        /**
         * When this set is empty all txn events will be logged, but if only specific txn are desired then this filter will limit the logging to just those events
         */
        private static final Set<TxnId> txnIdFilter = ImmutableSet.of();
        private static final Set<TxnReplyId> txnReplies = new HashSet<>();

        private static int ACTION_SIZE = Stream.of(NodeSink.Action.values()).map(Enum::name).mapToInt(String::length).max().getAsInt();
        private static int CLIENT_ACTION_SIZE = Stream.of(ClientAction.values()).map(Enum::name).mapToInt(String::length).max().getAsInt();
        private static int ALL_ACTION_SIZE = Math.max(ACTION_SIZE, CLIENT_ACTION_SIZE);

        @Override
        public void onMessage(NodeSink.Action action, Node.Id from, Node.Id to, long id, Message message)
        {
            if (txnIdFilter.isEmpty() || containsTxnId(from, to, id, message))
                logger.debug("Message {}: From {}, To {}, id {}, Message {}", normalize(action), normalize(from), normalize(to), normalizeMessageId(id), message);
        }

        @Override
        public void onClientAction(ClientAction action, Node.Id from, TxnId id, Object message)
        {
            if (txnIdFilter.isEmpty() || txnIdFilter.contains(id))
            {
                String log = message instanceof Throwable ?
                             "Client  {}: From {}, To {}, id {}" :
                             "Client  {}: From {}, To {}, id {}, Message {}";
                logger.debug(log, normalize(action), normalize(from), normalize(from), normalize(id), normalizeClientMessage(message));
            }
        }

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

        private static String normalize(NodeSink.Action action)
        {
            return Strings.padStart(action.name(), ALL_ACTION_SIZE, ' ');
        }

        private static String normalize(ClientAction action)
        {
            return Strings.padStart(action.name(), ALL_ACTION_SIZE, ' ');
        }

        private static String normalize(Node.Id id)
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
            final Node.Id from;
            final Node.Id to;
            final long id;

            private TxnReplyId(Node.Id from, Node.Id to, long id)
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
}
