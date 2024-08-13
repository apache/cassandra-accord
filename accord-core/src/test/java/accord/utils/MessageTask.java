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

package accord.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;

import accord.local.AgentExecutor;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.utils.async.AsyncResults;

/**
 * Message task that will continue sending messages to a set of nodes until all
 * nodes ack the message.
 */
public class MessageTask extends AsyncResults.SettableResult<Void> implements Runnable
{
    public interface NodeProcess
    {
        void process(Node node, Node.Id from, Consumer<Boolean> onDone);
    }

    private static final Reply SUCCESS = new Reply() {
        @Override
        public MessageType type()
        {
            return null;
        }

        @Override
        public String toString()
        {
            return "SUCCESS";
        }
    };

    private static final Reply FAILURE = new Reply() {
        @Override
        public MessageType type()
        {
            return null;
        }

        @Override
        public String toString()
        {
            return "FAILURE";
        }
    };

    private final Node originator;
    private final List<Node.Id> recipients;
    private final String desc;
    private final Request request;
    private final AgentExecutor executor;
    private final RetryingCallback callback;

    private class TaskRequest implements Request
    {
        private final NodeProcess process;
        private final String desc;

        public TaskRequest(NodeProcess process, String desc)
        {
            this.process = process;
            this.desc = desc;
        }

        @Override
        public void process(Node on, Node.Id from, ReplyContext replyContext)
        {
            process.process(on, from, success -> on.reply(from, replyContext, success ? SUCCESS : FAILURE, null));
        }

        @Override
        public MessageType type()
        {
            return null;
        }

        @Override
        public String toString()
        {
            return "TaskRequest{" + desc + '}';
        }
    }

    private class RetryingCallback implements Callback<Reply>
    {
        private final Set<Node.Id> outstanding;

        public RetryingCallback(Collection<Node.Id> outstanding)
        {
            this.outstanding = new HashSet<>(outstanding);
        }

        @Override
        public void onSuccess(Node.Id from, Reply reply)
        {
            Invariants.checkArgument(reply == SUCCESS || reply == FAILURE);
            if (reply == FAILURE)
            {
                originator.send(from, request, executor, this);
                return;
            }

            synchronized (this)
            {
                this.outstanding.remove(from);
                if (outstanding.isEmpty())
                    setSuccess(null);
            }
        }

        @Override
        public void onFailure(Node.Id from, Throwable failure)
        {
            originator.send(from, request, executor, this);
        }

        @Override
        public void onCallbackFailure(Node.Id from, Throwable failure)
        {
            tryFailure(failure);
        }
    }

    private MessageTask(Node originator,
                        List<Node.Id> recipients,
                        AgentExecutor executor, String desc, NodeProcess process)
    {
        Invariants.checkArgument(!recipients.isEmpty());
        this.originator = originator;
        this.recipients = ImmutableList.copyOf(recipients);
        this.desc = desc;
        this.request = new TaskRequest(process, desc);
        this.callback = new RetryingCallback(recipients);
        this.executor = executor;
    }

    private static MessageTask of(Node originator, Collection<Node.Id> recipients, AgentExecutor executor, String desc, NodeProcess process)
    {
        return new MessageTask(originator, new ArrayList<>(recipients), executor, desc, process);
    }

    public static MessageTask begin(Node originator, Collection<Node.Id> recipients, AgentExecutor executor, String desc, NodeProcess process)
    {
        MessageTask task = of(originator, recipients, executor, desc, process);
        executor.execute(task);
        return task;
    }

    public static MessageTask of(Node originator, Collection<Node.Id> recipients, AgentExecutor executor, String desc, BiConsumer<Node, Consumer<Boolean>> consumer)
    {
        NodeProcess process = (node, from, onDone) -> consumer.accept(node, onDone);
        return of(originator, recipients, executor, desc, process);
    }

    public static MessageTask apply(Node originator, Collection<Node.Id> recipients, AgentExecutor executor, String desc, NodeProcess process)
    {
        MessageTask task = of(originator, recipients, executor, desc, process);
        task.run();
        return task;
    }

    @Override
    public void run()
    {
        originator.send(recipients, request, executor, callback);
    }

    @Override
    public String toString()
    {
        return "MessageTask{" + desc + '}';
    }
}
