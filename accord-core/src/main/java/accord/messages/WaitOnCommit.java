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

package accord.messages;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import accord.api.Key;
import accord.local.*;
import accord.local.Node.Id;
import accord.topology.Topologies;
import accord.primitives.TxnId;
import accord.primitives.Keys;
import accord.utils.VisibleForImplementation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static accord.utils.Utils.listOf;

public class WaitOnCommit extends TxnRequest
{
    private static final Logger logger = LoggerFactory.getLogger(WaitOnCommit.class);

    static class LocalWait implements Listener, TxnOperation
    {
        final Node node;
        final Id replyToNode;
        final TxnId txnId;
        final ReplyContext replyContext;

        final AtomicInteger waitingOn = new AtomicInteger();

        LocalWait(Node node, Id replyToNode, TxnId txnId, ReplyContext replyContext)
        {
            this.node = node;
            this.replyToNode = replyToNode;
            this.txnId = txnId;
            this.replyContext = replyContext;
        }

        @Override
        public Iterable<TxnId> txnIds()
        {
            return Collections.singleton(txnId);
        }

        @Override
        public Iterable<Key> keys()
        {
            return Collections.emptyList();
        }

        @Override
        public TxnOperation listenerScope(TxnId caller)
        {
            return TxnOperation.scopeFor(listOf(txnId, caller), keys());
        }

        @Override
        public String toString()
        {
            return "WaitOnCommit$LocalWait{" + txnId + '}';
        }

        @Override
        public synchronized void onChange(Command command)
        {
            logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                         this, command.txnId(), command.status(), command);
            switch (command.status())
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                    return;

                case Committed:
                case Executed:
                case Applied:
                case Invalidated:
                case ReadyToExecute:
            }

            command.removeListener(this);
            ack();
        }

        @Override
        public boolean isTransient()
        {
            return true;
        }

        private void ack()
        {
            if (waitingOn.decrementAndGet() == 0)
                node.reply(replyToNode, replyContext, WaitOnCommitOk.INSTANCE);
        }

        void setup(Keys keys, CommandStore instance)
        {
            Command command = instance.command(txnId);
            switch (command.status())
            {
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                    command.addListener(this);
                    instance.progressLog().waiting(command, keys);
                    break;

                case Committed:
                case Executed:
                case Applied:
                case Invalidated:
                case ReadyToExecute:
                    ack();
            }
        }

        synchronized void setup(Keys keys)
        {
            List<CommandStore> instances = node.collectLocal(keys, txnId, ArrayList::new);
            waitingOn.set(instances.size());
            CommandStore.onEach(this, instances, instance -> setup(keys, instance));
        }
    }

    public final TxnId txnId;

    public WaitOnCommit(Id to, Topologies topologies, TxnId txnId, Keys keys)
    {
        super(to, topologies, keys);
        this.txnId = txnId;
    }

    @VisibleForImplementation
    public WaitOnCommit(Keys scope, long waitForEpoch, TxnId txnId)
    {
        super(scope, waitForEpoch);
        this.txnId = txnId;
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Iterable<Key> keys()
    {
        return scope();
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        new LocalWait(node, replyToNode, txnId, replyContext).setup(scope());
    }

    @Override
    public MessageType type()
    {
        return MessageType.WAIT_ON_COMMIT_REQ;
    }

    public static class WaitOnCommitOk implements Reply
    {
        public static final WaitOnCommitOk INSTANCE = new WaitOnCommitOk();

        private WaitOnCommitOk() {}

        @Override
        public MessageType type()
        {
            return MessageType.WAIT_ON_COMMIT_RSP;
        }
    }
}
