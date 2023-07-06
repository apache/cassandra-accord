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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import accord.local.*;
import accord.local.Node.Id;
import accord.primitives.*;
import accord.utils.MapReduceConsume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static accord.local.Status.Committed;
import accord.topology.Topology;

public class WaitOnCommit implements Request, MapReduceConsume<SafeCommandStore, Void>, PreLoadContext, Command.TransientListener
{
    private static final Logger logger = LoggerFactory.getLogger(WaitOnCommit.class);

    public static class SerializerSupport
    {
        public static WaitOnCommit create(TxnId txnId, Unseekables<?, ?> scope)
        {
            return new WaitOnCommit(txnId, scope);
        }
    }

    public final TxnId txnId;
    public final Unseekables<?, ?> scope;

    private transient Node node;
    private transient Id replyTo;
    private transient ReplyContext replyContext;
    private transient volatile int waitingOn;
    private static final AtomicIntegerFieldUpdater<WaitOnCommit> waitingOnUpdater = AtomicIntegerFieldUpdater.newUpdater(WaitOnCommit.class, "waitingOn");

    public WaitOnCommit(Id to, Topology topologies, TxnId txnId, Unseekables<?, ?> unseekables)
    {
        this.txnId = txnId;
        this.scope = unseekables.slice(topologies.rangesForNode(to));
    }

    public WaitOnCommit(TxnId txnId, Unseekables<?, ?> scope)
    {
        this.txnId = txnId;
        this.scope = scope;
    }

    @Override
    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        this.node = node;
        this.replyTo = replyToNode;
        this.replyContext = replyContext;
        node.mapReduceConsumeLocal(this, scope, txnId.epoch(), txnId.epoch(), this);
    }

    @Override
    public Void apply(SafeCommandStore safeStore)
    {
        SafeCommand safeCommand = safeStore.command(txnId);
        Command command = safeCommand.current();
        switch (command.status())
        {
            default: throw new AssertionError();
            case NotDefined:
                // TODO (expected): this could be Uninitialised and logically Truncated;
                //    can detect truncation beforehand or, better, we can pass scope to safeStore.command
                //    and have it yield a stock Truncated SafeCommand if it has been truncated
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
                waitingOnUpdater.incrementAndGet(this);
                safeCommand.addListener(this);
                safeStore.progressLog().waiting(txnId, Committed.minKnown, scope);
                break;

            case Committed:
            case PreApplied:
            case Applying:
            case Applied:
            case Invalidated:
            case Truncated:
            case ReadyToExecute:
        }
        return null;
    }

    @Override
    public void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                this, command.txnId(), command.status(), command);
        switch (command.status())
        {
            default: throw new AssertionError();
            case NotDefined:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
                return;

            case PreCommitted:
            case Committed:
            case ReadyToExecute:
            case PreApplied:
            case Applying:
            case Applied:
            case Truncated:
            case Invalidated:
        }

        if (safeCommand.removeListener(this))
            ack();
    }

    @Override
    public Void reduce(Void o1, Void o2)
    {
        return null;
    }

    @Override
    public void accept(Void result, Throwable failure)
    {
        ack();
    }

    private void ack()
    {
        if (waitingOnUpdater.decrementAndGet(this) == -1)
            node.reply(replyTo, replyContext, WaitOnCommitOk.INSTANCE);
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        return PreLoadContext.contextFor(txnId, caller, keys());
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

    @Override
    public long waitForEpoch()
    {
        return txnId.epoch();
    }
}
