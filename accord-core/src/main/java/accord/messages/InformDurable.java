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

import javax.annotation.Nullable;

import accord.local.Commands;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.local.Status.Durability;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.local.PreLoadContext.contextFor;
import static accord.messages.SimpleReply.Ok;

public class InformDurable extends TxnRequest<Reply> implements PreLoadContext
{
    public static class SerializationSupport
    {
        public static InformDurable create(TxnId txnId, Route<?> scope, long waitForEpoch, Timestamp executeAt, Durability durability)
        {
            return new InformDurable(txnId, scope, waitForEpoch, executeAt, durability);
        }
    }

    public final @Nullable Timestamp executeAt;
    public final Durability durability;

    public InformDurable(Id to, Topologies topologies, Route<?> route, TxnId txnId, @Nullable Timestamp executeAt, Durability durability)
    {
        super(to, topologies, route, txnId);
        this.executeAt = executeAt;
        this.durability = durability;
    }

    private InformDurable(TxnId txnId, Route<?> scope, long waitForEpoch, @Nullable Timestamp executeAt, Durability durability)
    {
        super(txnId, scope, waitForEpoch);
        this.executeAt = executeAt;
        this.durability = durability;
    }

    @Override
    public void process()
    {
        // TODO (expected, efficiency): do not load from disk to perform this update
        // TODO (expected, consider): do we need to send this to all epochs in between, or just execution epoch?
        node.mapReduceConsumeLocal(contextFor(txnId), scope, txnId.epoch(), executeAt != null ? executeAt.epoch() : txnId.epoch(), this);
    }

    @Override
    public Reply apply(SafeCommandStore safeStore)
    {
        SafeCommand safeCommand = safeStore.get(txnId, txnId, scope);
        if (safeCommand.current().is(Status.Truncated))
            return Ok;

        Commands.setDurability(safeStore, safeCommand, durability, scope, executeAt);
        return Ok;
    }

    @Override
    public Reply reduce(Reply o1, Reply o2)
    {
        return Ok;
    }

    @Override
    public void accept(Reply reply, Throwable failure)
    {
        node.reply(replyTo, replyContext, reply, failure);
    }

    @Override
    public String toString()
    {
        return "InformOfPersistence{" +
               "txnId:" + txnId +
               '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.INFORM_DURABLE_REQ;
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }
}
