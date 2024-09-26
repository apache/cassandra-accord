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

import accord.local.DurableBefore;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.TxnId;

import static accord.messages.SimpleReply.Ok;

// TODO (required): if we have epochs 1 and 2, and a syncpoint that executes on epoch 2, where the transactions have not
//    finished executing on epoch 1, it may not be safe to mark durable on replicas on epoch 1. This is a very unlikely
//    race condition, but must consider our behaviour - it may be simpler to wait for a sync point to execute on all epochs
//    that haven't been closed off.
public class SetGloballyDurable extends AbstractEpochRequest<SimpleReply>
        implements Request, PreLoadContext
{
    public final DurableBefore durableBefore;

    public SetGloballyDurable(TxnId txnId, DurableBefore durableBefore)
    {
        super(txnId);
        this.durableBefore = durableBefore;
    }

    @Override
    public void process()
    {
        node.mapReduceConsumeAllLocal(this, this);
    }

    @Override
    public SimpleReply apply(SafeCommandStore safeStore)
    {
        DurableBefore cur = safeStore.commandStore().durableBefore();
        DurableBefore upd = DurableBefore.merge(durableBefore, cur);
        // This is done asynchronously
        safeStore.upsertDurableBefore(upd);
        return Ok;
    }

    @Override
    public SimpleReply reduce(SimpleReply r1, SimpleReply r2)
    {
        return r1.merge(r2);
    }

    @Override
    public void accept(SimpleReply ok, Throwable failure)
    {
    }

    @Override
    public String toString()
    {
        return "SetGloballyDurable{" + durableBefore + '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.SET_GLOBALLY_DURABLE_REQ;
    }
}
