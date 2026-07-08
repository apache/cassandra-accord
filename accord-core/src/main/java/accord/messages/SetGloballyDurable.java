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

import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.ExecutionContext;
import accord.primitives.TxnId;
import accord.utils.async.Cancellable;

import static accord.messages.MessageType.StandardMessage.SET_GLOBALLY_DURABLE_REQ;
import static accord.messages.SimpleReply.Ok;

public class SetGloballyDurable implements Request, ExecutionContext
{
    public final DurableBefore durableBefore;

    public SetGloballyDurable(DurableBefore durableBefore)
    {
        this.durableBefore = durableBefore;
    }

    @Override
    public Cancellable process(Node node, Node.Id from, ReplyContext replyContext)
    {
        node.markDurable(durableBefore).invoke((success, fail) -> {
            node.reply(from, replyContext, fail == null ? Ok : null, fail, null);
        });
        return null;
    }

    @Override
    public String toString()
    {
        return "SetGloballyDurable{" + durableBefore + '}';
    }

    @Override
    public MessageType type()
    {
        return SET_GLOBALLY_DURABLE_REQ;
    }

    @Nullable
    @Override
    public TxnId primaryTxnId()
    {
        return null;
    }

    @Override
    public String reason()
    {
        return "SetGloballyDurable";
    }
}
