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

import accord.local.Node;
import accord.local.ExecutionContext;
import accord.local.DurableBefore;
import accord.primitives.TxnId;
import accord.utils.async.Cancellable;

import static accord.messages.MessageType.StandardMessage.GET_DURABLE_BEFORE_REQ;
import static accord.messages.MessageType.StandardMessage.GET_DURABLE_BEFORE_RSP;

public class GetDurableBefore implements Request, ExecutionContext
{
    public GetDurableBefore()
    {
    }

    @Override
    public Cancellable process(Node node, Node.Id replyTo, ReplyContext replyContext)
    {
        node.reply(replyTo, replyContext, new DurableBeforeReply(node.durableBefore()), null, null);
        return null;
    }

    @Override
    public String toString()
    {
        return "QueryDurableBefore";
    }

    @Override
    public MessageType type()
    {
        return GET_DURABLE_BEFORE_REQ;
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
        return "GetDurableBefore";
    }

    public static class DurableBeforeReply implements Reply
    {
        public final DurableBefore durableBefore;

        public DurableBeforeReply(DurableBefore durableBefore)
        {
            this.durableBefore = durableBefore;
        }

        @Override
        public MessageType type()
        {
            return GET_DURABLE_BEFORE_RSP;
        }

        @Override
        public String toString()
        {
            return durableBefore.toString();
        }
    }
}
