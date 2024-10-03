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
import accord.local.PreLoadContext;
import accord.local.DurableBefore;
import accord.primitives.TxnId;

public class QueryDurableBefore implements Request, PreLoadContext
{
    final long epoch;
    public QueryDurableBefore(long epoch)
    {
        this.epoch = epoch;
    }

    @Override
    public void process(Node node, Node.Id replyTo, ReplyContext replyContext)
    {
        node.reply(replyTo, replyContext, new DurableBeforeReply(node.durableBefore()), null);
    }

    @Override
    public String toString()
    {
        return "QueryDurableBefore{" + epoch + '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.QUERY_DURABLE_BEFORE_REQ;
    }

    @Override
    public long waitForEpoch()
    {
        return epoch;
    }

    @Nullable
    @Override
    public TxnId primaryTxnId()
    {
        return null;
    }

    public static class DurableBeforeReply implements Reply
    {
        public final DurableBefore durableBeforeMap;

        public DurableBeforeReply(DurableBefore durableBeforeMap)
        {
            this.durableBeforeMap = durableBeforeMap;
        }

        @Override
        public MessageType type()
        {
            return MessageType.QUERY_DURABLE_BEFORE_RSP;
        }
    }
}
