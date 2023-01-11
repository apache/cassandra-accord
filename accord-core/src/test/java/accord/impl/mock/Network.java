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

package accord.impl.mock;

import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;

public interface Network
{
    class MessageId implements ReplyContext
    {
        public final long msgId;

        public MessageId(long msgId)
        {
            this.msgId = msgId;
        }
    }

    static long getMessageId(ReplyContext ctx)
    {
        return ((MessageId) ctx).msgId;
    }

    static ReplyContext replyCtxFor(long messageId)
    {
        return new MessageId(messageId);
    }

    void send(Id from, Id to, Request request, Callback callback);
    void reply(Id from, Id replyingToNode, long replyingToMessage, Reply reply);

    Network BLACK_HOLE = new Network()
    {
        @Override
        public void send(Id from, Id to, Request request, Callback callback)
        {
            // TODO (easy, testing): log
        }

        @Override
        public void reply(Id from, Id replyingToNode, long replyingToMessage, Reply reply)
        {
            // TODO (easy, testing): log
        }
    };
}
