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

import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.TxnId;
import accord.utils.MapReduceConsume;

public abstract class AbstractEpochRequest<R extends Reply> implements PreLoadContext, Request, MapReduceConsume<SafeCommandStore, R>
{
    public final TxnId txnId;
    protected transient Node node;
    protected transient Node.Id replyTo;
    protected transient ReplyContext replyContext;

    protected AbstractEpochRequest(TxnId txnId)
    {
        this.txnId = txnId;
    }

    @Override
    public void process(Node on, Node.Id replyTo, ReplyContext replyContext)
    {
        this.node = on;
        this.replyTo = replyTo;
        this.replyContext = replyContext;
        process();
    }

    protected abstract void process();

    @Override
    public R reduce(R o1, R o2)
    {
        throw new IllegalStateException();
    }

    @Override
    public void accept(R reply, Throwable failure)
    {
        node.reply(replyTo, replyContext, reply, failure);
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }
}
