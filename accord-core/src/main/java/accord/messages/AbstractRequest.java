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

import accord.local.MapReduceConsumeCommandStores;
import accord.local.Node;
import accord.primitives.Participants;
import accord.primitives.TxnId;

// TODO (expected): merge cancel/timeout logic here from NoWaitRequest and ReadRequest
// TODO (expected): migrate to ExclusiveExecutor approach used by Coordinator logic, rather than synchronized
// TODO (desired): allow a task to be associated with more than one ExclusiveExecutor, and only commit a thread when both are ready to schedule it
public abstract class AbstractRequest<P extends Participants<?>, R> extends MapReduceConsumeCommandStores<P, R> implements Request
{
    protected transient Node node;
    protected transient Node.Id replyTo;
    protected transient ReplyContext replyContext;

    public final TxnId txnId;

    protected AbstractRequest(TxnId txnId, P scope)
    {
        super(scope);
        this.txnId = txnId;
    }

    @Nullable
    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }
}
