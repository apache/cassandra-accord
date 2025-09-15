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

package accord.coordinate;

import javax.annotation.Nullable;

import accord.local.Node;
import accord.local.SequentialAsyncExecutor;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.utils.Invariants;

// TODO (desired): move failure and callback handling here,
//  so we can standardise and cleanup as we advance the state machines
public abstract class AbstractSimpleCoordination<P extends Participants<?>> implements Coordination
{
    final long coordinationId;
    protected final Node node;
    protected final SequentialAsyncExecutor executor;
    protected final TxnId txnId;
    protected final P scope;
    private Throwable failure;
    private boolean isDoneWithReplies, isFinishing, isDone;

    protected AbstractSimpleCoordination(Node node, SequentialAsyncExecutor executor, TxnId txnId, P scope)
    {
        this.coordinationId = node.nextCoordinationId();
        this.node = node;
        this.executor = executor;
        this.txnId = Invariants.nonNull(txnId);
        this.scope = scope;
    }

    @Override
    public final long coordinationId()
    {
        return coordinationId;
    }

    @Override
    public final TxnId txnId()
    {
        return txnId;
    }

    public final P scope() { return scope; }

    @Override
    public final SequentialAsyncExecutor executor()
    {
        return executor;
    }

    void start()
    {
        node.register(this);
    }

    void setDone()
    {
        Invariants.require(!isDone);
        ensureDone();
    }

    void ensureDone()
    {
        isDoneWithReplies = isFinishing = isDone = true;
        node.unregister(this);
    }

    protected boolean isDone()
    {
        return isDone;
    }

    void setDoneWithReplies()
    {
        isDoneWithReplies = true;
    }

    void setFinishing()
    {
        Invariants.require(!isFinishing);
        isDoneWithReplies = isFinishing = true;
    }

    boolean isDoneWithReplies()
    {
        return isDoneWithReplies;
    }

    boolean isFinishing()
    {
        return isFinishing;
    }

    Throwable failure()
    {
        return failure;
    }

    void recordFailure(@Nullable Throwable failure)
    {
        this.failure = FailureAccumulator.append(failure, this.failure);
    }
}
