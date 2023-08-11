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

package accord.impl.list;

import java.util.function.Consumer;

import accord.api.Agent;
import accord.api.Result;
import accord.impl.mock.Network;
import accord.local.Command;
import accord.local.Node;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import static accord.local.Node.Id.NONE;
import static accord.utils.Invariants.checkState;
import static com.google.common.base.Functions.identity;

public class ListAgent implements Agent
{
    final long timeout;
    final Consumer<Throwable> onFailure;
    final Consumer<Runnable> retryBootstrap;

    public ListAgent(long timeout, Consumer<Throwable> onFailure, Consumer<Runnable> retryBootstrap)
    {
        this.timeout = timeout;
        this.onFailure = onFailure;
        this.retryBootstrap = retryBootstrap;
    }

    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        if (fail != null)
        {
            checkState(success == null, "fail (%s) and success (%s) are both not null", fail, success);
            // Can't respond without success and requestId
            node.agent().onUncaughtException(fail);
        }
        if (success != null)
        {
            ListResult result = (ListResult) success;
            if (result.requestId > Integer.MIN_VALUE)
                node.reply(result.client, Network.replyCtxFor(result.requestId), result, null);
        }
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        throw new AssertionError("Inconsistent execution timestamp detected for txnId " + command.txnId() + ": " + prev + " != " + next);
    }

    @Override
    public void onFailedBootstrap(String phase, Ranges ranges, Runnable retry, Throwable failure)
    {
        retryBootstrap.accept(retry);
    }

    @Override
    public void onUncaughtException(Throwable t)
    {
        // TODO (required, testing): ensure reported to runner
        onFailure.accept(t);
    }

    @Override
    public void onHandledException(Throwable t)
    {
    }

    @Override
    public boolean isExpired(TxnId initiated, long now)
    {
        return now - initiated.hlc() >= timeout && !initiated.rw().isSyncPoint();
    }

    @Override
    public Txn emptyTxn(Txn.Kind kind, Seekables<?, ?> keysOrRanges)
    {
        return new Txn.InMemory(kind, keysOrRanges, new ListRead(identity(), Keys.EMPTY, Keys.EMPTY), new ListQuery(NONE, Integer.MIN_VALUE), null);
    }
}
