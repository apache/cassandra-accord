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

package accord.maelstrom;

import accord.api.Agent;
import accord.api.Result;
import accord.local.Command;
import accord.local.Node;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static accord.utils.Invariants.checkState;

public class MaelstromAgent implements Agent
{
    static final MaelstromAgent INSTANCE = new MaelstromAgent();

    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        if (fail != null)
        {
            checkState(success == null, "fail (%s) and success (%s) are both not null", fail, success);
            // We don't really process errors for Recover here even though it is provided in the interface
        }
        if (success != null)
        {
            MaelstromResult result = (MaelstromResult) success;
            node.reply(result.client, MaelstromReplyContext.contextFor(result.requestId), new MaelstromReply(result.requestId, result), null);
        }
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        throw new AssertionError();
    }

    @Override
    public void onFailedBootstrap(String phase, Ranges ranges, Runnable retry, Throwable failure)
    {
        throw new AssertionError();
    }

    @Override
    public void onStale(Timestamp staleSince, Ranges ranges)
    {
    }

    @Override
    public void onUncaughtException(Throwable t)
    {
    }

    @Override
    public void onHandledException(Throwable t)
    {
    }

    @Override
    public long preAcceptTimeout()
    {
        return MICROSECONDS.convert(10, SECONDS);
    }

    @Override
    public long cfkHlcPruneDelta()
    {
        return 1000;
    }

    @Override
    public int cfkPruneInterval()
    {
        return 1;
    }

    @Override
    public Txn emptyTxn(Txn.Kind kind, Seekables<?, ?> keysOrRanges)
    {
        return new Txn.InMemory(kind, keysOrRanges, new MaelstromRead(Keys.EMPTY, Keys.EMPTY), new MaelstromQuery(Node.Id.NONE, -1), null);
    }
}
