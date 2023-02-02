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

import accord.local.Command;
import accord.local.Node;
import accord.api.Agent;
import accord.api.Result;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import java.util.concurrent.TimeUnit;

public class MaelstromAgent implements Agent
{
    static final MaelstromAgent INSTANCE = new MaelstromAgent();

    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        if (success != null)
        {
            MaelstromResult result = (MaelstromResult) success;
            node.reply(result.client, MaelstromReplyContext.contextFor(result.requestId), new MaelstromReply(result.requestId, result));
        }
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        throw new AssertionError();
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
    public boolean isExpired(TxnId initiated, long now)
    {
        return TimeUnit.SECONDS.convert(now - initiated.hlc(), TimeUnit.MICROSECONDS) >= 10;
    }
}
