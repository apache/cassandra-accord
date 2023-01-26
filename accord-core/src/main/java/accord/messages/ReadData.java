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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;

public class ReadData extends WhenReadyToExecute
{
    private static final Logger logger = LoggerFactory.getLogger(WhenReadyToExecute.class);

    public static class SerializerSupport
    {
        public static ReadData create(TxnId txnId, Seekables<?, ?> scope, long executeAtEpoch, long waitForEpoch)
        {
            return new ReadData(txnId, scope, executeAtEpoch, waitForEpoch);
        }
    }

    private Data data;

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Seekables<?, ?> readScope, Timestamp executeAt)
    {
        super(to, topologies, txnId, readScope, executeAt);
    }

    ReadData(TxnId txnId, Seekables<?, ?> readScope, long executeAtEpoch, long waitForEpoch)
    {
        super(txnId, readScope, executeAtEpoch, waitForEpoch);
    }

    @Override
    public ExecuteType kind()
    {
        return ExecuteType.readData;
    }

    @Override
    protected void readyToExecute(SafeCommandStore safeStore, Command.Committed command)
    {
        CommandStore unsafeStore = safeStore.commandStore();
        logger.trace("{}: executing read", command.txnId());
        command.read(safeStore).begin((next, throwable) -> {
            if (throwable != null)
            {
                // TODO (expected, exceptions): should send exception to client, and consistency handle/propagate locally
                logger.trace("{}: read failed for {}: {}", txnId, unsafeStore, throwable);
                node.reply(replyTo, replyContext, ExecuteNack.Error);
            }
            else
            {
                synchronized (ReadData.this)
                {
                    if (next != null)
                        data = data == null ? next : data.merge(next);
                    onExecuteComplete(unsafeStore);
                }
            }
        });
    }

    @Override
    protected void failed()
    {
        data = null;
    }

    @Override
    protected void sendSuccessReply()
    {
        node.reply(replyTo, replyContext, new ExecuteOk(data));
    }

    @Override
    public String toString()
    {
        return "ReadData{" +
                "txnId:" + txnId +
                '}';
    }
}
