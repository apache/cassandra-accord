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

import accord.api.Result;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.Commands.ApplyOutcome;
import accord.local.SafeCommandStore;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.primitives.Writes;

/*
 * Used by local and global inclusive sync points to effect the sync point at each node
 * Combines commit, execute (with nothing really to execute), and apply into one request/response
 */
public class WaitForDependenciesThenApply extends WhenReadyToExecute
{
    private static final Logger logger = LoggerFactory.getLogger(WhenReadyToExecute.class);

    @SuppressWarnings("unused")
    public static class SerializerSupport
    {
        public static WaitForDependenciesThenApply create(TxnId txnId, PartialRoute<?> route, PartialDeps deps, Seekables<?, ?> partialTxnKeys, Writes writes, Result result, boolean notifyAgent)
        {
            return new WaitForDependenciesThenApply(txnId, route, deps, partialTxnKeys, writes, result, notifyAgent);
        }
    }

    public final PartialRoute<?> route;
    public final PartialDeps deps;
    public final Writes writes;
    public final Result result;
    public final boolean notifyAgent;

    WaitForDependenciesThenApply(TxnId txnId, PartialRoute<?> route, PartialDeps deps, Seekables<?, ?> partialTxnKeys, Writes writes, Result result, boolean notifyAgent)
    {
        super(txnId, partialTxnKeys, txnId.epoch(), txnId.epoch());
        this.route = route;
        this.deps = deps;
        this.writes = writes;
        this.result = result;
        this.notifyAgent = notifyAgent;
    }

    @Override
    protected void readyToExecute(SafeCommandStore safeStore, Command.Committed command)
    {
        logger.trace("{}: executing WaitForDependenciesThenApply", command.txnId());
        CommandStore unsafeStore = safeStore.commandStore();
        if (notifyAgent)
            node.agent().onLocalBarrier(scope, txnId);
        // Send a response immediately once deps have been applied
        onExecuteComplete(unsafeStore);
        // Apply after, we aren't waiting for the side effects of this txn since it is part of an inclusive sync
        ApplyOutcome outcome = Commands.apply(safeStore, txnId, txnId.epoch(), route, txnId, deps, writes, result);
        switch (outcome)
        {
            default:
                throw new IllegalStateException("Unexpected outcome " + outcome);
            case Success:
                // TODO fine to ignore the other outcomes?
                break;
        }
    }

    @Override
    public ExecuteType kind()
    {
        return ExecuteType.waitAndApply;
    }

    @Override
    protected void failed()
    {
    }

    @Override
    protected void sendSuccessReply()
    {
        node.reply(replyTo, replyContext, new ExecuteOk(null));
    }

    @Override
    public String toString()
    {
        return "WaitForDependenciesThenApply{" +
                "txnId:" + txnId +
                '}';
    }
}
