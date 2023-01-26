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
import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.messages.Apply.ApplyReply;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.primitives.Writes;

/*
 * Used by local and global inclusive sync points to effect the sync point at each node
 * Combines commit, execute (with nothing really to execute), and apply into one request/response
 */
public class ApplyThenWaitUntilApplied extends WaitUntilApplied
{
    private static final Logger logger = LoggerFactory.getLogger(ReadData.class);

    @SuppressWarnings("unused")
    public static class SerializerSupport
    {
        public static ApplyThenWaitUntilApplied create(TxnId txnId, PartialRoute<?> route, PartialDeps deps, Seekables<?, ?> partialTxnKeys, Writes writes, Result result, boolean notifyAgent)
        {
            return new ApplyThenWaitUntilApplied(txnId, route, deps, partialTxnKeys, writes, result, notifyAgent);
        }
    }

    public final PartialRoute<?> route;
    public final PartialDeps deps;
    public final Writes writes;
    public final Result txnResult;
    public final boolean notifyAgent;
    public final Seekables<?, ?> partialTxnKeys;

    ApplyThenWaitUntilApplied(TxnId txnId, PartialRoute<?> route, PartialDeps deps, Seekables<?, ?> partialTxnKeys, Writes writes, Result txnResult, boolean notifyAgent)
    {
        super(txnId, partialTxnKeys.toParticipants(), txnId, txnId.epoch());
        this.route = route;
        this.deps = deps;
        this.writes = writes;
        this.txnResult = txnResult;
        this.notifyAgent = notifyAgent;
        this.partialTxnKeys = partialTxnKeys;
    }

    @Override
    public ReadType kind()
    {
        return ReadType.applyThenWaitUntilApplied;
    }

    @Override
    public ReadNack apply(SafeCommandStore safeStore)
    {
        RoutingKey progressKey = TxnRequest.progressKey(node, txnId.epoch(), txnId, route);
        ApplyReply applyReply = Apply.apply(safeStore, null, txnId, txnId, deps, route, writes, txnResult, progressKey);
        switch (applyReply)
        {
            default:
                throw new IllegalStateException("Unexpected ApplyReply");
            case Insufficient:
                throw new IllegalStateException("ApplyThenWaitUntilApplied is always sent with a maximal `Commit` so how can `Apply` have an `Insufficient` result");
            case Redundant:
            case Applied:
                // In both cases it's fine to continue to process and return a response saying
                // things were applied
                break;
        }
        return super.apply(safeStore);
    }

    @Override
    protected void readComplete(CommandStore commandStore, Data readResult, Ranges unavailable)
    {
        logger.trace("{}: readComplete ApplyThenWaitUntilApplied", txnId);
        commandStore.execute(PreLoadContext.contextFor(txnId), safeStore -> {
            super.readComplete(commandStore, readResult, unavailable);
        }).begin(node.agent());
    }

    @Override
    protected void onAllReadsComplete()
    {
        if (notifyAgent)
            node.agent().onLocalBarrier(partialTxnKeys, txnId);
    }

    @Override
    public String toString()
    {
        return "WaitForDependenciesThenApply{" +
                "txnId:" + txnId +
                '}';
    }
}
