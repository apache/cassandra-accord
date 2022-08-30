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

import accord.api.Key;
import accord.local.Command;
import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.topology.Shard;
import accord.primitives.TxnId;

import static accord.local.Status.Executed;

/**
 * Check on the status of a locally-uncommitted transaction. Returns early if any result indicates Committed, otherwise
 * waits only for a quorum and returns the maximum result.
 *
 * Updates local command stores based on the obtained information.
 */
public class CheckOnCommitted extends CheckShardStatus<CheckStatusOkFull>
{
    CheckOnCommitted(Node node, TxnId txnId, Key someKey, Shard someShard, long someEpoch)
    {
        super(node, txnId, someKey, someShard, someEpoch, IncludeInfo.Always);
    }

    public static CheckOnCommitted checkOnCommitted(Node node, TxnId txnId, Key someKey, Shard someShard, long shardEpoch)
    {
        CheckOnCommitted checkOnCommitted = new CheckOnCommitted(node, txnId, someKey, someShard, shardEpoch);
        checkOnCommitted.start();
        return checkOnCommitted;
    }

    @Override
    boolean hasMetSuccessCriteria()
    {
        return tracker.hasReachedQuorum() || hasApplied();
    }

    public boolean hasApplied()
    {
        return max != null && max.status.hasBeen(Executed);
    }

    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull max)
    {
        switch (max.status)
        {
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case Invalidated:
                return;
        }

        Key progressKey = node.trySelectProgressKey(txnId, max.txn.keys, max.homeKey);
        switch (max.status)
        {
            default: throw new IllegalStateException();
            case Executed:
            case Applied:
                node.forEachLocalSince(max.txn.keys, max.executeAt.epoch, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.apply(max.txn, max.homeKey, progressKey, max.executeAt, max.deps, max.writes, max.result);
                });
                node.forEachLocal(max.txn.keys, txnId.epoch, max.executeAt.epoch - 1, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.commit(max.txn, max.homeKey, progressKey, max.executeAt, max.deps);
                });
                break;
            case Committed:
            case ReadyToExecute:
                node.forEachLocalSince(max.txn.keys, txnId.epoch, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.commit(max.txn, max.homeKey, progressKey, max.executeAt, max.deps);
                });
        }
    }

    @Override
    void onSuccessCriteriaOrExhaustion()
    {
        CheckStatusOkFull full = (CheckStatusOkFull)max;
        try
        {
            onSuccessCriteriaOrExhaustion(full);
        }
        catch (Throwable t)
        {
            trySuccess(full);
            throw t;
        }
        trySuccess(full);
    }
}
