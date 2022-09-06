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
import accord.local.TxnOperation;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.topology.Shard;
import accord.primitives.Keys;
import accord.primitives.TxnId;

import static accord.local.Status.Committed;

/**
 * Check on the status of a locally-uncommitted transaction. Returns early if any result indicates Committed, otherwise
 * waits only for a quorum and returns the maximum result.
 *
 * Updates local command stores based on the obtained information.
 */
public class CheckOnUncommitted extends CheckOnCommitted
{
    final Keys someKeys;
    CheckOnUncommitted(Node node, TxnId txnId, Keys someKeys, Key someKey, Shard someShard, long shardEpoch)
    {
        super(node, txnId, someKey, someShard, shardEpoch);
        this.someKeys = someKeys;
    }

    public static CheckOnUncommitted checkOnUncommitted(Node node, TxnId txnId, Keys someKeys, Key someKey, Shard someShard, long shardEpoch)
    {
        CheckOnUncommitted checkOnUncommitted = new CheckOnUncommitted(node, txnId, someKeys, someKey, someShard, shardEpoch);
        checkOnUncommitted.start();
        return checkOnUncommitted;
    }

    @Override
    boolean hasMetSuccessCriteria()
    {
        return tracker.hasReachedQuorum() || hasCommitted();
    }

    public boolean hasCommitted()
    {
        return max != null && max.status.hasBeen(Committed);
    }

    @Override
    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull full)
    {
        switch (full.status)
        {
            default: throw new IllegalStateException();
            case Invalidated:
                node.forEachLocalSince(TxnOperation.scopeFor(txnId), someKeys, txnId.epoch, commandStore -> {
                    Command command = commandStore.ifPresent(txnId);
                    if (command != null)
                        command.commitInvalidate();
                });
            case NotWitnessed:
            case AcceptedInvalidate:
                break;
            case PreAccepted:
            case Accepted:
                node.forEachLocalSince(TxnOperation.scopeFor(txnId), full.txn.keys(), txnId.epoch, commandStore -> {
                    Command command = commandStore.ifPresent(txnId);
                    if (command != null)
                        command.homeKey(full.homeKey);
                });
                break;
            case Executed:
            case Applied:
            case Committed:
            case ReadyToExecute:
                super.onSuccessCriteriaOrExhaustion(full);
        }
    }
}
