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

import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusReply;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;

import static accord.primitives.DataConsistencyLevel.INVALID;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public abstract class CheckShards extends ReadCoordinator<CheckStatusReply>
{
    final Unseekables<?, ?> contact;

    /**
     * The epoch until which we want to fetch data from remotely
     * TODO (required, consider): configure the epoch we want to start with
     */
    final long untilRemoteEpoch;
    final IncludeInfo includeInfo;

    protected CheckStatusOk merged;

    protected CheckShards(Node node, TxnId txnId, Unseekables<?, ?> contact, long srcEpoch, IncludeInfo includeInfo)
    {
        super(node, topologyFor(node, txnId, contact, srcEpoch), txnId, INVALID);
        this.untilRemoteEpoch = srcEpoch;
        this.contact = contact;
        this.includeInfo = includeInfo;
    }

    private static Topologies topologyFor(Node node, TxnId txnId, Unseekables<?, ?> contact, long epoch)
    {
        return node.topology().preciseEpochs(contact, txnId.epoch(), epoch);
    }

    @Override
    protected void contact(Id id)
    {
        Unseekables<?, ?> unseekables = contact.slice(topologies().computeRangesForNode(id));
        node.send(id, new CheckStatus(txnId, unseekables, txnId.epoch(), untilRemoteEpoch, includeInfo), this);
    }

    protected boolean isSufficient(Id from, CheckStatusOk ok) { return isSufficient(ok); }
    protected abstract boolean isSufficient(CheckStatusOk ok);

    protected Action checkSufficient(Id from, CheckStatusOk ok)
    {
        if (isSufficient(from, ok))
            return Action.Approve;

        return Action.ApproveIfQuorum;
    }

    @Override
    protected Action process(Id from, CheckStatusReply reply)
    {
        if (debug != null)
            debug.put(from, reply);
        
        if (reply.isOk())
        {
            CheckStatusOk ok = (CheckStatusOk) reply;
            if (merged == null) merged = ok;
            else merged = merged.merge(ok);

            return checkSufficient(from, ok);
        }
        else
        {
            onFailure(from, new IllegalStateException("Submitted command to a replica that did not own the range"));
            return Action.Abort;
        }
    }
}
