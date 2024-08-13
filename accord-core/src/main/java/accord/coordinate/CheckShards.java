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
import accord.primitives.*;
import accord.topology.Topologies;
import accord.utils.Invariants;

import static accord.utils.Invariants.illegalState;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public abstract class CheckShards<U extends Unseekables<?>> extends ReadCoordinator<CheckStatusReply>
{
    final U route;

    /**
     * The epoch we want to fetch data from remotely
     * Either txnId.epoch() or executeAt.epoch()
     */
    final long sourceEpoch;
    final IncludeInfo includeInfo;

    protected CheckStatusOk merged;
    protected boolean truncated;

    // srcEpoch is either txnId.epoch() or executeAt.epoch()
    protected CheckShards(Node node, TxnId txnId, U route, IncludeInfo includeInfo)
    {
        this(node, txnId, route, txnId.epoch(), includeInfo);
        Invariants.checkState(txnId.kind().isGloballyVisible());
    }

    protected CheckShards(Node node, TxnId txnId, U route, long srcEpoch, IncludeInfo includeInfo)
    {
        super(node, topologyFor(node, txnId, route, srcEpoch), txnId);
        this.sourceEpoch = srcEpoch;
        this.route = route;
        this.includeInfo = includeInfo;
    }

    private static Topologies topologyFor(Node node, TxnId txnId, Unseekables<?> contact, long epoch)
    {
        // TODO (expected): only fetch data from source epoch
        return node.topology().preciseEpochs(contact, txnId.epoch(), epoch);
    }

    @Override
    protected void contact(Id id)
    {
        Unseekables<?> unseekables = route.slice(topologies().computeRangesForNode(id));
        node.send(id, new CheckStatus(txnId, unseekables, sourceEpoch, includeInfo), this);
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
        if (reply.isOk())
        {
            CheckStatusOk ok = (CheckStatusOk) reply;
            if (merged == null) merged = ok;
            else merged = merged.merge(ok);
            return checkSufficient(from, ok);
        }
        else
        {
            switch ((CheckStatus.CheckStatusNack)reply)
            {
                default: throw new AssertionError(String.format("Unexpected status: %s", reply));
                case NotOwned:
                    finishOnFailure(illegalState("Submitted command to a replica that did not own the range"), true);
                    return Action.Aborted;
            }
        }
    }

    @Override
    protected void finishOnExhaustion()
    {
        if (merged != null && merged.isTruncatedResponse()) finishOnFailure(new Truncated(txnId, null), false);
        else super.finishOnExhaustion();
    }
}
