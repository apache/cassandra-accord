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

import java.util.function.BiConsumer;
import javax.annotation.Nullable;

import accord.local.Node;
import accord.local.Node.Id;
import accord.api.ExclusiveAsyncExecutor;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusReply;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.*;
import accord.topology.Topologies;
import accord.topology.TopologyException;
import accord.utils.Invariants;

import static accord.topology.SelectShards.ALL;
import static accord.utils.Invariants.illegalState;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public abstract class CheckShards<R, U extends Participants<?>> extends ReadCoordinator<R, CheckStatusReply>
{
    final U query;

    /**
     * The epoch we want to fetch data from remotely
     * Either txnId.epoch() or executeAt.epoch()
     */
    final long sourceEpoch;
    final IncludeInfo includeInfo;
    final @Nullable Ballot bumpBallot;
    final Infer.InvalidIf previouslyKnownToBeInvalidIf;

    protected CheckStatusOk merged;
    protected boolean truncated;

    // srcEpoch is either txnId.epoch() or executeAt.epoch()
    protected CheckShards(Node node, ExclusiveAsyncExecutor executor, TxnId txnId, U query, IncludeInfo includeInfo, @Nullable Ballot bumpBallot, Infer.InvalidIf previouslyKnownToBeInvalidIf, BiConsumer<? super R, Throwable> callback) throws TopologyException
    {
        this(node, executor, txnId, query, txnId.epoch(), includeInfo, bumpBallot, previouslyKnownToBeInvalidIf, callback);
    }

    protected CheckShards(Node node, ExclusiveAsyncExecutor executor, TxnId txnId, U query, long srcEpoch, IncludeInfo includeInfo, @Nullable Ballot bumpBallot, Infer.InvalidIf previouslyKnownToBeInvalidIf, BiConsumer<? super R, Throwable> callback) throws TopologyException
    {
        super(node, executor, topologyFor(node, txnId, query, srcEpoch), txnId, query, callback);
        this.sourceEpoch = srcEpoch;
        this.query = query;
        this.includeInfo = includeInfo;
        this.bumpBallot = bumpBallot;
        this.previouslyKnownToBeInvalidIf = previouslyKnownToBeInvalidIf;
        Invariants.require(txnId.isVisible());
    }

    private static Topologies topologyFor(Node node, TxnId txnId, Unseekables<?> contact, long epoch) throws TopologyException
    {
        // TODO (desired): only fetch data from source epoch
        return node.topology().active().preciseEpochs(contact, txnId.epoch(), epoch, ALL);
    }

    @Override
    protected void contact(Id id)
    {
        Participants<?> unseekables = query.overlapping(topologies().computeRangesForNode(id));
        node.send(id, new CheckStatus(txnId, unseekables, sourceEpoch, includeInfo, bumpBallot), executor, this, tracing);
    }

    protected boolean isSufficient(Id from, CheckStatusOk ok) { return isSufficient(ok); }
    protected abstract boolean isSufficient(CheckStatusOk ok);

    protected Action checkSufficient(Id from, CheckStatusOk ok)
    {
        return isSufficient(from, ok) ? Action.Approve : Action.ApproveIfQuorum;
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
                    finishWithFailureOverride(illegalState("Submitted command to a replica that did not own the range"));
                    return Action.Aborted;
            }
        }
    }

    @Override
    protected void finishOnExhaustion()
    {
        if (merged != null && merged.map.hasFullyTruncated(query)) finishWithFailure(new Truncated(txnId, null));
        else super.finishOnExhaustion();
    }

    @Override
    public Participants<?> scope()
    {
        return query;
    }

    @Override
    public @Nullable Ballot ballot()
    {
        return bumpBallot;
    }

    @Override
    public String describe()
    {
        return "sourceEpoch=" + sourceEpoch +
               ", include=" + includeInfo +
               ", ballot=" + bumpBallot +
               ", previouslyKnownToBeInvalidIf=" + previouslyKnownToBeInvalidIf;
    }
}
