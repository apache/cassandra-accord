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

import accord.local.Status;
import accord.local.Status.Known;
import accord.primitives.*;
import accord.utils.Invariants;

import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.topology.Topologies;

import javax.annotation.Nullable;

import static accord.messages.CheckStatus.WithQuorum.HasQuorum;
import static accord.messages.CheckStatus.WithQuorum.NoQuorum;
import static accord.messages.Commit.Invalidate.commitInvalidate;
import static accord.primitives.ProgressToken.APPLIED;
import static accord.primitives.ProgressToken.DURABLE;
import static accord.primitives.ProgressToken.INVALIDATED;
import static accord.primitives.ProgressToken.TRUNCATED;
import static accord.primitives.Route.castToFullRoute;

public class RecoverWithRoute extends CheckShards<FullRoute<?>>
{
    final @Nullable Ballot promisedBallot; // if non-null, has already been promised by some shard
    final BiConsumer<Outcome, Throwable> callback;
    final Status witnessedByInvalidation;

    private RecoverWithRoute(Node node, Topologies topologies, @Nullable Ballot promisedBallot, TxnId txnId, FullRoute<?> route, Status witnessedByInvalidation, BiConsumer<Outcome, Throwable> callback)
    {
        super(node, txnId, route, IncludeInfo.All);
        // if witnessedByInvalidation == AcceptedInvalidate then we cannot assume its definition was known, and our comparison with the status is invalid
        Invariants.checkState(witnessedByInvalidation != Status.AcceptedInvalidate);
        // if witnessedByInvalidation == Invalidated we should anyway not be recovering
        Invariants.checkState(witnessedByInvalidation != Status.Invalidated);
        this.promisedBallot = promisedBallot;
        this.callback = callback;
        this.witnessedByInvalidation = witnessedByInvalidation;
        assert topologies.oldestEpoch() == topologies.currentEpoch() && topologies.currentEpoch() == txnId.epoch();
    }

    public static RecoverWithRoute recover(Node node, TxnId txnId, FullRoute<?> route, @Nullable Status witnessedByInvalidation, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, node.topology().forEpoch(route, txnId.epoch()), txnId, route, witnessedByInvalidation, callback);
    }

    public static RecoverWithRoute recover(Node node, Topologies topologies, TxnId txnId, FullRoute<?> route, @Nullable Status witnessedByInvalidation, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, topologies, null, txnId, route, witnessedByInvalidation, callback);
    }

    public static RecoverWithRoute recover(Node node, @Nullable Ballot promisedBallot, TxnId txnId, FullRoute<?> route, @Nullable Status witnessedByInvalidation, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, node.topology().forEpoch(route, txnId.epoch()), promisedBallot, txnId, route, witnessedByInvalidation, callback);
    }

    public static RecoverWithRoute recover(Node node, Topologies topologies, Ballot ballot, TxnId txnId, FullRoute<?> route, Status witnessedByInvalidation, BiConsumer<Outcome, Throwable> callback)
    {
        RecoverWithRoute recover = new RecoverWithRoute(node, topologies, ballot, txnId, route, witnessedByInvalidation, callback);
        recover.start();
        return recover;
    }

    private FullRoute<?> route()
    {
        return castToFullRoute(this.route);
    }

    @Override
    public void contact(Id to)
    {
        node.send(to, new CheckStatus(to, topologies(), txnId, route, sourceEpoch, IncludeInfo.All), this);
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        Ranges rangesForNode = topologies().forEpoch(txnId.epoch()).rangesForNode(from);
        PartialRoute<?> route = this.route.slice(rangesForNode);
        return isSufficient(route, ok);
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return isSufficient(route, merged);
    }

    protected boolean isSufficient(Route<?> route, CheckStatusOk ok)
    {
        CheckStatusOkFull full = (CheckStatusOkFull)ok;
        Known sufficientTo = full.sufficientFor(route, NoQuorum);
        if (!sufficientTo.isDefinitionKnown())
            return false;

        if (sufficientTo.outcome.isInvalidated())
            return true;

        Invariants.checkState(full.partialTxn.covers(route));
        return true;
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure != null)
        {
            callback.accept(null, failure);
            return;
        }

        CheckStatusOkFull merged = (CheckStatusOkFull) this.merged;
        Known known = merged.sufficientFor(route, success == Success.Quorum ? HasQuorum : NoQuorum);
        switch (known.outcome)
        {
            default: throw new AssertionError();
            case Unknown:
                if (known.definition.isKnown())
                {
                    Txn txn = merged.partialTxn.reconstitute(route);
                    Recover.recover(node, txnId, txn, route, callback);
                }
                else
                {
                    if (witnessedByInvalidation != null && witnessedByInvalidation.compareTo(Status.PreAccepted) > 0)
                        throw new IllegalStateException("We previously invalidated, finding a status that should be recoverable");
                    Invalidate.invalidate(node, txnId, route, witnessedByInvalidation != null, callback);
                }
                break;

            case TruncatedApply:
                if (!known.definition.isKnown() || !known.executeAt.hasDecidedExecuteAt() || !known.deps.hasDecidedDeps())
                {
                    callback.accept(DURABLE, null);
                    break;
                }

            case Applying:
            case Applied:
                Invariants.checkState(known.definition.isKnown());
                Invariants.checkState(known.executeAt.hasDecidedExecuteAt());
                // TODO (required): we might not be able to reconstitute Txn if we have GC'd on some shards
                Txn txn = merged.partialTxn.reconstitute(route);
                if (known.deps.hasDecidedDeps())
                {
                    Deps deps = merged.committedDeps.reconstitute(route());
                    node.withEpoch(merged.executeAt.epoch(), () -> {
                        Persist.persistMaximal(node, txnId, route(), txn, merged.executeAt, deps, merged.writes, merged.result);
                    });
                    callback.accept(APPLIED, null);
                }
                else
                {
                    Recover.recover(node, txnId, txn, route, callback);
                }
                break;

            case Invalidate:
                if (witnessedByInvalidation != null && witnessedByInvalidation.hasBeen(Status.PreCommitted))
                    throw new IllegalStateException("We previously invalidated, finding a status that should be recoverable");

                long untilEpoch = node.topology().epoch();
                commitInvalidate(node, txnId, route, untilEpoch);
                callback.accept(INVALIDATED, null);
                break;

            case Truncated:
                callback.accept(TRUNCATED, null);
                break;
        }
    }
}
