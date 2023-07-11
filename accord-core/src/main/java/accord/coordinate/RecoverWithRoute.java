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

import accord.coordinate.FetchData.OnDone;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.local.Status.Known;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialRoute;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import javax.annotation.Nullable;

import static accord.local.Status.KnownExecuteAt.ExecuteAtKnown;
import static accord.local.Status.Outcome.Apply;
import static accord.messages.CheckStatus.WithQuorum.HasQuorum;
import static accord.messages.CheckStatus.WithQuorum.NoQuorum;
import static accord.primitives.ProgressToken.APPLIED;
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
        Known sufficientTo = full.sufficientFor(route.participants(), NoQuorum);
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

        CheckStatusOkFull full = (CheckStatusOkFull) this.merged;
        Known known = full.sufficientFor(route.participants(), success == Success.Quorum ? HasQuorum : NoQuorum);

        switch (known.outcome)
        {
            default: throw new AssertionError();
            case Unknown:
                if (known.definition.isKnown())
                {
                    Txn txn = full.partialTxn.reconstitute(route);
                    Recover.recover(node, txnId, txn, route, callback);
                }
                else
                {
                    if (witnessedByInvalidation != null && witnessedByInvalidation.compareTo(Status.PreAccepted) > 0)
                        throw new IllegalStateException("We previously invalidated, finding a status that should be recoverable");
                    Invalidate.invalidate(node, txnId, route, witnessedByInvalidation != null, callback);
                }
                break;

            case WasApply:
            case Apply:
                if (!known.isDefinitionKnown() || known.executeAt != ExecuteAtKnown || known.outcome != Apply)
                {
                    OnDone.propagate(node, txnId, sourceEpoch, success.withQuorum, route, null, full, (s, f) -> callback.accept(f == null ? full.toProgressToken() : null, f));
                    break;
                }

                // TODO (required): might not be able to fully recover transaction - may only have enough for local shard
                Txn txn = full.partialTxn.reconstitute(route);
                if (known.deps.hasDecidedDeps())
                {
                    Deps deps = full.committedDeps.reconstitute(route());
                    node.withEpoch(full.executeAt.epoch(), () -> {
                        Persist.persistMaximal(node, txnId, route(), txn, full.executeAt, deps, full.writes, full.result);
                    });
                    callback.accept(APPLIED, null);
                }
                else
                {
                    Recover.recover(node, txnId, txn, route, callback);
                }
                break;

            case Invalidated:
                if (witnessedByInvalidation != null && witnessedByInvalidation.hasBeen(Status.PreCommitted))
                    throw new IllegalStateException("We previously invalidated, finding a status that should be recoverable");

                OnDone.propagate(node, txnId, sourceEpoch, success.withQuorum, route, null, full, (s, f) -> callback.accept(f == null ? INVALIDATED : null, f));
                break;

            case Erased:
                OnDone.propagate(node, txnId, sourceEpoch, success.withQuorum, route, null, full, (s, f) -> callback.accept(f == null ? TRUNCATED : null, f));
                break;
        }
    }
}
