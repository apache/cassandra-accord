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

import accord.local.Commands;
import accord.local.Commands.AcceptOutcome;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.*;
import accord.local.Node.Id;
import accord.topology.Topologies;

import accord.api.RoutingKey;
import accord.primitives.PartialDeps;
import accord.primitives.FullRoute;
import accord.primitives.Ballot;

import java.util.Collections;
import accord.primitives.Deps;
import accord.primitives.TxnId;

import javax.annotation.Nonnull;

import javax.annotation.Nullable;

import static accord.local.Commands.AcceptOutcome.*;

// TODO (low priority, efficiency): use different objects for send and receive, so can be more efficient
//                                  (e.g. serialize without slicing, and without unnecessary fields)
public class Accept extends TxnRequest.WithUnsynced<Accept.AcceptReply>
{
    public static class SerializerSupport
    {
        public static Accept create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, Ballot ballot, Timestamp executeAt, Seekables<?, ?> keys, PartialDeps partialDeps)
        {
            return new Accept(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey, ballot, executeAt, keys, partialDeps);
        }
    }

    public final Ballot ballot;
    public final Timestamp executeAt;
    public final Seekables<?, ?> keys;
    public final PartialDeps partialDeps;

    public Accept(Id to, Topologies topologies, Ballot ballot, TxnId txnId, FullRoute<?> route, Timestamp executeAt, Seekables<?, ?> keys, Deps deps)
    {
        super(to, topologies, txnId, route);
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.keys = keys.slice(scope.covering());
        this.partialDeps = deps.slice(scope.covering());
    }

    private Accept(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, Ballot ballot, Timestamp executeAt, Seekables<?, ?> keys, PartialDeps partialDeps)
    {
        super(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey);
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.keys = keys;
        this.partialDeps = partialDeps;
    }

    @Override
    public synchronized AcceptReply apply(SafeCommandStore safeStore)
    {
        if (minUnsyncedEpoch < txnId.epoch())
        {
            // if we include unsync'd epochs, check if we intersect the ranges for coordination or execution;
            // if not, we're just providing dependencies, and we can do that without updating our state
            Ranges acceptRanges = safeStore.ranges().between(txnId.epoch(), executeAt.epoch());
            if (!acceptRanges.intersects(scope))
                return new AcceptReply(calculatePartialDeps(safeStore));
        }

        // only accept if we actually participate in the ranges - otherwise we're just looking
        switch (Commands.accept(safeStore, txnId, ballot, scope, keys, progressKey, executeAt, partialDeps))
        {
            default: throw new IllegalStateException();
            case Redundant:
                return AcceptReply.REDUNDANT;
            case RejectedBallot:
                return new AcceptReply(safeStore.command(txnId).current().promised());
            case Success:
                // TODO (desirable, efficiency): we don't need to calculate deps if executeAt == txnId
                return new AcceptReply(calculatePartialDeps(safeStore));
        }
    }

    private PartialDeps calculatePartialDeps(SafeCommandStore safeStore)
    {
        return PreAccept.calculatePartialDeps(safeStore, txnId, keys, executeAt, safeStore.ranges().between(minUnsyncedEpoch, executeAt.epoch()));
    }

    @Override
    public AcceptReply reduce(AcceptReply ok1, AcceptReply ok2)
    {
        if (!ok1.isOk() || !ok2.isOk())
            return ok1.outcome().compareTo(ok2.outcome()) >= 0 ? ok1 : ok2;

        PartialDeps deps = ok1.deps.with(ok2.deps);
        if (deps == ok1.deps) return ok1;
        if (deps == ok2.deps) return ok2;
        return new AcceptReply(deps);
    }

    @Override
    public void accept(AcceptReply reply, Throwable failure)
    {
        node.reply(replyTo, replyContext, reply);
    }

    @Override
    public void process()
    {
        node.mapReduceConsumeLocal(this, minUnsyncedEpoch, executeAt.epoch(), this);
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return keys;
    }

    @Override
    public MessageType type()
    {
        return MessageType.ACCEPT_REQ;
    }

    public String toString() {
        return "Accept{" +
                "ballot: " + ballot +
                ", txnId: " + txnId +
                ", executeAt: " + executeAt +
                ", deps: " + partialDeps +
                '}';
    }

    public static final class AcceptReply implements Reply
    {
        public static final AcceptReply ACCEPT_INVALIDATE = new AcceptReply(Success);
        public static final AcceptReply REDUNDANT = new AcceptReply(Redundant);

        public final AcceptOutcome outcome;
        public final Ballot supersededBy;
        public final @Nullable PartialDeps deps;

        private AcceptReply(AcceptOutcome outcome)
        {
            this.outcome = outcome;
            this.supersededBy = null;
            this.deps = null;
        }

        public AcceptReply(Ballot supersededBy)
        {
            this.outcome = RejectedBallot;
            this.supersededBy = supersededBy;
            this.deps = null;
        }

        public AcceptReply(@Nonnull PartialDeps deps)
        {
            this.outcome = Success;
            this.supersededBy = null;
            this.deps = deps;
        }

        @Override
        public MessageType type()
        {
            return MessageType.ACCEPT_RSP;
        }

        public boolean isOk()
        {
            return outcome == Success;
        }

        public AcceptOutcome outcome()
        {
            return outcome;
        }

        @Override
        public String toString()
        {
            switch (outcome)
            {
                default: throw new AssertionError();
                case Success:
                    return "AcceptOk{deps=" + deps + '}';
                case Redundant:
                    return "AcceptNack()";
                case RejectedBallot:
                    return "AcceptNack(" + supersededBy + ")";
            }
        }
    }

    public static class Invalidate extends AbstractEpochRequest<AcceptReply>
    {
        public final Ballot ballot;
        public final RoutingKey someKey;

        public Invalidate(Ballot ballot, TxnId txnId, RoutingKey someKey)
        {
            super(txnId);
            this.ballot = ballot;
            this.someKey = someKey;
        }

        @Override
        public void process()
        {
            node.mapReduceConsumeLocal(this, someKey, txnId.epoch(), this);
        }

        @Override
        public AcceptReply apply(SafeCommandStore safeStore)
        {
            SafeCommand safeCommand = safeStore.command(txnId);
            switch (Commands.acceptInvalidate(safeStore, safeCommand, ballot))
            {
                default:
                case Redundant:
                    return AcceptReply.REDUNDANT;
                case Success:
                    return AcceptReply.ACCEPT_INVALIDATE;
                case RejectedBallot:
                    return new AcceptReply(safeCommand.current().promised());
            }
        }

        @Override
        public MessageType type()
        {
            return MessageType.ACCEPT_INVALIDATE_REQ;
        }

        @Override
        public String toString()
        {
            return "AcceptInvalidate{ballot:" + ballot + ", txnId:" + txnId + ", key:" + someKey + '}';
        }

        @Override
        public long waitForEpoch()
        {
            return txnId.epoch();
        }
    }
}
