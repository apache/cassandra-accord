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

import accord.api.Key;
import accord.local.SafeCommandStore;
import accord.primitives.*;
import accord.local.Node.Id;
import accord.topology.Topologies;

import accord.api.RoutingKey;
import accord.local.Command.AcceptOutcome;
import accord.primitives.PartialDeps;
import accord.primitives.Route;
import accord.primitives.Txn;
import accord.primitives.Ballot;
import accord.local.Command;

import java.util.Collections;
import accord.primitives.Deps;
import accord.primitives.TxnId;

import static accord.local.Command.AcceptOutcome.RejectedBallot;
import static accord.local.Command.AcceptOutcome.Success;

import javax.annotation.Nullable;

import static accord.messages.PreAccept.calculatePartialDeps;

// TODO: use different objects for send and receive, so can be more efficient (e.g. serialize without slicing, and without unnecessary fields)
public class Accept extends TxnRequest.WithUnsynced<Accept.AcceptReply>
{
    public static class SerializerSupport
    {
        public static Accept create(TxnId txnId, PartialRoute scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, Ballot ballot, Timestamp executeAt, Keys keys, PartialDeps partialDeps, Txn.Kind kind)
        {
            return new Accept(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey, ballot, executeAt, keys, partialDeps, kind);
        }
    }

    public final Ballot ballot;
    public final Timestamp executeAt;
    public final Keys keys;
    public final PartialDeps partialDeps;
    public final Txn.Kind kind;

    public Accept(Id to, Topologies topologies, Ballot ballot, TxnId txnId, Route route, Timestamp executeAt, Keys keys, Deps deps, Txn.Kind kind)
    {
        super(to, topologies, txnId, route);
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.keys = keys.slice(scope.covering);
        this.partialDeps = deps.slice(scope.covering);
        this.kind = kind;
    }

    private Accept(TxnId txnId, PartialRoute scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey, Ballot ballot, Timestamp executeAt, Keys keys, PartialDeps partialDeps, Txn.Kind kind)
    {
        super(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey);
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.keys = keys;
        this.partialDeps = partialDeps;
        this.kind = kind;
    }

    @Override
    public synchronized AcceptReply apply(SafeCommandStore safeStore)
    {
        Command command = safeStore.command(txnId);
        switch (command.accept(safeStore, ballot, scope, progressKey, executeAt, partialDeps))
        {
            default: throw new IllegalStateException();
            case Redundant:
                return AcceptNack.REDUNDANT;
            case RejectedBallot:
                return new AcceptNack(RejectedBallot, command.promised());
            case Success:
                // TODO: we don't need to calculate deps if executeAt == txnId
                return new AcceptOk(calculatePartialDeps(safeStore, txnId, keys, kind, executeAt, safeStore.ranges().between(minEpoch, executeAt.epoch)));
        }
    }

    @Override
    public AcceptReply reduce(AcceptReply r1, AcceptReply r2)
    {
        if (!r1.isOk() || !r2.isOk())
            return r1.outcome().compareTo(r2.outcome()) >= 0 ? r1 : r2;

        AcceptOk ok1 = (AcceptOk) r1;
        AcceptOk ok2 = (AcceptOk) r2;
        PartialDeps deps = ok1.deps.with(ok2.deps);
        if (deps == ok1.deps) return ok1;
        if (deps == ok2.deps) return ok2;
        return new AcceptOk(deps);
    }

    @Override
    public void accept(AcceptReply reply, Throwable failure)
    {
        node.reply(replyTo, replyContext, reply);
    }

    public void process()
    {
        node.mapReduceConsumeLocal(this, minEpoch, executeAt.epoch, this);
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Iterable<Key> keys()
    {
        return keys;
    }

    @Override
    public MessageType type()
    {
        return MessageType.ACCEPT_REQ;
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

        public void process()
        {
            node.mapReduceConsumeLocal(this, someKey, txnId.epoch, this);
        }

        @Override
        public AcceptReply apply(SafeCommandStore safeStore)
        {
            Command command = safeStore.command(txnId);
            switch (command.acceptInvalidate(safeStore, ballot))
            {
                default:
                case Redundant:
                    return AcceptNack.REDUNDANT;
                case Success:
                    return new AcceptOk(null);
                case RejectedBallot:
                    return new AcceptNack(RejectedBallot, command.promised());
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
            return txnId.epoch;
        }
    }

    public static abstract class AcceptReply implements Reply
    {
        @Override
        public MessageType type()
        {
            return MessageType.ACCEPT_RSP;
        }

        public abstract boolean isOk();
        public abstract AcceptOutcome outcome();
    }

    public static class AcceptOk extends AcceptReply
    {
        @Nullable
        // TODO: migrate this to PartialDeps? Need to think carefully about semantics when ownership changes between txnId and executeAt
        public final PartialDeps deps;

        public AcceptOk(PartialDeps deps)
        {
            this.deps = deps;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public AcceptOutcome outcome()
        {
            return Success;
        }

        @Override
        public String toString()
        {
            return "AcceptOk{deps=" + deps + '}';
        }
    }

    public static class AcceptNack extends AcceptReply
    {
        public static final AcceptNack REDUNDANT = new AcceptNack(AcceptOutcome.Redundant, null);

        public final AcceptOutcome outcome;
        public final Ballot supersededBy;

        public AcceptNack(AcceptOutcome outcome, Ballot supersededBy)
        {
            this.outcome = outcome;
            this.supersededBy = supersededBy;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public AcceptOutcome outcome()
        {
            return outcome;
        }

        @Override
        public String toString()
        {
            return "AcceptNack{" + outcome + ",supersededBy=" + supersededBy + '}';
        }
    }

    public String toString() {
        return "Accept{" +
                "ballot: " + ballot +
                ", txnId: " + txnId +
                ", executeAt: " + executeAt +
                ", deps: " + partialDeps +
                '}';
    }
}
