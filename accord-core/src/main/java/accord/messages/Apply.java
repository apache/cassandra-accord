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

import javax.annotation.Nullable;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.messages.Apply.ApplyReply;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;

public class Apply extends TxnRequest<ApplyReply>
{
    public static final Factory FACTORY = Apply::new;
    public static class SerializationSupport
    {
        public static Apply create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Kind kind, Seekables<?, ?> keys, Timestamp executeAt, PartialDeps deps, PartialTxn txn, @Nullable FullRoute<?> fullRoute, Writes writes, Result result)
        {
            return new Apply(kind, txnId, scope, waitForEpoch, keys, executeAt, deps, txn, fullRoute, writes, result);
        }
    }

    public interface Factory
    {
        Apply create(Kind kind, Id to, Topologies participates, Topologies executes, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result);
    }

    public final Kind kind;
    public final Timestamp executeAt;
    public final Seekables<?, ?> keys;
    public final PartialDeps deps; // TODO (expected): this should be nullable, and only included if we did not send Commit (or if sending Maximal apply)
    public final @Nullable PartialTxn txn;
    public final @Nullable FullRoute<?> fullRoute;
    public final Writes writes;
    public final Result result;

    public enum Kind { Minimal, Maximal }

    protected Apply(Kind kind, Id to, Topologies participates, Topologies executes, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(to, participates, route, txnId);
        Invariants.checkState(txnId.kind() != Txn.Kind.Write || writes != null);
        Ranges slice = kind == Kind.Maximal || executes == participates ? scope.covering() : executes.computeRangesForNode(to);
        // TODO (desired): it's wasteful to encode the full set of ranges owned by the recipient node;
        //     often it will be cheaper to include the FullRoute for Deps scope (or come up with some other safety-preserving encoding scheme)
        this.kind = kind;
        this.deps = deps.slice(slice);
        this.keys = txn.keys().slice(slice);
        this.txn = kind == Kind.Maximal ? txn.slice(slice, true) : null;
        this.fullRoute = kind == Kind.Maximal ? route : null;
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    public static void sendMaximal(Node node, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Topologies executes = executes(node, route, executeAt);
        Topologies participates = participates(node, route, txnId, executeAt, executes);
        node.send(participates.nodes(), to -> applyMaximal(FACTORY, to, participates, executes, txnId, route, txn, executeAt, deps, writes, result));
    }

    public static void sendMaximal(Node node, Id to, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Topologies executes = executes(node, route, executeAt);
        Topologies participates = participates(node, route, txnId, executeAt, executes);
        node.send(to, applyMaximal(FACTORY, to, participates, executes, txnId, route, txn, executeAt, deps, writes, result));
    }

    public static Topologies executes(Node node, Unseekables<?> route, Timestamp executeAt)
    {
        return node.topology().forEpoch(route, executeAt.epoch());
    }

    public static Topologies participates(Node node, Unseekables<?> route, TxnId txnId, Timestamp executeAt, Topologies executes)
    {
        return txnId.epoch() == executeAt.epoch() ? executes : node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
    }

    public static Apply applyMinimal(Factory factory, Id to, Topologies sendTo, Topologies applyTo, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps stableDeps, Writes writes, Result result)
    {
        return factory.create(Kind.Minimal, to, sendTo, applyTo, txnId, route, txn, executeAt, stableDeps, writes, result);
    }

    public static Apply applyMaximal(Factory factory, Id to, Topologies participates, Topologies executes, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps stableDeps, Writes writes, Result result)
    {
        return factory.create(Kind.Maximal, to, participates, executes, txnId, route, txn, executeAt, stableDeps, writes, result);
    }

    protected Apply(Kind kind, TxnId txnId, PartialRoute<?> route, long waitForEpoch, Seekables<?, ?> keys, Timestamp executeAt, PartialDeps deps, @Nullable PartialTxn txn, @Nullable FullRoute<?> fullRoute, Writes writes, Result result)
    {
        super(txnId, route, waitForEpoch);
        this.kind = kind;
        this.executeAt = executeAt;
        this.deps = deps;
        this.keys = keys;
        this.txn = txn;
        this.fullRoute = fullRoute;
        this.writes = writes;
        this.result = result;
    }

    @Override
    public void process()
    {
        // note, we do not also commit here if txnId.epoch != executeAt.epoch, as the scope() for a commit would be different
        node.mapReduceConsumeLocal(this, txnId.epoch(), executeAt.epoch(), this);
    }


    @Override
    public ApplyReply apply(SafeCommandStore safeStore)
    {
        return apply(safeStore, txn, txnId, executeAt, deps, fullRoute != null ? fullRoute : scope, writes, result, progressKey);
    }

    public static ApplyReply apply(SafeCommandStore safeStore, PartialTxn txn, TxnId txnId, Timestamp executeAt, PartialDeps deps, Route<?> route, Writes writes, Result result, RoutingKey progressKey)
    {
        SafeCommand safeCommand = safeStore.get(txnId, executeAt, route);
        return apply(safeStore, safeCommand, txn, txnId, executeAt, deps, route, writes, result, progressKey);
    }

    public static ApplyReply apply(SafeCommandStore safeStore, SafeCommand safeCommand, PartialTxn txn, TxnId txnId, Timestamp executeAt, PartialDeps deps, Route<?> route, Writes writes, Result result, RoutingKey progressKey)
    {
        switch (Commands.apply(safeStore, safeCommand, txnId, route, progressKey, executeAt, deps, txn, writes, result))
        {
            default:
            case Insufficient:
                return ApplyReply.Insufficient;
            case Redundant:
                return ApplyReply.Redundant;
            case Success:
                return ApplyReply.Applied;
        }
    }

    @Override
    public ApplyReply reduce(ApplyReply a, ApplyReply b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    @Override
    public void accept(ApplyReply reply, Throwable failure)
    {
        node.reply(replyTo, replyContext, reply, failure);
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return keys;
    }

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.COMMANDS;
    }

    @Override
    public MessageType type()
    {
        switch (kind)
        {
            case Minimal: return MessageType.APPLY_MINIMAL_REQ;
            case Maximal: return MessageType.APPLY_MAXIMAL_REQ;
            default: throw new IllegalStateException();
        }
    }

    public enum ApplyReply implements Reply
    {
        Applied, Redundant, Insufficient;

        @Override
        public MessageType type()
        {
            return MessageType.APPLY_RSP;
        }

        @Override
        public String toString()
        {
            return "Apply" + name();
        }

        @Override
        public boolean isFinal()
        {
            return this != Insufficient;
        }
    }

    @Override
    public String toString()
    {
        return "Apply{kind:" + kind +
               ", txnId:" + txnId +
               ", deps:" + deps +
               ", executeAt:" + executeAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }
}
