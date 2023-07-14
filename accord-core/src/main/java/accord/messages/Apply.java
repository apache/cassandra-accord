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

import accord.local.*;
import accord.primitives.*;
import accord.local.Node.Id;
import accord.api.Result;
import accord.topology.Topologies;

import accord.messages.Apply.ApplyReply;

import static accord.messages.MessageType.APPLY_REQ;
import static accord.messages.MessageType.APPLY_RSP;

public class Apply extends TxnRequest<ApplyReply>
{
    public static class SerializationSupport
    {
        public static Apply create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Seekables<?, ?> keys, Timestamp executeAt, PartialDeps deps, PartialTxn txn, Writes writes, Result result)
        {
            return new Apply(txnId, scope, waitForEpoch, keys, executeAt, deps, txn, writes, result);
        }
    }

    public final Timestamp executeAt;
    public final Seekables<?, ?> keys;
    public final PartialDeps deps; // TODO (expected): this should be nullable, and only included if we did not send Commit (or if sending Maximal apply)
    public final @Nullable PartialTxn txn;
    public final Writes writes;
    public final Result result;

    private Apply(Id to, Topologies participates, Topologies executes, TxnId txnId, Route<?> route, Txn txn, Timestamp executeAt, Deps deps, boolean isMaximal, Writes writes, Result result)
    {
        super(to, participates, route, txnId);
        Ranges slice = isMaximal || executes == participates ? scope.covering() : executes.computeRangesForNode(to);
        // TODO (desired): it's wasteful to encode the full set of ranges owned by the recipient node;
        //     often it will be cheaper to include the FullRoute for Deps scope (or come up with some other safety-preserving encoding scheme)
        this.deps = deps.slice(slice);
        this.keys = txn.keys().slice(slice);
        this.txn = isMaximal ? txn.slice(slice, true) : null;
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    public static void sendMaximal(Node node, TxnId txnId, Route<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Topologies executes = executes(node, route, executeAt);
        Topologies participates = participates(node, route, txnId, executeAt, executes);
        node.send(participates.nodes(), to -> applyMaximal(to, participates, executes, txnId, route, txn, executeAt, deps, writes, result));
    }

    public static void sendMaximal(Node node, Id to, TxnId txnId, Route<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Topologies executes = executes(node, route, executeAt);
        Topologies participates = participates(node, route, txnId, executeAt, executes);
        node.send(to, applyMaximal(to, participates, executes, txnId, route, txn, executeAt, deps, writes, result));
    }

    public static Topologies executes(Node node, Route<?> route, Timestamp executeAt)
    {
        return node.topology().forEpoch(route, executeAt.epoch());
    }

    public static Topologies participates(Node node, Route<?> route, TxnId txnId, Timestamp executeAt, Topologies executes)
    {
        return txnId.epoch() == executeAt.epoch() ? executes : node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
    }

    public static Apply applyMinimal(Id to, Topologies sendTo, Topologies applyTo, TxnId txnId, Route<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        return new Apply(to, sendTo, applyTo, txnId, route, txn, executeAt, deps, false, writes, result);
    }

    public static Apply applyMaximal(Id to, Topologies participates, Topologies executes, TxnId txnId, Route<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        return new Apply(to, participates, executes, txnId, route, txn, executeAt, deps, true, writes, result);
    }

    private Apply(TxnId txnId, PartialRoute<?> route, long waitForEpoch, Seekables<?, ?> keys, Timestamp executeAt, PartialDeps deps, @Nullable PartialTxn txn, Writes writes, Result result)
    {
        super(txnId, route, waitForEpoch);
        this.executeAt = executeAt;
        this.deps = deps;
        this.keys = keys;
        this.txn = txn;
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
        SafeCommand safeCommand = safeStore.get(txnId, executeAt, scope);
        switch (Commands.apply(safeStore, safeCommand, txnId, scope, progressKey, executeAt, deps, txn, writes, result))
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
        node.reply(replyTo, replyContext, reply);
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        if (txn == null) return Keys.EMPTY;
        return txn.keys();
    }

    @Override
    public MessageType type()
    {
        return APPLY_REQ;
    }

    public enum ApplyReply implements Reply
    {
        Applied, Redundant, Insufficient;

        @Override
        public MessageType type()
        {
            return APPLY_RSP;
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
        return "Apply{" +
               "txnId:" + txnId +
               ", deps:" + deps +
               ", executeAt:" + executeAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }
}
