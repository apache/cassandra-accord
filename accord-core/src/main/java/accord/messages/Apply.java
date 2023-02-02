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

import accord.local.SafeCommandStore;
import accord.local.*;
import accord.primitives.*;
import accord.local.Node.Id;
import accord.api.Result;
import accord.topology.Topologies;
import com.google.common.collect.Iterables;

import java.util.Collections;
import accord.messages.Apply.ApplyReply;

import static accord.local.PreLoadContext.empty;
import static accord.messages.MessageType.APPLY_REQ;
import static accord.messages.MessageType.APPLY_RSP;

public class Apply extends TxnRequest<ApplyReply>
{
    public static class SerializationSupport
    {
        public static Apply create(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long untilEpoch, Seekables<?, ?> keys, Timestamp executeAt, PartialDeps deps, Writes writes, Result result)
        {
            return new Apply(txnId, scope, waitForEpoch, untilEpoch, keys, executeAt, deps, writes, result);
        }
    }

    public final long untilEpoch;
    public final Timestamp executeAt;
    public final PartialDeps deps;
    public final Seekables<?, ?> keys;
    public final Writes writes;
    public final Result result;

    public Apply(Id to, Topologies sendTo, Topologies applyTo, long untilEpoch, TxnId txnId, Route<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(to, sendTo, route, txnId);
        this.untilEpoch = untilEpoch;
        Ranges slice = applyTo == sendTo ? scope.covering() : applyTo.computeRangesForNode(to);

        this.deps = deps.slice(slice);
        this.keys = txn.keys().slice(slice);
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    private Apply(TxnId txnId, PartialRoute<?> route, long waitForEpoch, long untilEpoch, Seekables<?, ?> keys, Timestamp executeAt, PartialDeps deps, Writes writes, Result result)
    {
        super(txnId, route, waitForEpoch);
        this.untilEpoch = untilEpoch;
        this.executeAt = executeAt;
        this.deps = deps;
        this.keys = keys;
        this.writes = writes;
        this.result = result;
    }

    @Override
    public void process()
    {
        // note, we do not also commit here if txnId.epoch != executeAt.epoch, as the scope() for a commit would be different
        node.mapReduceConsumeLocal(this, txnId.epoch(), untilEpoch, this);
    }

    @Override
    public ApplyReply apply(SafeCommandStore safeStore)
    {
        switch (Commands.apply(safeStore, txnId, untilEpoch, scope, executeAt, deps, writes, result))
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
        if (reply == ApplyReply.Applied)
        {
            node.ifLocal(empty(), scope.homeKey(), txnId.epoch(), instance -> {
                node.withEpoch(executeAt.epoch(), () -> instance.progressLog().durableLocal(txnId));
            }).begin(node.agent());
        }
        node.reply(replyTo, replyContext, reply);
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Iterables.concat(Collections.singleton(txnId), deps.txnIds());
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return keys;
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
