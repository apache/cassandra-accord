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

import accord.primitives.*;
import accord.local.PreLoadContext;
import accord.messages.TxnRequest.WithUnsynced;
import accord.local.Node.Id;
import accord.api.Key;
import accord.local.CommandStore;
import accord.topology.Topologies;
import accord.local.Node;
import accord.local.Command;
import accord.txn.Txn;
import com.google.common.annotations.VisibleForTesting;

import java.util.Collections;

import static accord.messages.PreAccept.calculateDeps;

public class Accept extends WithUnsynced
{
    public static class SerializerSupport
    {
        public static Accept create(Keys scope, long epoch, TxnId txnId, Ballot ballot, Key homeKey, Txn txn, Timestamp executeAt, Deps deps)
        {
            return new Accept(scope, epoch, txnId, ballot, homeKey, txn, executeAt, deps);
        }
    }

    public final Ballot ballot;
    public final Key homeKey;
    public final Txn txn;
    public final Timestamp executeAt;
    public final Deps deps;

    public Accept(Id to, Topologies topologies, Ballot ballot, TxnId txnId, Key homeKey, Txn txn, Timestamp executeAt, Deps deps)
    {
        super(to, topologies, txn.keys(), txnId);
        this.ballot = ballot;
        this.homeKey = homeKey;
        this.txn = txn;
        this.executeAt = executeAt;
        this.deps = deps;
    }

    protected Accept(Keys scope, long epoch, TxnId txnId, Ballot ballot, Key homeKey, Txn txn, Timestamp executeAt, Deps deps)
    {
        super(scope, epoch, txnId);
        this.ballot = ballot;
        this.homeKey = homeKey;
        this.txn = txn;
        this.executeAt = executeAt;
        this.deps = deps;
    }

    @VisibleForTesting
    public AcceptReply process(CommandStore instance, Key progressKey)
    {
        // TODO: when we begin expunging old epochs we need to ensure we handle the case where we do not fully handle the keys;
        //       since this will likely imply the transaction has been applied or aborted we can indicate the coordinator
        //       should enquire as to the result
        Command command = instance.command(txnId);
        if (!command.accept(ballot, txn, homeKey, progressKey, executeAt, deps))
            return new AcceptNack(txnId, command.promised());
        return new AcceptOk(txnId, calculateDeps(instance, txnId, txn, executeAt));
    }

    public void process(Node node, Node.Id replyToNode, ReplyContext replyContext)
    {
        Key progressKey = progressKey(node, homeKey);
        node.reply(replyToNode, replyContext, node.mapReduceLocal(this, minEpoch, executeAt.epoch, cs -> process(cs, progressKey),
        (r1, r2) -> {
            if (!r1.isOK()) return r1;
            if (!r2.isOK()) return r2;
            AcceptOk ok1 = (AcceptOk) r1;
            AcceptOk ok2 = (AcceptOk) r2;
            if (ok1.deps.isEmpty()) return ok2;
            if (ok2.deps.isEmpty()) return ok1;
            return new AcceptOk(txnId, ok1.deps.with(ok2.deps));
        }));
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Iterable<Key> keys()
    {
        return txn.keys();
    }

    @Override
    public MessageType type()
    {
        return MessageType.ACCEPT_REQ;
    }

    // TODO (now): can EpochRequest inherit TxnOperation?
    public static class Invalidate implements EpochRequest, PreLoadContext
    {
        public final Ballot ballot;
        public final TxnId txnId;
        public final Key someKey;

        public Invalidate(Ballot ballot, TxnId txnId, Key someKey)
        {
            this.ballot = ballot;
            this.txnId = txnId;
            this.someKey = someKey;
        }

        public void process(Node node, Node.Id replyToNode, ReplyContext replyContext)
        {
            node.reply(replyToNode, replyContext, node.ifLocal(this, someKey, txnId.epoch, instance -> {
                Command command = instance.command(txnId);
                if (!command.acceptInvalidate(ballot))
                    return new AcceptNack(txnId, command.promised());
                return new AcceptOk(txnId, null);
            }));
        }

        @Override
        public Iterable<TxnId> txnIds()
        {
            return Collections.singleton(txnId);
        }

        @Override
        public Iterable<Key> keys()
        {
            return Collections.emptyList();
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

    public interface AcceptReply extends Reply
    {
        @Override
        default MessageType type()
        {
            return MessageType.ACCEPT_RSP;
        }

        boolean isOK();
    }

    public static class AcceptOk implements AcceptReply
    {
        public final TxnId txnId;
        public final Deps deps;

        public AcceptOk(TxnId txnId, Deps deps)
        {
            this.txnId = txnId;
            this.deps = deps;
        }

        @Override
        public boolean isOK()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "AcceptOk{" +
                    "txnId=" + txnId +
                    ", deps=" + deps +
                    '}';
        }
    }

    public static class AcceptNack implements AcceptReply
    {
        public final TxnId txnId;
        public final Timestamp reject;

        public AcceptNack(TxnId txnId, Timestamp reject)
        {
            this.txnId = txnId;
            this.reject = reject;
        }

        @Override
        public boolean isOK()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "AcceptNack{" +
                    "txnId=" + txnId +
                    ", reject=" + reject +
                    '}';
        }
    }

    @Override
    public String toString()
    {
        return "Accept{" +
               "ballot: " + ballot +
               ", txnId: " + txnId +
               ", txn: " + txn +
               ", executeAt: " + executeAt +
               ", deps: " + deps +
               '}';
    }
}
