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
import accord.api.Result;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.local.TxnOperation;
import accord.messages.BeginRecovery.RecoverNack;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.txn.Txn;
import accord.primitives.TxnId;
import accord.txn.Writes;

import java.util.Collections;

public class BeginInvalidate implements EpochRequest, TxnOperation
{
    public final Ballot ballot;
    public final TxnId txnId;
    public final Key someKey;

    public BeginInvalidate(TxnId txnId, Key someKey, Ballot ballot)
    {
        this.txnId = txnId;
        this.someKey = someKey;
        this.ballot = ballot;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        RecoverReply reply = node.ifLocal(this, someKey, txnId, instance -> {
            Command command = instance.command(txnId);

            if (!command.preAcceptInvalidate(ballot))
                return new InvalidateNack(command.promised(), command.txn(), command.homeKey());

            return new InvalidateOk(txnId, command.status(), command.accepted(), command.executeAt(), command.savedDeps(),
                                    command.writes(), command.result(), command.txn(), command.homeKey());
        });

        node.reply(replyToNode, replyContext, reply);
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
    public long waitForEpoch()
    {
        return txnId.epoch;
    }

    @Override
    public MessageType type()
    {
        return MessageType.BEGIN_INVALIDATE_REQ;
    }

    @Override
    public String toString()
    {
        return "BeginInvalidate{" +
               "txnId:" + txnId +
               ", ballot:" + ballot +
               '}';
    }

    public static class InvalidateOk extends RecoverOk
    {
        public final Txn txn;
        public final Key homeKey;

        public InvalidateOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, Deps deps, Writes writes, Result result, Txn txn, Key homeKey)
        {
            super(txnId, status, accepted, executeAt, deps, null, null, false, writes, result);
            this.txn = txn;
            this.homeKey = homeKey;
        }

        @Override
        public boolean isOK()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toString("InvalidateOk");
        }

        @Override
        public MessageType type()
        {
            return MessageType.BEGIN_INVALIDATE_RSP;
        }
    }

    public static class InvalidateNack extends RecoverNack
    {
        public final Txn txn;
        public final Key homeKey;
        public InvalidateNack(Ballot supersededBy, Txn txn, Key homeKey)
        {
            super(supersededBy);
            this.txn = txn;
            this.homeKey = homeKey;
        }

        @Override
        public boolean isOK()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "InvalidateNack{supersededBy:" + supersededBy + '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.BEGIN_INVALIDATE_RSP;
        }
    }
}
