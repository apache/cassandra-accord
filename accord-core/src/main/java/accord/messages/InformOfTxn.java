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
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.TxnOperation;
import accord.txn.Txn;
import accord.primitives.TxnId;

import java.util.Collections;

import static accord.messages.InformOfTxn.InformOfTxnNack.nack;
import static accord.messages.InformOfTxn.InformOfTxnOk.ok;

public class InformOfTxn implements EpochRequest, TxnOperation
{
    final TxnId txnId;
    final Key homeKey;
    final Txn txn;

    public InformOfTxn(TxnId txnId, Key homeKey, Txn txn)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.txn = txn;
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

    // TODO (now): audit all messages to ensure requests passed to command store
    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Key progressKey = node.selectProgressKey(txnId, txn.keys(), homeKey);
        Reply reply = node.ifLocal(this, homeKey, txnId, instance -> {
            instance.command(txnId).preaccept(txn, homeKey, progressKey);
            return ok();
        });

        if (reply == null)
            reply = nack();

        node.reply(replyToNode, replyContext, reply);
    }

    @Override
    public String toString()
    {
        return "InformOfTxn{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               '}';
    }

    public interface InformOfTxnReply extends Reply
    {
        boolean isOk();
    }

    public static class InformOfTxnOk implements InformOfTxnReply
    {
        private static final InformOfTxnOk instance = new InformOfTxnOk();

        @Override
        public MessageType type()
        {
            return MessageType.INFORM_RSP;
        }

        static InformOfTxnReply ok()
        {
            return instance;
        }

        private InformOfTxnOk() { }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "InformOfTxnOk";
        }
    }

    public static class InformOfTxnNack implements InformOfTxnReply
    {
        private static final InformOfTxnNack instance = new InformOfTxnNack();

        @Override
        public MessageType type()
        {
            return MessageType.INFORM_RSP;
        }

        static InformOfTxnReply nack()
        {
            return instance;
        }

        private InformOfTxnNack() { }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "InformOfTxnNack";
        }
    }

    @Override
    public MessageType type()
    {
        return MessageType.INFORM_REQ;
    }

    @Override
    public long waitForEpoch()
    {
        return txnId.epoch;
    }
}
