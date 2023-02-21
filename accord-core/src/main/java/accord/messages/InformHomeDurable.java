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

import java.util.Set;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.local.PreLoadContext.contextFor;

public class InformHomeDurable implements Request
{
    public final TxnId txnId;
    public final RoutingKey homeKey;
    public final Timestamp executeAt;
    public final Durability durability;
    public final Set<Id> persistedOn;

    public InformHomeDurable(TxnId txnId, RoutingKey homeKey, Timestamp executeAt, Durability durability, Set<Id> persistedOn)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
        this.durability = durability;
        this.persistedOn = persistedOn;
    }

    @Override
    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        // TODO (expected, efficiency): do not load txnId first
        node.ifLocal(contextFor(txnId), homeKey, txnId.epoch(), safeStore -> {
            Command command = safeStore.command(txnId);
            command.setDurability(safeStore, durability, homeKey, executeAt);
            safeStore.progressLog().durable(command, persistedOn);
        }).begin(node.agent());
    }

    @Override
    public String toString()
    {
        return "InformHomeDurable{txnId:" + txnId + '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.INFORM_HOME_DURABLE_REQ;
    }
}
