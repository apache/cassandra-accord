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
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.Status;
import accord.local.Status.Durability;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.local.PreLoadContext.contextFor;

public class InformHomeDurable implements Request
{
    public final TxnId txnId;
    // we send a partial route covering this node, because we otherwise can't know if the command may have been truncated;
    // since the homeKey isn't something we truncate on (as it's not
    public final Route<?> route;
    public final Timestamp executeAt;
    public final Durability durability;

    public InformHomeDurable(TxnId txnId, Route<?> route, Timestamp executeAt, Durability durability)
    {
        this.txnId = txnId;
        this.route = route;
        this.executeAt = executeAt;
        this.durability = durability;
    }

    @Override
    public void preProcess(Node node, Id replyToNode, ReplyContext replyContext)
    {
        // no-op
    }

    @Override
    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        // TODO (expected, efficiency): do not load txnId first
        node.ifLocal(contextFor(txnId), route.homeKey(), txnId.epoch(), safeStore -> {
            SafeCommand safeCommand = safeStore.get(txnId, txnId, route);
            if (safeCommand.current().is(Status.Truncated))
                return;

            Commands.setDurability(safeStore, safeCommand, durability, route, executeAt);
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
