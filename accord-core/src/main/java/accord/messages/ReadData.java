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

import java.util.Collections;
import java.util.Set;

import accord.primitives.Deps;
import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.local.*;
import accord.local.Node.Id;
import accord.api.Data;
import accord.topology.Topologies;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.txn.Txn;
import accord.primitives.TxnId;
import accord.utils.DeterministicIdentitySet;
import com.google.common.collect.Iterables;

public class ReadData extends TxnRequest
{
    static class LocalRead implements Listener, TxnOperation
    {
        final TxnId txnId;
        final Deps deps;
        final Node node;
        final Node.Id replyToNode;
        final Keys readKeys;
        final Keys txnKeys;
        final ReplyContext replyContext;

        Data data;
        boolean isObsolete; // TODO: respond with the Executed result we have stored?
        Set<CommandStore> waitingOn;

        LocalRead(TxnId txnId, Deps deps, Node node, Id replyToNode, Keys readKeys, Keys txnKeys, ReplyContext replyContext)
        {
            Preconditions.checkArgument(!readKeys.isEmpty());
            this.txnId = txnId;
            this.deps = deps;
            this.node = node;
            this.replyToNode = replyToNode;
            this.readKeys = readKeys;
            this.txnKeys = txnKeys;  // TODO (now): is this needed? Does the read update commands per key?
            this.replyContext = replyContext;
        }

        @Override
        public Iterable<TxnId> txnIds()
        {
            return Iterables.concat(Collections.singleton(txnId), deps.txnIds());
        }

        @Override
        public Iterable<Key> keys()
        {
            return txnKeys;
        }

        @Override
        public synchronized void onChange(Command command)
        {
            switch (command.status())
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                case Committed:
                    return;

                case Executed:
                case Applied:
                case Invalidated:
                    obsolete();
                case ReadyToExecute:
            }

            command.removeListener(this);
            if (!isObsolete)
                read(command);
        }

        @Override
        public boolean isTransient()
        {
            return true;
        }

        private synchronized void readComplete(CommandStore commandStore, Data result)
        {
            data = data == null ? result : data.merge(result);

            waitingOn.remove(commandStore);
            if (waitingOn.isEmpty())
                node.reply(replyToNode, replyContext, new ReadOk(data));
        }

        private void read(Command command)
        {
            command.read(readKeys).addCallback((next, throwable) -> {
                if (throwable != null)
                    node.reply(replyToNode, replyContext, new ReadNack());
                else
                    readComplete(command.commandStore(), next);
            });
        }

        void obsolete()
        {
            if (!isObsolete)
            {
                isObsolete = true;
                node.reply(replyToNode, replyContext, new ReadNack());
            }
        }

        synchronized void setup(TxnId txnId, Txn txn, Key homeKey, Keys keys, Timestamp executeAt)
        {
            Key progressKey = node.trySelectProgressKey(txnId, txn.keys(), homeKey);
            waitingOn = node.collectLocal(keys, executeAt, DeterministicIdentitySet::new);
            CommandStores.forEachNonBlocking(waitingOn, this, instance -> {
                Command command = instance.command(txnId);
                command.preaccept(txn, homeKey, progressKey); // ensure pre-accepted
                switch (command.status())
                {
                    default:
                    case NotWitnessed:
                        throw new IllegalStateException();

                    case PreAccepted:
                    case Accepted:
                    case AcceptedInvalidate:
                    case Committed:
                        command.addListener(this);
                        break;

                    case Executed:
                    case Applied:
                    case Invalidated:
                        obsolete();
                        break;

                    case ReadyToExecute:
                        if (!isObsolete)
                            read(command);
                }
            });
        }
    }

    public final TxnId txnId;
    public final Txn txn;
    public final Deps deps;
    public final Key homeKey;
    public final Timestamp executeAt;

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Txn txn, Deps deps, Key homeKey, Timestamp executeAt)
    {
        super(to, topologies, txn.keys());
        this.txnId = txnId;
        this.txn = txn;
        this.deps = deps;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
    }

    public void process(Node node, Node.Id from, ReplyContext replyContext)
    {
        new LocalRead(txnId, deps, node, from, txn.read().keys().intersect(scope()), txn.keys(), replyContext)
            .setup(txnId, txn, homeKey, scope(), executeAt);
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Iterables.concat(Collections.singleton(txnId), deps.txnIds());
    }

    @Override
    public Iterable<Key> keys()
    {
        return Collections.emptyList();
    }

    @Override
    public MessageType type()
    {
        return MessageType.READ_REQ;
    }

    public static class ReadReply implements Reply
    {
        @Override
        public MessageType type()
        {
            return MessageType.READ_RSP;
        }

        public boolean isOK()
        {
            return true;
        }
    }

    public static class ReadNack extends ReadReply
    {
        @Override
        public boolean isOK()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "ReadNack";
        }
    }

    public static class ReadOk extends ReadReply
    {
        public final Data data;
        public ReadOk(Data data)
        {
            this.data = data;
        }

        @Override
        public String toString()
        {
            return "ReadOk{" + data + '}';
        }
    }

    @Override
    public String toString()
    {
        return "ReadData{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               '}';
    }
}
