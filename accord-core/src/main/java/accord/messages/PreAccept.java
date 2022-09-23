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
import java.util.Objects;
import java.util.function.Consumer;

import accord.local.*;
import accord.utils.VisibleForImplementation;
import com.google.common.annotations.VisibleForTesting;

import accord.api.Key;
import accord.local.Node.Id;
import accord.messages.TxnRequest.WithUnsynced;
import accord.topology.Topologies;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.Deps;
import accord.txn.Txn;
import accord.primitives.TxnId;

public class PreAccept extends WithUnsynced
{
    public final Key homeKey;
    public final Txn txn;
    public final long maxEpoch;

    public PreAccept(Id to, Topologies topologies, TxnId txnId, Txn txn, Key homeKey)
    {
        super(to, topologies, txn.keys(), txnId);
        this.homeKey = homeKey;
        this.txn = txn;
        this.maxEpoch = topologies.currentEpoch();
    }

    @VisibleForTesting
    @VisibleForImplementation
    public PreAccept(Keys scope, long epoch, TxnId txnId, Txn txn, Key homeKey)
    {
        super(scope, epoch, txnId);
        this.homeKey = homeKey;
        this.txn = txn;
        this.maxEpoch = epoch;
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

    @VisibleForTesting
    public PreAcceptReply process(CommandStore instance, Key progressKey)
    {
        // note: this diverges from the paper, in that instead of waiting for JoinShard,
        //       we PreAccept to both old and new topologies and require quorums in both.
        //       This necessitates sending to ALL replicas of old topology, not only electorate (as fast path may be unreachable).
        Command command = instance.command(txnId);
        if (!command.preaccept(txn, homeKey, progressKey))
            return PreAcceptNack.INSTANCE;
        return new PreAcceptOk(txnId, command.executeAt(), calculateDeps(instance, txnId, txn, txnId));
    }

    public void process(Node node, Id from, ReplyContext replyContext)
    {
        // TODO: verify we handle all of the scope() keys
        Key progressKey = progressKey(node, homeKey);
        node.reply(from, replyContext, node.mapReduceLocal(this, minEpoch, maxEpoch, cs -> process(cs, progressKey),
        (r1, r2) -> {
            if (!r1.isOK()) return r1;
            if (!r2.isOK()) return r2;
            PreAcceptOk ok1 = (PreAcceptOk) r1;
            PreAcceptOk ok2 = (PreAcceptOk) r2;
            PreAcceptOk okMax = ok1.witnessedAt.compareTo(ok2.witnessedAt) >= 0 ? ok1 : ok2;
            Deps deps = ok1.deps.with(ok2.deps);
            if (deps == okMax.deps)
                return okMax;
            return new PreAcceptOk(txnId, okMax.witnessedAt, deps);
        }));
    }

    @Override
    public MessageType type()
    {
        return MessageType.PREACCEPT_REQ;
    }

    public interface PreAcceptReply extends Reply
    {
        @Override
        default MessageType type()
        {
            return MessageType.PREACCEPT_RSP;
        }

        boolean isOK();
    }

    public static class PreAcceptOk implements PreAcceptReply
    {
        public final TxnId txnId;
        public final Timestamp witnessedAt;
        public final Deps deps;

        public PreAcceptOk(TxnId txnId, Timestamp witnessedAt, Deps deps)
        {
            this.txnId = txnId;
            this.witnessedAt = witnessedAt;
            this.deps = deps;
        }

        @Override
        public boolean isOK()
        {
            return true;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PreAcceptOk that = (PreAcceptOk) o;
            return witnessedAt.equals(that.witnessedAt) && deps.equals(that.deps);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(witnessedAt, deps);
        }

        @Override
        public String toString()
        {
            return "PreAcceptOk{" +
                    "txnId:" + txnId +
                    ", witnessedAt:" + witnessedAt +
                    ", deps:" + deps +
                    '}';
        }
    }

    public static class PreAcceptNack implements PreAcceptReply
    {
        public static final PreAcceptNack INSTANCE = new PreAcceptNack();

        private PreAcceptNack() {}

        @Override
        public boolean isOK()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "PreAcceptNack{}";
        }
    }

    static Deps calculateDeps(CommandStore commandStore, TxnId txnId, Txn txn, Timestamp executeAt)
    {
        try (Deps.OrderedBuilder builder = Deps.orderedBuilder(false);)
        {
            txn.keys().forEach(key -> {
                CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
                if (forKey == null)
                    return;

                builder.nextKey(key);
                forKey.uncommitted().before(executeAt).forEach(conflicts(txnId, txn.isWrite(), builder));
                forKey.committedByExecuteAt().before(executeAt).forEach(conflicts(txnId, txn.isWrite(), builder));
            });

            return builder.build();
        }
    }

    @Override
    public String toString()
    {
        return "PreAccept{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               ", homeKey:" + homeKey +
               '}';
    }

    private static Consumer<PartialCommand> conflicts(TxnId txnId, boolean isWrite, Deps.OrderedBuilder builder)
    {
        return command -> {
            if (!txnId.equals(command.txnId()) && (isWrite || command.txn().isWrite()))
                builder.add(command.txnId());
        };
    }
}
