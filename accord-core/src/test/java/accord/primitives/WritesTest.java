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

package accord.primitives;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.impl.list.ListQuery;
import accord.impl.list.ListRead;
import accord.impl.list.ListUpdate;
import accord.impl.list.ListWrite;
import accord.local.Node;

import static accord.impl.IntKey.key;
import static accord.impl.IntKey.keys;

/**
 * This file was authored by LLM
 * Writes may be computed by a replica executing only its own keys of a multi key transaction, and then be propagated
 * to (or recovered by) other replicas; the keys a replica applies must be derived from its own definition of the
 * transaction, not from the keys for which the Writes were computed.
 */
class WritesTest
{
    @Test
    void appliesToDerivedFromOwnTxn()
    {
        ListUpdate update = new ListUpdate(null);
        update.put(key(1), 1);
        update.put(key(2), 2);
        update.put(key(3), 3);
        Txn txn = new Txn.InMemory(keys(1, 2, 3), new ListRead(null, false, Keys.EMPTY, Keys.EMPTY), new ListQuery(null, 0, false), update);
        TxnId txnId = new TxnId(1, 1, Txn.Kind.Write, Routable.Domain.Key, new Node.Id(1));

        // Writes computed by a replica that executes only key 1
        Writes executedByOther = new Writes(txnId, txnId, keys(1), new ListWrite(null));
        // our definition of the transaction, as a replica of keys 2 and 3
        PartialTxn ours = txn.intersecting(keys(2, 3).toParticipants(), true);

        Assertions.assertThat(executedByOther.appliesTo(ours)).isEqualTo(keys(2, 3));
        Assertions.assertThat(executedByOther.appliesToAny(ours, keys(2).toParticipants())).isTrue();
        Assertions.assertThat(executedByOther.appliesToAny(ours, keys(3).toParticipants())).isTrue();
        Assertions.assertThat(executedByOther.appliesToAny(ours, keys(4).toParticipants())).isFalse();

        // previously, we used the keys for which the Writes were computed, and so would apply nothing
        Assertions.assertThat(executedByOther.keys.intersects(keys(2, 3).toParticipants())).isFalse();

        // Writes computed by the coordinator (for all keys) are applied for the same keys either way
        Writes executedByCoordinator = new Writes(txnId, txnId, keys(1, 2, 3), new ListWrite(null));
        Assertions.assertThat(executedByCoordinator.appliesTo(ours).overlapping(keys(2, 3).toParticipants()))
                  .isEqualTo(executedByCoordinator.keys.overlapping(keys(2, 3).toParticipants()));

        // Writes.keys may include only the keys whose writes are not derivable, so we refuse to apply without a definition
        Assertions.assertThatThrownBy(() -> executedByOther.appliesTo(null)).isInstanceOf(IllegalStateException.class);
        // but there is nothing to apply if there is no write
        Writes noWrite = new Writes(txnId, txnId, Keys.EMPTY, null);
        Assertions.assertThat(noWrite.appliesTo(null)).isEqualTo(Keys.EMPTY);
        Assertions.assertThat(noWrite.appliesToAny(null, keys(1).toParticipants())).isFalse();
        // and Writes without a write may not claim any keys
        Assertions.assertThatThrownBy(() -> new Writes(txnId, txnId, keys(1), null)).isInstanceOf(IllegalStateException.class);
        Assertions.assertThat(executedByOther.appliesToAny(ours, Keys.EMPTY.toParticipants())).isFalse();
    }
}
