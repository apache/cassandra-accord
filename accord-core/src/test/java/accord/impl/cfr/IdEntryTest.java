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

package accord.impl.cfr;

import org.junit.jupiter.api.Test;

import org.assertj.core.api.Assertions;

import accord.impl.IntKey;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

/**
 * This file was authored by LLM
 */
public class IdEntryTest
{
    // the format persisted prior to separating SaveStatus.id from ordinal: [durability][5 bits of id][executeAt bit]
    private static int legacyEncoding(SaveStatus saveStatus, Status.Durability durability, boolean executeAtIsTxnId)
    {
        Assertions.assertThat(saveStatus.id).isLessThan(32);
        return (saveStatus.id << Status.Durability.ENCODING_BITS) | durability.encoded() | (executeAtIsTxnId ? 1 << (5 + Status.Durability.ENCODING_BITS) : 0);
    }

    @Test
    public void serializedFormIsStable()
    {
        TxnId txnId = new TxnId(1, 100, ExclusiveSyncPoint, Range, new Node.Id(1));
        Timestamp otherExecuteAt = Timestamp.fromValues(1, 200, new Node.Id(1));
        Range range = IntKey.range(0, 10);
        for (SaveStatus saveStatus : SaveStatus.values())
        {
            for (Status.Durability durability : new Status.Durability[] { Status.Durability.NotDurable, Status.Durability.DurablyStable, Status.Durability.Universal, Status.Durability.UniversalOrInvalidated })
            {
                for (boolean executeAtIsTxnId : new boolean[] { true, false })
                {
                    IdSingleEntry entry = new IdSingleEntry(txnId, range);
                    entry.update(saveStatus, durability, executeAtIsTxnId ? txnId : otherExecuteAt);
                    int serialized = entry.serialize();
                    if (saveStatus.id <= 30)
                        Assertions.assertThat(serialized).isEqualTo(legacyEncoding(saveStatus, durability, executeAtIsTxnId));

                    IdSingleEntry reloaded = IdEntry.SerializerSupport.create(txnId, serialized, range);
                    Assertions.assertThat(reloaded.saveStatus()).isSameAs(saveStatus);
                    Assertions.assertThat(reloaded.durability()).isSameAs(durability);
                    Assertions.assertThat(reloaded.maybeExecuteAt() != null).isEqualTo(executeAtIsTxnId);
                    Assertions.assertThat(reloaded.encoded).isEqualTo(entry.encoded);
                }
            }
        }
    }
}
