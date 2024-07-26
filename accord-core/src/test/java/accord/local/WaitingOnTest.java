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

package accord.local;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;

import accord.local.Command.WaitingOn;
import accord.primitives.Deps;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

class WaitingOnTest
{
    private static final Gen<Gen<Boolean>> BOOLEAN_DISTRO = Gens.bools().mixedDistribution();
    private static final Gen<Deps> DEPS_GEN = AccordGens.depsFromKey(AccordGens.intRoutingKey(), AccordGens.ranges(AccordGens.intRoutingKey()));
    private static final Gen<Gen<WaitingOn>> WAITING_ON_DISTRO = rs -> AccordGens.waitingOn(DEPS_GEN, BOOLEAN_DISTRO.next(rs),
                                                                                            BOOLEAN_DISTRO.next(rs),
                                                                                            BOOLEAN_DISTRO.next(rs),
                                                                                            BOOLEAN_DISTRO.next(rs));

    @Test
    void property()
    {
        qt().withSeed(462761298159978126L).forAll(Gens.flatten(WAITING_ON_DISTRO)).check(WaitingOnTest::validateWaitingOn);
    }

    private static void validateWaitingOn(WaitingOn waitingOn)
    {
        validateIndexOf(waitingOn);
        validateDirectKeyTxnReachable(waitingOn);
        validateWaitingOnKey(waitingOn);
        validateNextWaitingOn(waitingOn);
    }

    private static void validateIndexOf(WaitingOn waitingOn)
    {
        // property: indexOf(txnId(i)) == i
        WaitingOn.Update update = new WaitingOn.Update(waitingOn);
        for (int i = 0, size = waitingOn.txnIdCount(); i < size; i++)
        {
            TxnId expected = waitingOn.txnId(i);
            Assertions.assertThat(waitingOn.indexOf(expected))
                      .describedAs("Txn %s indexOf is not what is expected", expected)
                      .isEqualTo(i);

            // what about update?
            Assertions.assertThat(update.txnId(i)).isEqualTo(expected);
            Assertions.assertThat(update.indexOf(expected))
                      .describedAs("Txn %s Update.indexOf is not what is expected", expected)
                      .isEqualTo(i);
        }
    }

    private static void validateDirectKeyTxnReachable(WaitingOn waitingOn)
    {
        // property: direct key txn are reachable
        for (TxnId expected : waitingOn.directKeyDeps.txnIds())
        {
            Assertions.assertThat(waitingOn.indexOf(expected))
                      .describedAs("Txn %s came from directKeyDeps but indexOf does not match", expected)
                      .isEqualTo(waitingOn.directRangeDeps.txnIdCount() + waitingOn.directKeyDeps.indexOf(expected));
        }
    }

    private static void validateWaitingOnKey(WaitingOn waitingOn)
    {
        // property: isWaitingOnKey(key_index) == is_set(key_offset + key_index)
        RoutingKeys keys = waitingOn.keys;
        int offset = waitingOn.directRangeDeps.txnIdCount() + waitingOn.directKeyDeps.txnIdCount();
        boolean hasKeyWaiting = false;
        for (int i = 0; i < keys.size(); i++)
        {
            boolean expected = waitingOn.waitingOn.get(offset + i);
            Assertions.assertThat(waitingOn.isWaitingOnKey(i))
                      .isEqualTo(expected);
            hasKeyWaiting |= expected;
        }
        Assertions.assertThat(waitingOn.isWaitingOnKey()).isEqualTo(hasKeyWaiting);
    }

    private static void validateNextWaitingOn(WaitingOn waitingOn)
    {
        TxnId nextWaitingOn = waitingOn.nextWaitingOn();
        List<TxnId> waitingOnTxns = waitingOnTxns(waitingOn);
        TxnId maxTxn = waitingOnTxns.stream().max(Comparator.naturalOrder()).orElse(null);
        TxnId minTxn = waitingOnTxns.stream().min(Comparator.naturalOrder()).orElse(null);
        Assertions.assertThat(nextWaitingOn)
                  .describedAs("nextWaitingOn did not match expected")
                  .isEqualTo(maxTxn);

        WaitingOn.Update update = new WaitingOn.Update(waitingOn);
        Assertions.assertThat(update.minWaitingOnTxnId())
                  .describedAs("Update.minWaitingOnTxnId did not match expected")
                  .isEqualTo(minTxn);
        Assertions.assertThat(update.minWaitingOnTxnIdx())
                  .describedAs("Update.minWaitingOnTxnIdx did not match expected")
                  .isEqualTo(minTxn == null ? -1 : waitingOn.indexOf(minTxn));
    }

    private static List<TxnId> waitingOnTxns(WaitingOn waitingOn)
    {
        List<TxnId> matches = new ArrayList<>();
        for (int i = 0, size = waitingOn.txnIdCount(); i < size; i++)
        {
            if (waitingOn.waitingOn.get(i))
                matches.add(waitingOn.txnId(i));
        }
        return matches;
    }
}