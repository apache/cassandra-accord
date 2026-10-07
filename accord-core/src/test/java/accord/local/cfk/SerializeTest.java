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

package accord.local.cfk;

import java.util.Arrays;
import java.util.function.Supplier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.local.Node;
import accord.primitives.Routable.Domain;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.primitives.TxnId.Cardinality;
import accord.primitives.TxnId.FastPath;
import accord.primitives.TxnId.MediumPath;
import accord.utils.RandomSource;
import accord.utils.RandomTestRunner;

import static accord.local.cfk.Serialize.EMPTY_FLAG_HISTORY;
import static accord.local.cfk.Serialize.encodedFlagBits;
import static accord.local.cfk.Serialize.selectFlagHistory;
import static accord.local.cfk.Serialize.updateFlagHistory;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.TxnId.Cardinality.Any;
import static accord.utils.ArrayUtils.shuffle;

// this class is mostly tested in C* integration due to having concrete serializers to work with
public class SerializeTest
{
    @Test
    public void testFlagHistory()
    {
        for (int c = 0 ; c < 1000 ; ++c)
            testOneFlagHistory();
    }

    private void testOneFlagHistory()
    {
        RandomTestRunner.test().check(rnd -> {
            int[] inputs = new int[rnd.nextInt(100, 1000)];

            Supplier<Kind> kinds = rnd.randomWeightedPicker(randomSubset(Kind.values(), rnd));
            Supplier<Domain> domains = rnd.randomWeightedPicker(randomSubset(Domain.values(), rnd));
            Supplier<FastPath> fastPaths = rnd.randomWeightedPicker(randomSubset(FastPath.values(), rnd));
            Supplier<MediumPath> mediumPaths = rnd.randomWeightedPicker(randomSubset(MediumPath.values(), rnd));
            Supplier<Cardinality> cardinalities = rnd.randomWeightedPicker(randomSubset(Cardinality.values(), rnd));
            float randomBitsChance = rnd.nextFloat() * 0.2f;

            for (int i = 0 ; i < inputs.length ; ++i)
            {
                if (rnd.decide(randomBitsChance))
                {
                    inputs[i] = rnd.nextInt(rnd.nextBoolean() ? 0x100 : 0x10000);
                }
                else
                {
                    Domain domain = domains.get();
                    Cardinality cardinality = domain == Range ? Any : cardinalities.get();
                    TxnId txnId = new TxnId(0, 0, fastPaths.get().bits | mediumPaths.get().bit(), kinds.get(), domain, cardinality, Node.Id.NONE);
                    inputs[i] = txnId.flags();
                }
            }

            int[] sortedHistoryFlags = new int[6];
            long flagHistory = Serialize.EMPTY_FLAG_HISTORY;
            int[] encoded = new int[inputs.length];
            int[] raw = new int[inputs.length];
            long[] histories = new long[inputs.length];
            for (int i = 0 ; i < inputs.length ; ++i)
            {
                int flags = inputs[i];
                encoded[i] = encodedFlagBits(flags, flagHistory);
                Assertions.assertTrue(encoded[i] < 8);
                raw[i] = encoded[i] == 0 ? (flags & 0xff) : encoded[i] == 1 ? (flags & 0xffff) : -1;
                if (encoded[i] == 0)
                {
                    flagHistory = updateFlagHistory(flags, encoded[i], flagHistory);
                    for (int j = 0 ; j < 6 ; ++j)
                        sortedHistoryFlags[j] = (int) ((flagHistory >>> (9 * j)) & 0xff);
                    Arrays.sort(sortedHistoryFlags);
                    for (int j = 1 ; j < 6 ; ++j)
                        Assertions.assertNotEquals(sortedHistoryFlags[j - 1], sortedHistoryFlags[j]);
                }
                histories[i] = flagHistory;
            }

            flagHistory = EMPTY_FLAG_HISTORY;
            for (int i = 0 ; i < inputs.length ; ++i)
            {
                int decodedFlags = encoded[i] < 2 ? raw[i] : selectFlagHistory(encoded[i], flagHistory);
                Assertions.assertEquals(inputs[i], decodedFlags);
                if (encoded[i] == 0)
                    flagHistory = updateFlagHistory(decodedFlags, encoded[i], flagHistory);
                Assertions.assertEquals(histories[i], flagHistory);
            }
        });
    }

    private static <T> T[] randomSubset(T[] input, RandomSource rnd)
    {
        shuffle(input, rnd);
        return Arrays.copyOf(input, 1 + rnd.nextInt(input.length - 1));
    }
}
