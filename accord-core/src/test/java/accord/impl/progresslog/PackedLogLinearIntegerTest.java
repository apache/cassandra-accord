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

package accord.impl.progresslog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PackedLogLinearIntegerTest
{
    @Test
    public void test()
    {
        testCycle("10011", "10100", 3, 5);
        for (int l = 0, h = 1; l <= 10 ; h = (h == 4 ? 1 : h + 1), l += (h == 1 ? 1 : 0))
        {
            int highestBitPosition = ((1 << h) - 2 + l);
            int highestBit = 1 << highestBitPosition;
            int highestValue = highestBit + (((1 << l) - 1) << (highestBitPosition - l));
            testExactCycle(0, l, h);
            testExactCycle(1, l, h);
            testExactCycle((1 << l) - 1, l, h);
            testExactCycle(highestBit, l, h);
            testExactCycle(highestValue, l, h);
            testCycle(highestValue + 1, highestValue, l, h);
            if (l >= 1)
            {
                testExactCycle((1 << (l - 1)) - 1, l, h);
                testExactCycle((1 << (l - 1)), l, h);
            }
        }
    }

    private void testExactCycle(int value, int lowBits, int highBits)
    {
        int encoded = PackedLogLinearInteger.encode(value, lowBits, lowBits + highBits);
        int decoded = PackedLogLinearInteger.decode(encoded, lowBits);
        Assertions.assertEquals(value, decoded);
    }

    private void testCycle(String inStr, String outStr, int lowBits, int highBits)
    {
        int in = Integer.parseInt(inStr, 2);
        int out = Integer.parseInt(outStr, 2);
        testCycle(in, out, lowBits, highBits);
    }

    private void testCycle(int in, int out, int lowBits, int highBits)
    {
        int encoded = PackedLogLinearInteger.encode(in, lowBits, lowBits + highBits);
        int decoded = PackedLogLinearInteger.decode(encoded, lowBits);
        Assertions.assertEquals(out, decoded);
    }

}
