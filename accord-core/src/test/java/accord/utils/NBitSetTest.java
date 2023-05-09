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

package accord.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NBitSetTest
{
    @Test
    public void testBasicOperations()
    {
        int capacity = 10;
        int bitsPerValue = 4;
        NBitSet test = new NBitSet(capacity, bitsPerValue);

        for (int i = 0; i < capacity; i++)
        {
            test.set(i, i);
            for (int j = 0; j < capacity; j++)
            {
                if (j <= i)
                    assertEquals(j, test.get(j));
                else
                    assertEquals(0, test.get(j));
            }
        }
    }

    @Test
    public void testSpanningWords()
    {
        doTestSpanningWords();
    }

    public NBitSet doTestSpanningWords()
    {
        int capacity = 10;
        int bitsPerValue = 31;
        NBitSet test = new NBitSet(capacity, bitsPerValue);

        for (int i = 0; i < capacity; i++)
        {
            test.set(i, Integer.MAX_VALUE);
            for (int j = 0; j < capacity; j++)
            {
                if (j <= i)
                    assertEquals(Integer.MAX_VALUE, test.get(j));
                else
                    assertEquals(0, test.get(j));
            }
        }
        return test;
    }

    @Test
    public void testIsEmpty()
    {
        NBitSet test = doTestSpanningWords();
        for (int i = 0; i < 10; i++)
        {
            assertFalse(test.isEmpty());
            test.set(i, 0);
        }
        assertTrue(test.isEmpty());
    }

    @Test
    public void testBoundsChecking()
    {
        int capacity = 10;
        int bitsPerValue = 3;
        NBitSet test = new NBitSet(capacity, bitsPerValue);

        assertThrows(IndexOutOfBoundsException.class, () -> test.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> test.get(capacity));
        assertThrows(IndexOutOfBoundsException.class, () -> test.set(-1, 1));
    }

    @Test
    public void testValueChecking()
    {
        int capacity = 10;
        int bitsPerValue = 3;
        NBitSet test = new NBitSet(capacity, bitsPerValue);

        assertThrows(IllegalArgumentException.class, () -> test.set(0, -1));
        assertThrows(IllegalArgumentException.class, () -> test.set(0, 1 << bitsPerValue));
    }

    @Test
    public void testResizing()
    {
        int capacity = 10;
        int bitsPerValue = 4;
        NBitSet test = new NBitSet(capacity, bitsPerValue);

        // Set values beyond the initial capacity
        int newIndex = capacity * 2;
        int newValue = 5;
        test.set(newIndex, newValue);
        assertEquals(newValue, test.get(newIndex));

        // Check that values within the initial capacity are not affected
        for (int i = 0; i < capacity; i++)
        {
            test.set(i, i);
            for (int j = 0; j <= newIndex; j++)
            {
                if (j <= i)
                    assertEquals(j, test.get(j));
                else if (j == newIndex)
                    assertEquals(newValue, test.get(j));
                else
                    assertEquals(0, test.get(j));
            }
        }
    }
    
    private static void assertThrows(Class<? extends Throwable> clazz, Runnable r)
    {
        try
        {
            r.run();
        }
        catch (Throwable t)
        {
            if (!clazz.equals(t.getClass()))
                throw t;
        }
    }
}
